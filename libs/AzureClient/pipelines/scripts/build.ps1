[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string]$Preset,

    [string]$BuildPreset,

    [string]$TestPreset,

    [bool]$WithTests = $true,

    [bool]$WithBenchmarks = $false,

    [string]$TestLabel,

    [string[]]$AdditionalConfigureArgs = @(),

    [string[]]$AdditionalBuildArgs = @(),

    [string[]]$AdditionalTestArgs = @(),

    [switch]$ConfigureOnly,

    [switch]$SkipTests
)

$ErrorActionPreference = "Stop"
$PSNativeCommandUseErrorActionPreference = $true

$repoRoot = (Resolve-Path (Join-Path (Join-Path $PSScriptRoot "..") "..")).Path
if (-not $env:BUILD_BINARIESDIRECTORY)
{
    $env:BUILD_BINARIESDIRECTORY = Join-Path (Join-Path $repoRoot "build") "ci"
}

New-Item -ItemType Directory -Force -Path $env:BUILD_BINARIESDIRECTORY | Out-Null

if (-not $BuildPreset)
{
    $BuildPreset = $Preset
}

if (-not $TestPreset -and $WithTests -and -not $SkipTests)
{
    $TestPreset = $Preset
}

$env:AVEVA_AZURE_CLIENT_WITH_TESTS = if ($WithTests) { "ON" } else { "OFF" }

# The configure, build and test presets must share one binary directory, otherwise the test step would
# run against a stale or missing build tree.
function Get-PresetBinaryDir([object]$Presets, [string]$Name)
{
    $preset = $Presets.configurePresets | Where-Object { $_.name -eq $Name } | Select-Object -First 1
    if (-not $preset)
    {
        throw "Configure preset '$Name' was not found in CMakePresets.json."
    }
    if ($preset.binaryDir)
    {
        return $preset.binaryDir
    }
    foreach ($parent in @($preset.inherits))
    {
        if ($parent)
        {
            $inherited = Get-PresetBinaryDir $Presets $parent
            if ($inherited)
            {
                return $inherited
            }
        }
    }
    return $null
}

function Assert-PresetsShareBinaryDir
{
    $presets = Get-Content (Join-Path $repoRoot "CMakePresets.json") -Raw | ConvertFrom-Json
    $configureDir = Get-PresetBinaryDir $presets $Preset
    $dependents = @(
        @{ Kind = "build"; Name = $BuildPreset; Presets = $presets.buildPresets },
        @{ Kind = "test"; Name = $TestPreset; Presets = $presets.testPresets })
    foreach ($dependent in $dependents)
    {
        if (-not $dependent.Name)
        {
            continue
        }
        $match = $dependent.Presets | Where-Object { $_.name -eq $dependent.Name } | Select-Object -First 1
        if (-not $match)
        {
            throw "The $($dependent.Kind) preset '$($dependent.Name)' was not found in CMakePresets.json."
        }
        $dir = Get-PresetBinaryDir $presets $match.configurePreset
        if ($dir -ne $configureDir)
        {
            throw "The $($dependent.Kind) preset '$($dependent.Name)' uses binary directory '$dir' but configure preset '$Preset' uses '$configureDir'."
        }
    }
}

Assert-PresetsShareBinaryDir

if (-not $env:AVEVA_AZURE_CLIENT_VCPKG_FEATURES)
{
    $features = New-Object System.Collections.Generic.List[string]
    if ($WithTests)
    {
        $features.Add("tests")
    }
    if ($WithBenchmarks)
    {
        $features.Add("benchmarks")
    }

    $env:AVEVA_AZURE_CLIENT_VCPKG_FEATURES = [string]::Join(";", $features)
}

if (-not $env:CMAKE_BUILD_PARALLEL_LEVEL)
{
    $env:CMAKE_BUILD_PARALLEL_LEVEL = [Environment]::ProcessorCount.ToString()
}

Write-Host "Repository root: $repoRoot"
Write-Host "BUILD_BINARIESDIRECTORY: $env:BUILD_BINARIESDIRECTORY"
Write-Host "Preset: $Preset"
Write-Host "VCPKG features: $env:AVEVA_AZURE_CLIENT_VCPKG_FEATURES"

$configureCommand = @("--preset", $Preset) + $AdditionalConfigureArgs
& cmake @configureCommand

if ($ConfigureOnly)
{
    return
}

$buildCommand = @("--build", "--preset", $BuildPreset, "--parallel", $env:CMAKE_BUILD_PARALLEL_LEVEL) + $AdditionalBuildArgs
& cmake @buildCommand

if (-not $SkipTests -and $WithTests -and $TestPreset)
{
    $testCommand = @("--preset", $TestPreset, "--parallel", $env:CMAKE_BUILD_PARALLEL_LEVEL)
    if ($TestLabel)
    {
        $testCommand += @("-L", $TestLabel)
    }
    $testCommand += $AdditionalTestArgs
    & ctest @testCommand
}
