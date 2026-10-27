# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright 2026 AVEVA
#Requires -Version 7.2
# Dot-sourced by Compare-Backends.ps1 and Measure-Loopback.ps1: records what a benchmark ran on so results can be
# reproduced and compared. Nothing here reads, stores or prints credentials.

function Get-RepoState([string]$RepoRoot) {
    $revision = (& git -C $RepoRoot rev-parse HEAD 2>$null)
    $dirty = [bool](& git -C $RepoRoot status --porcelain 2>$null)
    [ordered]@{ revision = "$revision".Trim(); uncommittedChanges = $dirty }
}

$script:BuildConfigurations = 'Release', 'RelWithDebInfo', 'MinSizeRel', 'Debug'

# A multi-config build (Visual Studio, Ninja Multi-Config) puts the configuration in a folder of the executable's path
# (...\Release\x.exe). A single-config build (Ninja, Makefiles) has no such folder, so fall back to the build type
# the cache recorded; an empty or unrecognised build type stays 'unknown' rather than being guessed.
function Get-ExecutableConfiguration([string]$Path, [string]$BuildType) {
    foreach ($name in $script:BuildConfigurations) {
        if ($Path -match "[\\/]$name[\\/]") { return $name }
    }
    if ($BuildType -and $BuildType -in $script:BuildConfigurations) { return $BuildType }
    'unknown'
}

# Finds the output of a CMake target below a build tree: <dir>/<Configuration>/<name> for multi-config generators,
# <dir>/<name> for single-config ones. Without -Configuration every layout is probed and more than one hit is an error,
# so a stale binary from another configuration is never picked silently. Only the Windows '.exe' suffix is probed on
# Windows; elsewhere the bare name is used (Linux execution of this helper is untested).
function Resolve-BenchmarkExecutable([string]$BuildDir, [string]$RelativeDir, [string]$Name, [string]$Configuration) {
    $file = if ($IsWindows) { "$Name.exe" } else { $Name }
    $dir = Join-Path $BuildDir $RelativeDir
    $candidates = if ($Configuration) {
        @((Join-Path $dir "$Configuration/$file"), (Join-Path $dir $file))
    }
    else {
        @(Join-Path $dir $file) + @($script:BuildConfigurations | ForEach-Object { Join-Path $dir "$_/$file" })
    }
    $found = @($candidates | Where-Object { Test-Path -PathType Leaf $_ })
    if ($found.Count -eq 0) {
        throw "Benchmark executable '$file' not found under '$dir' (looked for: $($candidates -join ', ')). Build it first, or pass its path explicitly."
    }
    if ($found.Count -gt 1 -and -not $Configuration) {
        throw "Several builds of '$file' exist ($($found -join ', ')); pass -Configuration or an explicit executable path."
    }
    (Resolve-Path $found[0]).Path
}

# Compiler and triplet come from the CMakeCache of the build tree that produced the executable, when one is found
# above it.
function Get-BuildTreeInfo([string]$ExePath) {
    $dir = Split-Path -Parent (Resolve-Path $ExePath).Path
    while ($dir) {
        $cache = Join-Path $dir 'CMakeCache.txt'
        if (Test-Path $cache) {
            $info = [ordered]@{ buildTree = $dir }
            foreach ($key in 'CMAKE_CXX_COMPILER', 'CMAKE_GENERATOR', 'VCPKG_TARGET_TRIPLET', 'CMAKE_BUILD_TYPE') {
                $line = Select-String -Path $cache -Pattern "^${key}:[A-Z]+=(.*)$" | Select-Object -First 1
                if ($line) { $info[$key] = $line.Matches[0].Groups[1].Value }
            }
            return $info
        }
        $dir = Split-Path -Parent $dir
    }
    [ordered]@{ buildTree = 'not found' }
}

# $Revision is only what the caller can vouch for the binary being built from; leave it empty when unknown (the default)
# instead of passing the repository's current HEAD, which says nothing about when the binary was built.
function Get-ExecutableInfo([string]$Path, [string]$Revision) {
    $item = Get-Item $Path
    $build = Get-BuildTreeInfo $item.FullName
    [ordered]@{
        path = $item.FullName
        revision = if ($Revision) { $Revision } else { 'unknown' }
        configuration = Get-ExecutableConfiguration $item.FullName $build['CMAKE_BUILD_TYPE']
        sizeBytes = $item.Length
        lastWriteUtc = $item.LastWriteTimeUtc.ToString('o')
        sha256 = (Get-FileHash -Algorithm SHA256 $item.FullName).Hash
        build = $build
    }
}

function Get-HostInfo([string]$RepoRoot) {
    $vcpkg = Get-Content (Join-Path $RepoRoot 'vcpkg.json') -Raw -ErrorAction SilentlyContinue | ConvertFrom-Json
    [ordered]@{
        generatedUtc = (Get-Date).ToUniversalTime().ToString('o')
        os = [Runtime.InteropServices.RuntimeInformation]::OSDescription
        logicalProcessors = [Environment]::ProcessorCount
        powershell = $PSVersionTable.PSVersion.ToString()
        vcpkgBaseline = if ($vcpkg) { $vcpkg.'builtin-baseline' } else { 'unknown' }
        repository = Get-RepoState $RepoRoot
    }
}
