$hasErrors = $false
$hasWarnings = $false

# Check for long path support on Windows
$longPathEnabled = $false
try
{
    $longPathKey = Get-ItemProperty -Path "HKLM:\SYSTEM\CurrentControlSet\Control\FileSystem" -Name "LongPathsEnabled" -ErrorAction SilentlyContinue
    $longPathEnabled = ($longPathKey.LongPathsEnabled -eq 1)
}
catch
{
    $longPathEnabled = $false
}

if (-not $longPathEnabled)
{
    $hasWarnings = $true
    Write-Host("⚠️ Long path support is not enabled. This may cause issues with deep directory structures in vcpkg and build outputs. To enable, run PowerShell as Administrator and execute: 'New-ItemProperty -Path `"HKLM:\SYSTEM\CurrentControlSet\Control\FileSystem`" -Name `"LongPathsEnabled`" -Value 1 -PropertyType DWORD -Force' or see: 'https://learn.microsoft.com/en-us/windows/win32/fileio/maximum-file-path-limitation'") -ForegroundColor Yellow
}
else
{
    Write-Host("✅ Long path support is enabled") -ForegroundColor Green
}

# Check PowerShell version
$psVersion = $PSVersionTable.PSVersion
if ($psVersion.Major -ge 7)
{
    Write-Host("✅ PowerShell $($psVersion.Major).$($psVersion.Minor) is installed") -ForegroundColor Green
}
else
{
    $hasWarnings = $true
    Write-Host("⚠️ PowerShell $($psVersion.Major).$($psVersion.Minor) detected. PowerShell 7+ is recommended for better cross-platform support and performance. Download from: 'https://github.com/PowerShell/PowerShell/releases'") -ForegroundColor Yellow
}

# Check Git installation and configuration
$GIT_PATH = (Get-Command git -ErrorAction SilentlyContinue).Source
if (-not $GIT_PATH)
{
    $hasErrors = $true
    Write-Host("❌ Git is not installed or not in PATH. Please install Git from 'https://git-scm.com/downloads'") -ForegroundColor Red
}
else
{
    Write-Host("✅ Found Git: '${GIT_PATH}'") -ForegroundColor Green

    # Check Git user.name
    $gitUserName = git config --global user.name 2>$null
    if (-not $gitUserName)
    {
        $hasWarnings = $true
        Write-Host("⚠️ Git user.name is not configured. Set it with: 'git config --global user.name `"Your Name`"'") -ForegroundColor Yellow
    }
    else
    {
        Write-Host("✅ Git user.name is configured: '$gitUserName'") -ForegroundColor Green
    }

    # Check Git user.email
    $gitUserEmail = git config --global user.email 2>$null
    if (-not $gitUserEmail)
    {
        $hasWarnings = $true
        Write-Host("⚠️ Git user.email is not configured. Set it with: 'git config --global user.email `"your.email@example.com`"'") -ForegroundColor Yellow
    }
    else
    {
        Write-Host("✅ Git user.email is configured: '$gitUserEmail'") -ForegroundColor Green
    }

    $repoRoot = git rev-parse --show-toplevel 2>$null
    if ($LASTEXITCODE -eq 0 -and $repoRoot)
    {
        $expectedHooksPath = ".github/hooks"
        $hooksDirectory = Join-Path $repoRoot $expectedHooksPath

        if (Test-Path $hooksDirectory)
        {
            $currentHooksPath = git config --local --get core.hooksPath 2>$null
            if ($currentHooksPath -eq ".githooks")
            {
                $hasWarnings = $true
                Write-Host("⚠️ Stale hooks path detected: '$currentHooksPath'. Migrating to '$expectedHooksPath'...") -ForegroundColor Yellow
                git config --local core.hooksPath $expectedHooksPath 2>$null
                if ($LASTEXITCODE -eq 0)
                {
                    Write-Host("✅ Migrated git hooks path to '$expectedHooksPath'") -ForegroundColor Green
                }
                else
                {
                    Write-Host("❌ Failed to migrate hooks path. Run 'git config --local core.hooksPath $expectedHooksPath' manually.") -ForegroundColor Red
                }
            }
            elseif ($currentHooksPath -eq $expectedHooksPath)
            {
                Write-Host("✅ Local git hooks path is configured: '$currentHooksPath'") -ForegroundColor Green
            }
            else
            {
                git config --local core.hooksPath $expectedHooksPath 2>$null
                if ($LASTEXITCODE -eq 0)
                {
                    Write-Host("✅ Configured local git hooks path: '$expectedHooksPath'") -ForegroundColor Green
                }
                else
                {
                    $hasWarnings = $true
                    Write-Host("⚠️ Unable to configure local git hooks path to '$expectedHooksPath'. Run 'git config --local core.hooksPath $expectedHooksPath' from the repository root.") -ForegroundColor Yellow
                }
            }

            $preCommitHookPath = Join-Path $hooksDirectory "pre-commit"
            if (Test-Path $preCommitHookPath)
            {
                Write-Host("✅ Found pre-commit hook: '$preCommitHookPath'") -ForegroundColor Green
            }
            else
            {
                $hasWarnings = $true
                Write-Host("⚠️ Pre-commit hook is missing at '$preCommitHookPath'.") -ForegroundColor Yellow
            }
        }
        else
        {
            $hasWarnings = $true
            Write-Host("⚠️ Git hooks directory '$hooksDirectory' was not found. Pull latest changes or create it before running devdoctor again.") -ForegroundColor Yellow
        }
    }
    else
    {
        $hasWarnings = $true
        Write-Host("⚠️ Not running inside a git working tree. Skipping git hook setup.") -ForegroundColor Yellow
    }
}

# Check Visual Studio installation and required C++ workload (Windows only)
if ($IsWindows -or $env:OS -eq "Windows_NT")
{
    $vswhere = Join-Path ${env:ProgramFiles(x86)} "Microsoft Visual Studio\Installer\vswhere.exe"
    if (-not (Test-Path $vswhere))
    {
        $hasErrors = $true
        Write-Host("❌ Unable to find Visual Studio installation. Please install Visual Studio with the 'Desktop development with C++' workload.") -ForegroundColor Red
    }
    else
    {
        $VS_PATH = &$vswhere -property installationpath
        if (-not $VS_PATH)
        {
            $hasErrors = $true
            Write-Host("❌ Unable to find Visual Studio installation. Please install Visual Studio with the 'Desktop development with C++' workload.") -ForegroundColor Red
        }
        else
        {
            Write-Host("✅ Found a Visual Studio installation path: '$VS_PATH'") -ForegroundColor Green

            # `-requires` filters to installations that actually have the C++ toolset; an empty
            # result means the workload is missing. Avoid `-property isComplete`, which can return
            # the string "0" (truthy in PowerShell) and falsely report the workload as present.
            $cppInstallPath = &$vswhere -products * -requires "Microsoft.VisualStudio.Component.VC.Tools.x86.x64" -property installationPath
            if (-not $cppInstallPath)
            {
                $hasErrors = $true
                Write-Host("❌ The 'Desktop development with C++' workload is not installed in Visual Studio. Please install it via the Visual Studio Installer.") -ForegroundColor Red
            }
            else
            {
                Write-Host("✅ Found 'Desktop development with C++' workload") -ForegroundColor Green
            }
        }
    }
}

# Check available disk space
try
{
    $drive = Get-PSDrive -Name ($PWD.Drive.Name) -ErrorAction SilentlyContinue
    if ($drive)
    {
        $freeSpaceGB = [math]::Round($drive.Free / 1GB, 2)
        if ($freeSpaceGB -lt 10)
        {
            $hasWarnings = $true
            Write-Host("⚠️ Low disk space: ${freeSpaceGB}GB available. vcpkg builds and C++ compilation can require significant disk space (20GB+ recommended).") -ForegroundColor Yellow
        }
        elseif ($freeSpaceGB -lt 20)
        {
            $hasWarnings = $true
            Write-Host("⚠️ Available disk space: ${freeSpaceGB}GB. Consider freeing up space if you plan to do full builds (20GB+ recommended).") -ForegroundColor Yellow
        }
        else
        {
            Write-Host("✅ Available disk space: ${freeSpaceGB}GB") -ForegroundColor Green
        }
    }
}
catch
{
    # Silently continue if we can't check disk space
}

# Check CMake installation and version
$CMAKE_PATH = (Get-Command cmake -ErrorAction SilentlyContinue).Source
if (-not $CMAKE_PATH)
{
    $hasErrors = $true
    Write-Host("❌ Unable to find cmake. Please install CMake 3.22+ and ensure it is on your PATH. " +
        "On Windows, run from a Visual Studio developer console with the 'Desktop development with C++' workload.") -ForegroundColor Red
}
else
{
    Write-Host("✅ Found cmake: '${CMAKE_PATH}'") -ForegroundColor Green

    # Check CMake version against minimum required in the root CMakeLists.txt
    $cmakeListsPath = "CMakeLists.txt"
    $requiredMajor = 3
    $requiredMinor = 22

    if (Test-Path $cmakeListsPath)
    {
        $cmakeListsContent = Get-Content $cmakeListsPath -Raw
        if ($cmakeListsContent -match "cmake_minimum_required\s*\(\s*VERSION\s+(\d+)\.(\d+)")
        {
            $requiredMajor = [int]$matches[1]
            $requiredMinor = [int]$matches[2]
        }
    }

    $cmakeVersionOutput = (cmake --version 2>&1) | Out-String
    if ($cmakeVersionOutput -match "cmake version (\d+)\.(\d+)\.(\d+)")
    {
        $cmakeMajor = [int]$matches[1]
        $cmakeMinor = [int]$matches[2]
        $cmakePatch = [int]$matches[3]
        $cmakeVersion = "$cmakeMajor.$cmakeMinor.$cmakePatch"

        if ($cmakeMajor -gt $requiredMajor -or ($cmakeMajor -eq $requiredMajor -and $cmakeMinor -ge $requiredMinor))
        {
            Write-Host("✅ CMake version $cmakeVersion meets minimum requirement ($requiredMajor.$requiredMinor)") -ForegroundColor Green
        }
        else
        {
            $hasErrors = $true
            Write-Host("❌ CMake version $cmakeVersion is too old. Minimum required version is $requiredMajor.$requiredMinor. Please update CMake.") -ForegroundColor Red
        }
    }
    else
    {
        $hasWarnings = $true
        Write-Host("⚠️ Unable to parse CMake version. Please ensure CMake $requiredMajor.$requiredMinor or later is installed.") -ForegroundColor Yellow
    }
}

# Check clang-format (used by formatting and the pre-commit hook)
$CLANG_FORMAT_PATH = (Get-Command clang-format -ErrorAction SilentlyContinue).Source
if (-not $CLANG_FORMAT_PATH)
{
    $hasWarnings = $true
    Write-Host("⚠️ Unable to find clang-format in PATH. Install LLVM/Clang tools and ensure clang-format is available for native code formatting and pre-commit hooks.") -ForegroundColor Yellow
}
else
{
    Write-Host("✅ Found clang-format: '${CLANG_FORMAT_PATH}'") -ForegroundColor Green
}

# Check clang-tidy (used for static analysis)
$CLANG_TIDY_PATH = (Get-Command clang-tidy -ErrorAction SilentlyContinue).Source
if (-not $CLANG_TIDY_PATH)
{
    $hasWarnings = $true
    Write-Host("⚠️ Unable to find clang-tidy in PATH. Install LLVM/Clang tools to enable static analysis checks defined in .clang-tidy.") -ForegroundColor Yellow
}
else
{
    Write-Host("✅ Found clang-tidy: '${CLANG_TIDY_PATH}'") -ForegroundColor Green
}

# Check gitleaks (required by the pre-commit hook for secret scanning)
$GITLEAKS_PATH = (Get-Command gitleaks -ErrorAction SilentlyContinue).Source
if (-not $GITLEAKS_PATH)
{
    $hasWarnings = $true
    Write-Host("⚠️ Unable to find gitleaks in PATH. The pre-commit hook requires it for secret scanning. Install via: winget install gitleaks.gitleaks") -ForegroundColor Yellow
}
else
{
    Write-Host("✅ Found gitleaks: '${GITLEAKS_PATH}'") -ForegroundColor Green
}

# Check vcpkg installation and configuration
$VCPKG_PATH = (Get-Command vcpkg -ErrorAction SilentlyContinue).Source
if (-not $VCPKG_PATH)
{
    $hasErrors = $true
    Write-Host("❌ Unable to find vcpkg. Please clone vcpkg from 'https://github.com/microsoft/vcpkg', run bootstrap-vcpkg.bat, and add the vcpkg directory to your PATH. See: 'https://learn.microsoft.com/en-us/vcpkg/get_started/get-started?pivots=shell-powershell'") -ForegroundColor Red
}
else
{
    Write-Host("✅ Found vcpkg: '${VCPKG_PATH}'") -ForegroundColor Green
}

if (-not $env:VCPKG_ROOT)
{
    $hasErrors = $true
    Write-Host("❌ VCPKG_ROOT environment variable is not set. Please set it to your vcpkg installation directory.") -ForegroundColor Red
}
else
{
    Write-Host("✅ Found VCPKG_ROOT: '$env:VCPKG_ROOT'") -ForegroundColor Green
}

$expectedBinarySource = "x-az-universal,https://dev.azure.com/AVEVA-VSTS,,Cloud-Platform,"
$binarySourceRead = "${expectedBinarySource}read"
$binarySourceReadWrite = "${expectedBinarySource}readwrite"
if (-not $env:VCPKG_BINARY_SOURCES)
{
    $hasWarnings = $true
    Write-Host("⚠️ VCPKG_BINARY_SOURCES environment variable is not set. Consider setting it to '$binarySourceReadWrite' or '$binarySourceRead' to enable binary caching.") -ForegroundColor Yellow
}
elseif ($env:VCPKG_BINARY_SOURCES -notmatch [regex]::Escape($expectedBinarySource) + "(read|readwrite)")
{
    $hasWarnings = $true
    Write-Host("⚠️ VCPKG_BINARY_SOURCES is set but not configured for Azure binary caching. Consider setting it to '$binarySourceRead' or '$binarySourceReadWrite' to take advantage of binary caching. You don't have to set this up but it's HIGHLY encouraged. Your initial build is GUARANTEED TO BE SLOW without it. Current value: '$env:VCPKG_BINARY_SOURCES'") -ForegroundColor Yellow
}
else
{
    Write-Host("✅ Found VCPKG_BINARY_SOURCES: '$env:VCPKG_BINARY_SOURCES'") -ForegroundColor Green

    $AZ_CLI_PATH = (Get-Command az -ErrorAction SilentlyContinue).Source
    if (-not $AZ_CLI_PATH)
    {
        $hasErrors = $true
        Write-Host("❌ Azure CLI (az) is not installed. Since VCPKG_BINARY_SOURCES is configured for Azure DevOps, please install it: 'https://learn.microsoft.com/en-us/cli/azure/install-azure-cli-windows'") -ForegroundColor Red
    }
    else
    {
        Write-Host("✅ Found Azure CLI: '${AZ_CLI_PATH}'") -ForegroundColor Green

        # Check if logged in to Azure
        $azDevOpsAccount = az account show 2>$null | ConvertFrom-Json -ErrorAction SilentlyContinue
        if (-not $azDevOpsAccount)
        {
            $hasWarnings = $true
            Write-Host("⚠️ Not logged in to Azure. Please follow the authentication instructions at: 'https://dev.azure.com/AVEVA-VSTS/Cloud%20Platform/_artifacts/feed/Cloud-Platform/connect'. Click on the 'Universal Packages' section to find the instructions.") -ForegroundColor Yellow
        }
        else
        {
            Write-Host("✅ Logged in to Azure (Subscription: $($azDevOpsAccount.name))") -ForegroundColor Green
        }
    }
}

# Check Node.js / npm (optional — used to run agentrc readiness tooling via npx)
$NODE_PATH = (Get-Command node -ErrorAction SilentlyContinue).Source
if (-not $NODE_PATH)
{
    $hasWarnings = $true
    Write-Host("⚠️ Node.js is not installed. It is only needed to run the agentrc readiness check via npx. Install from 'https://nodejs.org/' if you plan to run that tooling.") -ForegroundColor Yellow
}
else
{
    Write-Host("✅ Found Node.js: '${NODE_PATH}'") -ForegroundColor Green
}

# Summary
Write-Host ""
if ($hasErrors)
{
    Write-Host "❌ Development environment check failed. Please address the errors above before proceeding." -ForegroundColor Red
}
elseif ($hasWarnings)
{
    Write-Host "⚠️ Development environment is functional but could be improved. Consider addressing the warnings above for a better development experience." -ForegroundColor Yellow
}
else
{
    Write-Host "🎉 All checks passed! Your development environment is ready to go!" -ForegroundColor Green
}
