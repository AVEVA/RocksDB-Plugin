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

# The build configuration of a multi-config build is a folder in the executable's path (…\Release\x.exe).
function Get-ExecutableConfiguration([string]$Path) {
    foreach ($name in 'Release', 'RelWithDebInfo', 'MinSizeRel', 'Debug') {
        if ($Path -match "[\\/]$name[\\/]") { return $name }
    }
    'unknown'
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

function Get-ExecutableInfo([string]$Path, [string]$Revision) {
    $item = Get-Item $Path
    [ordered]@{
        path = $item.FullName
        revision = $Revision
        configuration = Get-ExecutableConfiguration $item.FullName
        sizeBytes = $item.Length
        sha256 = (Get-FileHash -Algorithm SHA256 $item.FullName).Hash
        build = Get-BuildTreeInfo $item.FullName
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
