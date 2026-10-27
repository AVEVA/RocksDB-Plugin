# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright 2026 AVEVA
#Requires -Version 7.2
#Requires -Modules @{ ModuleName = 'Pester'; ModuleVersion = '5.0.0' }
# Tests binary discovery and configuration metadata with fake build trees (no builds, no Azure): Invoke-Pester <this file>

BeforeAll {
    . (Join-Path $PSScriptRoot 'BenchmarkMetadata.ps1')
    $script:File = if ($IsWindows) { 'tool.exe' } else { 'tool' }

    function New-Exe([string]$Root, [string]$Sub) {
        $dir = Join-Path $Root $Sub
        New-Item -ItemType Directory $dir -Force | Out-Null
        Set-Content (Join-Path $dir $script:File) 'x'
    }
    function New-Cache([string]$Root, [string]$BuildType) {
        $lines = @('CMAKE_GENERATOR:INTERNAL=Ninja')
        if ($BuildType) { $lines += "CMAKE_BUILD_TYPE:STRING=$BuildType" }
        Set-Content (Join-Path $Root 'CMakeCache.txt') $lines
    }
}

Describe 'Resolve-BenchmarkExecutable' {
    BeforeEach { $script:Root = Join-Path $TestDrive ([guid]::NewGuid().ToString('N')) }

    It 'finds a multi-config (MSVC) layout' {
        New-Exe $script:Root 'sub\Release'
        Resolve-BenchmarkExecutable $script:Root 'sub' 'tool' '' | Should -BeLike "*sub*Release*$script:File"
        Resolve-BenchmarkExecutable $script:Root 'sub' 'tool' 'Release' | Should -BeLike "*Release*$script:File"
    }

    It 'finds a single-config (Ninja) layout' {
        New-Exe $script:Root 'sub'
        Resolve-BenchmarkExecutable $script:Root 'sub' 'tool' '' | Should -BeLike "*sub*$script:File"
        Resolve-BenchmarkExecutable $script:Root 'sub' 'tool' 'RelWithDebInfo' | Should -BeLike "*sub*$script:File"
    }

    It 'throws when the binary is missing' {
        { Resolve-BenchmarkExecutable $script:Root 'sub' 'tool' '' } | Should -Throw '*not found*'
    }

    It 'throws when several configurations exist and none is requested' {
        New-Exe $script:Root 'sub\Release'
        New-Exe $script:Root 'sub\Debug'
        { Resolve-BenchmarkExecutable $script:Root 'sub' 'tool' '' } | Should -Throw '*Several builds*'
        Resolve-BenchmarkExecutable $script:Root 'sub' 'tool' 'Debug' | Should -BeLike "*Debug*$script:File"
    }

    It 'does not fall back to another configuration folder when one is requested' {
        New-Exe $script:Root 'sub\Debug'
        { Resolve-BenchmarkExecutable $script:Root 'sub' 'tool' 'Release' } | Should -Throw '*not found*'
    }
}

Describe 'Configuration metadata' {
    It 'prefers the path component of a multi-config build' {
        Get-ExecutableConfiguration 'C:\b\tests\Debug\x.exe' 'Release' | Should -Be 'Debug'
    }

    It 'falls back to the cache build type for a single-config build, keeping RelWithDebInfo distinct' {
        Get-ExecutableConfiguration 'C:\b\tests\x.exe' 'RelWithDebInfo' | Should -Be 'RelWithDebInfo'
        Get-ExecutableConfiguration 'C:\b\tests\x.exe' 'Release' | Should -Be 'Release'
    }

    It 'reports unknown when neither source says' {
        Get-ExecutableConfiguration 'C:\b\tests\x.exe' '' | Should -Be 'unknown'
        Get-ExecutableConfiguration 'C:\b\tests\x.exe' 'Custom' | Should -Be 'unknown'
    }

    It 'reads the build type from the cache and does not claim a revision' {
        $root = Join-Path $TestDrive ([guid]::NewGuid().ToString('N'))
        New-Exe $root 'tests'
        New-Cache $root 'RelWithDebInfo'
        $info = Get-ExecutableInfo (Join-Path $root "tests\$script:File")
        $info.configuration | Should -Be 'RelWithDebInfo'
        $info.revision | Should -Be 'unknown'
        (Get-ExecutableInfo (Join-Path $root "tests\$script:File") 'abc123').revision | Should -Be 'abc123'
    }

    It 'is unknown without a cache or configuration folder' {
        $root = Join-Path $TestDrive ([guid]::NewGuid().ToString('N'))
        New-Exe $root 'tests'
        (Get-ExecutableInfo (Join-Path $root "tests\$script:File")).configuration | Should -Be 'unknown'
    }
}
