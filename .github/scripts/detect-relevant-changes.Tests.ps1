# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright 2026 AVEVA
#Requires -Version 7.2
#Requires -Modules @{ ModuleName = 'Pester'; ModuleVersion = '5.0.0' }
# Runs detect-relevant-changes.sh against throwaway git repositories: Invoke-Pester <this file>

BeforeAll {
    $script:Script = Join-Path $PSScriptRoot 'detect-relevant-changes.sh'

    function Invoke-Git([string]$Repo, [string[]]$GitArgs) {
        & git -C $Repo -c user.name=t -c user.email=t@example.com @GitArgs | Out-Null
        if ($LASTEXITCODE -ne 0) { throw "git $GitArgs failed" }
    }

    # Commits one file, then a second commit adding $Paths; returns the repo and the first commit.
    function New-Repo([string[]]$Paths) {
        $repo = Join-Path $TestDrive ([guid]::NewGuid().ToString('N'))
        New-Item -ItemType Directory $repo | Out-Null
        Invoke-Git $repo @('init', '-q')
        Set-Content (Join-Path $repo 'base.txt') 'x'
        Invoke-Git $repo @('add', '-A')
        Invoke-Git $repo @('commit', '-q', '-m', 'base')
        $base = (& git -C $repo rev-parse HEAD)
        foreach ($p in $Paths) {
            $full = Join-Path $repo $p
            [IO.Directory]::CreateDirectory((Split-Path $full)) | Out-Null
            [IO.File]::WriteAllText($full, 'x')
        }
        Invoke-Git $repo @('add', '-A')
        Invoke-Git $repo @('commit', '-q', '-m', 'change')
        [pscustomobject]@{ Repo = $repo; Base = $base }
    }

    function Invoke-Detect([string]$Repo, [string]$Base) {
        Push-Location $Repo
        try {
            $output = & bash $script:Script $Base 2>&1 | Out-String
            [pscustomobject]@{ ExitCode = $LASTEXITCODE; Output = $output }
        } finally { Pop-Location }
    }
}

Describe 'detect-relevant-changes.sh' {
    It 'selects the build when an early relevant path is followed by more than a pipe buffer of paths' {
        # The relevant path is listed first; the long tail keeps git writing after the match.
        $paths = @('include/a.hpp') + (1..3000 | ForEach-Object { 'jdocs/' + ('d' * 60) + "$_.md" })
        $r = New-Repo $paths
        $res = Invoke-Detect $r.Repo $r.Base
        $res.ExitCode | Should -Be 0
        $res.Output | Should -Match 'relevant=true'
    }

    It 'skips only when nothing relevant changed' {
        $r = New-Repo @('docs/readme.md')
        $res = Invoke-Detect $r.Repo $r.Base
        $res.ExitCode | Should -Be 0
        $res.Output | Should -Match 'relevant=false'
    }

    It 'selects the build when <Path> changes' -ForEach @(
        @{ Path = '.github/workflows/build-and-test.yml' }
        @{ Path = '.github/scripts/detect-relevant-changes.sh' }
        @{ Path = 'tools/CMakeLists.txt' }
    ) {
        $r = New-Repo @($Path)
        $res = Invoke-Detect $r.Repo $r.Base
        $res.ExitCode | Should -Be 0
        $res.Output | Should -Match 'relevant=true'
    }

    It 'fails explicitly when the diff command fails' {
        $r = New-Repo @('docs/readme.md')
        $res = Invoke-Detect $r.Repo ('0' * 40)
        $res.ExitCode | Should -Not -Be 0
        $res.Output | Should -Not -Match 'relevant=false'
    }
}
