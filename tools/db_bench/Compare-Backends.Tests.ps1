# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright 2026 AVEVA
#Requires -Version 7.2
#Requires -Modules @{ ModuleName = 'Pester'; ModuleVersion = '5.0.0' }
# Runs Compare-Backends.ps1 against fake db_bench executables (no Azure access): Invoke-Pester <this file>

BeforeAll {
    $script:Script = Join-Path $PSScriptRoot 'Compare-Backends.ps1'

    # Creates a fake db_bench .cmd that prints $Lines and exits with $Exit.
    function New-FakeBench([string]$Dir, [string]$Name, [string[]]$Lines, [int]$Exit = 0) {
        $body = @('@echo off') + ($Lines | ForEach-Object { "echo $_" }) + "exit /b $Exit"
        $path = Join-Path $Dir "$Name.cmd"
        Set-Content -Path $path -Value $body
        $path
    }

    function Invoke-Compare([string]$Baseline, [string]$Candidate, [string]$Out) {
        $env:AZURE_TENANT_ID = 't'
        $env:AZURE_SERVICE_PRINCIPAL_ID = 'c'
        $env:AZURE_SERVICE_PRINCIPAL_SECRET = 'fake-secret-value'
        $output = & pwsh -NoProfile -File $script:Script -BaselineExe $Baseline -CandidateExe $Candidate -Account fake `
            -Scenarios write -Reps 1 -Quick -OutDir $Out 2>&1 | Out-String
        [pscustomobject]@{
            ExitCode = $LASTEXITCODE
            Output   = $output
            Csv      = if (Test-Path "$Out\summary.csv") { @(Import-Csv "$Out\summary.csv") } else { @() }
        }
    }

    $script:Good = 'fillseq      :     100000.000 micros/op 10 ops/sec', 'fillrandom   :     200000.000 micros/op 5 ops/sec',
                   'overwrite    :     300000.000 micros/op 3 ops/sec'
}

Describe 'Compare-Backends.ps1 result validation' {
    BeforeEach {
        $script:Dir = Join-Path $TestDrive ([guid]::NewGuid().ToString('N'))
        New-Item -ItemType Directory $script:Dir | Out-Null
        $script:Good1 = New-FakeBench $script:Dir 'good' $script:Good
    }

    It 'succeeds with valid paired output' {
        $r = Invoke-Compare $script:Good1 $script:Good1 "$script:Dir\out"
        $r.ExitCode | Should -Be 0
        $r.Csv.Count | Should -Be 3
        $r.Csv[0].baseline | Should -Be '100.0'
        $r.Csv[0].Samples | Should -Be '1/1'
    }

    It 'fails when the candidate exits nonzero' {
        $bad = New-FakeBench $script:Dir 'bad' $script:Good 3
        $r = Invoke-Compare $script:Good1 $bad "$script:Dir\out"
        $r.ExitCode | Should -Not -Be 0
        $r.Csv | ForEach-Object { $_.candidate | Should -Be 'n/a' }
    }

    It 'fails when a zero-exit run lacks the expected metrics' {
        $none = New-FakeBench $script:Dir 'none' @('no metrics here')
        $r = Invoke-Compare $script:Good1 $none "$script:Dir\out"
        $r.ExitCode | Should -Not -Be 0
        $r.Csv | ForEach-Object { $_.candidate | Should -Be 'n/a' }
        $r.Csv | ForEach-Object { $_.candidate | Should -Not -Be '0.0' }
    }

    It 'fails on malformed metrics' {
        $mal = New-FakeBench $script:Dir 'mal' @('fillseq : abc micros/op', 'fillrandom : 1.2.3 micros/op', 'overwrite : 5 micros/op')
        $r = Invoke-Compare $mal $script:Good1 "$script:Dir\out"
        $r.ExitCode | Should -Not -Be 0
        $r.Csv | ForEach-Object { $_.baseline | Should -Be 'n/a' }
    }

    It 'fails when a backend is missing every sample' {
        $bad = New-FakeBench $script:Dir 'bad' $script:Good 1
        $r = Invoke-Compare $bad $bad "$script:Dir\out"
        $r.ExitCode | Should -Not -Be 0
        $r.Csv.Count | Should -Be 3
    }
}
