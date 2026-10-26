# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright 2026 AVEVA
#Requires -Version 7.2
<#
.SYNOPSIS
    Runs the loopback allocation/latency benchmarks of the production HTTP and Azure clients and records what they ran on.

.DESCRIPTION
    Needs no Azure account and spends no cloud resources. It runs two gtest executables that talk to an in-process
    loopback server on its own thread:
      aveva-http-client-tests  (HttpClientAllocations.PublicClientPooledRequestWithIndependentServer)
          the public HttpClient::SendAsync, including its final completion hand-off.
      aveva-azure-client-e2e-benchmark
          BlobClient ranged reads -> request pipeline -> HttpClient -> TCP -> loopback page-blob server.
    Each prints 'BENCH ...' lines with allocations and bytes per request, requests/connections the server saw,
    throughput and p50/p99/max latency. These are loopback CPU and allocation numbers; they are NOT Azure network
    results (use Compare-Backends.ps1 -ConfirmLiveAzure for those). Use a Release build: Debug allocation counts
    and timings are not comparable.

    Run it once per revision under test (for example before and after an R08-R17 change) with a different -Label and
    compare the BENCH lines in the output folders. loopback-metadata.json records the revision, whether the tree had
    uncommitted changes, the executables' hashes and build trees, and the host.

.EXAMPLE
    cmake --preset WindowsRelease
    cmake --build build\WindowsRelease --config Release --target aveva-http-client-tests aveva-azure-client-e2e-benchmark
    .\Measure-Loopback.ps1 -Label before
#>
[CmdletBinding()]
param(
    [string] $BuildDir = (Join-Path $PSScriptRoot '..\..\build\WindowsRelease'),
    [string] $Configuration = 'Release',
    [string] $Label = 'run',
    # Runs the measurement N times; repeat runs show how much the timings (not the allocation counts) vary.
    [int] $Repeat = 1,
    [string] $OutDir = (Join-Path ([IO.Path]::GetTempPath()) ("loopback_bench_${Label}_" + (Get-Date -Format yyyyMMdd_HHmmss)))
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest
. (Join-Path $PSScriptRoot 'BenchmarkMetadata.ps1')
$repoRoot = Resolve-Path (Join-Path $PSScriptRoot '..\..')

$targets = [ordered]@{
    'http-client' = @{
        Exe = Join-Path $BuildDir "libs\HttpClient\tests\$Configuration\aveva-http-client-tests.exe"
        Args = @('--gtest_filter=HttpClientAllocations.PublicClientPooledRequestWithIndependentServer')
    }
    'azure-client' = @{
        Exe = Join-Path $BuildDir "libs\AzureClient\tests\$Configuration\aveva-azure-client-e2e-benchmark.exe"
        Args = @()
    }
}
foreach ($t in $targets.Values) {
    if (-not (Test-Path $t.Exe)) { throw "Benchmark executable not found: $($t.Exe). Build it first (see Get-Help)." }
}
if ($Configuration -ne 'Release') {
    Write-Warning "$Configuration build: allocation counts and timings are not comparable with Release results."
}

New-Item -ItemType Directory $OutDir -Force | Out-Null
$failed = $false
$exes = [ordered]@{}
foreach ($name in $targets.Keys) {
    $t = $targets[$name]
    $exes[$name] = Get-ExecutableInfo $t.Exe (Get-RepoState $repoRoot).revision
    for ($i = 1; $i -le $Repeat; $i++) {
        $log = Join-Path $OutDir "$name-$i.txt"
        & $t.Exe @($t.Args) *> $log
        if ($LASTEXITCODE -ne 0) { $failed = $true; Write-Host "FAILED: $name run $i (exit $LASTEXITCODE), see $log" -ForegroundColor Red }
        Select-String -Path $log -Pattern '^BENCH ' | ForEach-Object { Write-Host $_.Line }
    }
}

[ordered]@{
    kind = 'loopback-cpu'
    label = $Label
    host = Get-HostInfo $repoRoot
    executables = $exes
    parameters = [ordered]@{
        repeat = $Repeat; network = 'loopback only (not Azure)'; server = 'independent thread'
        state = 'warm: connections opened by a warm-up request, measured requests reuse the pooled keep-alive connection'
        counted = 'operator new on the client thread only; the server thread is excluded'
    }
} | ConvertTo-Json -Depth 8 | Set-Content (Join-Path $OutDir 'loopback-metadata.json')

Write-Host "Logs and loopback-metadata.json: $OutDir"
if ($failed) { exit 1 }
