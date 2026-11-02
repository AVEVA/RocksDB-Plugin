# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright 2026 AVEVA
#Requires -Version 7.2
<#
.SYNOPSIS
    Runs the loopback allocation/latency benchmarks of the production HTTP and Azure clients and records what they ran on.

.DESCRIPTION
    Needs no Azure account and spends no cloud resources.     It runs three gtest executables that talk to an in-process
        or loopback server:
      aveva-http-client-tests  (HttpClientAllocations.PublicClientPooledRequestWithIndependentServer)
          the public HttpClient::SendAsync, including its final completion hand-off.
      aveva-azure-client-e2e-benchmark
          BlobClient ranged reads -> request pipeline -> HttpClient -> TCP -> loopback page-blob server.
      aveva-rocksdb-plugin-azure-impl-benchmark
          the plugin's file classes (4 MiB reads, merged MultiRead, prefetch hits, small append+Sync) over the real
          BlobClient and an in-process fake service; no sockets or TLS. Its counters cover the issuing threads and
          the whole process, including the inline fake service; see tools/db_bench/README.md for what they exclude.
    Each prints 'BENCH ...' lines with allocations and bytes per request, requests/connections the server saw,
    throughput and p50/p99/max latency. These are loopback CPU and allocation numbers; they are NOT Azure network
    results (use Compare-Backends.ps1 -ConfirmLiveAzure for those). Use a Release build: Debug allocation counts
    and timings are not comparable.

    Run it once per revision under test (for example before and after an R08-R17 change) with a different -Label and
    compare the BENCH lines in the output folders. loopback-metadata.json records the repository HEAD and dirty state at
    run time, and each executable's path, configuration, hash and build tree. The executable's own revision is recorded as
    'unknown': HEAD at run time is not proof that the binary was built from it (compare lastWriteUtc and rebuild first).
    Binaries are found for single-config (Ninja) and multi-config (Visual Studio) trees; pass -Configuration if several
    configurations are built, or -HttpClientExe/-AzureClientExe/-PluginExe for explicit paths. Only Windows has been
    exercised; on other platforms the suffix-less name is looked up but this is untested.

.EXAMPLE
    cmake --preset WindowsRelease
    cmake --build build\WindowsRelease --config Release --target aveva-http-client-tests aveva-azure-client-e2e-benchmark aveva-rocksdb-plugin-azure-impl-benchmark
    .\Measure-Loopback.ps1 -Label before
#>
[CmdletBinding()]
param(
    [string] $BuildDir = (Join-Path $PSScriptRoot '..\..\build\WindowsRelease'),
    # Empty: discover the build layout (single-config, e.g. Ninja, or a single multi-config folder such as Release).
    [string] $Configuration = '',
    # Explicit executables override discovery under -BuildDir.
    [string] $HttpClientExe,
    [string] $AzureClientExe,
    [string] $PluginExe,
    [string] $Label = 'run',
    # Runs the measurement N times; repeat runs show how much the timings (not the allocation counts) vary.
    [int] $Repeat = 1,
    [string] $OutDir = (Join-Path ([IO.Path]::GetTempPath()) ("loopback_bench_${Label}_" + (Get-Date -Format yyyyMMdd_HHmmss)))
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest
. (Join-Path $PSScriptRoot 'BenchmarkMetadata.ps1')
$repoRoot = Resolve-Path (Join-Path $PSScriptRoot '..\..')

function Resolve-Target([string]$Explicit, [string]$RelativeDir, [string]$Name) {
    if ($Explicit) {
        if (-not (Test-Path -PathType Leaf $Explicit)) { throw "Benchmark executable not found: $Explicit" }
        return (Resolve-Path $Explicit).Path
    }
    Resolve-BenchmarkExecutable $BuildDir $RelativeDir $Name $Configuration
}

$targets = [ordered]@{
    'http-client' = @{
        Exe = Resolve-Target $HttpClientExe 'libs\HttpClient\tests' 'aveva-http-client-tests'
        Args = @('--gtest_filter=HttpClientAllocations.PublicClientPooledRequestWithIndependentServer')
    }
    'azure-client' = @{
        Exe = Resolve-Target $AzureClientExe 'libs\AzureClient\tests' 'aveva-azure-client-e2e-benchmark'
        Args = @()
    }
    'plugin' = @{
        Exe = Resolve-Target $PluginExe 'tests\AVEVA\RocksDB\Plugin\Azure\Impl' 'aveva-rocksdb-plugin-azure-impl-benchmark'
        Args = @()
    }
}

New-Item -ItemType Directory $OutDir -Force | Out-Null
$failed = $false
$exes = [ordered]@{}
foreach ($name in $targets.Keys) {
    $t = $targets[$name]
    $exes[$name] = Get-ExecutableInfo $t.Exe
    if ($exes[$name].configuration -notin 'Release', 'RelWithDebInfo', 'MinSizeRel') {
        Write-Warning "$name is a $($exes[$name].configuration) build: allocation counts and timings are not comparable with Release results."
    }
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
        counted = 'scalar operator new only (no aligned new, malloc, OpenSSL or OS allocations). http-client/azure-client: client thread only, server thread excluded. plugin: issuing threads and whole process, including the inline fake service'
    }
} | ConvertTo-Json -Depth 8 | Set-Content (Join-Path $OutDir 'loopback-metadata.json')

Write-Host "Logs and loopback-metadata.json: $OutDir"
if ($failed) { exit 1 }
