# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright 2026 AVEVA
<#
.SYNOPSIS
    Runs a db_bench suite against real Azure Blob Storage for the Azure SDK backend (main) and the Boost.Asio
    backend (this branch), then prints a side-by-side table.

.DESCRIPTION
    Needs two db_bench executables: -SdkExe (built from main with the tool backported) and -AsioExe (this branch).
    Credentials come from AZURE_STORAGE_ACCOUNT_NAME, AZURE_TENANT_ID, AZURE_SERVICE_PRINCIPAL_ID and
    AZURE_SERVICE_PRINCIPAL_SECRET (never printed). Each backend uses its own container, <ContainerPrefix>-sdk and
    <ContainerPrefix>-asio; they must already exist. Every run uses a fresh blob path and the test blobs are left
    behind, so delete the containers when finished.

    Scenarios (latency in ms/op unless noted; lower is better):
      write-*           single-thread fillseq / fillrandom / overwrite (network round trip bound)
      load+compact      fillrandom then waitforcompaction, wall-clock seconds (SST write pipelining)
      readrandom-cold   point reads with a tiny block cache
      seek-*            iterator scans (seek + 1000 nexts), async_io off/on
      multiget-*        batched MultiGet (32 keys), async_io off/on (exercises MultiRead)

.EXAMPLE
    .\Compare-Backends.ps1 -SdkExe D:\bench-main\...\db_bench.exe -AsioExe D:\repo\build\WindowsRelease\tools\db_bench\db_bench.exe

.EXAMPLE
    .\Compare-Backends.ps1 -SdkExe ... -AsioExe ... -Quick -Scenarios seek,multiget
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory)] [string] $SdkExe,
    [Parameter(Mandatory)] [string] $AsioExe,
    [string] $Account = $env:AZURE_STORAGE_ACCOUNT_NAME,
    [string] $ContainerPrefix = 'bench',
    [int] $Num = 200000,
    [int] $Reps = 2,
    [ValidateSet('write', 'load', 'readrandom', 'seek', 'multiget')]
    [string[]] $Scenarios = @('write', 'load', 'readrandom', 'seek', 'multiget'),
    # Smaller data set and fewer ops: a smoke run of a few minutes.
    [switch] $Quick,
    [string] $OutDir = (Join-Path ([IO.Path]::GetTempPath()) ("db_bench_compare_" + (Get-Date -Format yyyyMMdd_HHmmss)))
)

$ErrorActionPreference = 'Stop'
foreach ($exe in $SdkExe, $AsioExe) {
    if (-not (Test-Path $exe)) { throw "db_bench not found: $exe" }
}
foreach ($v in 'AZURE_TENANT_ID', 'AZURE_SERVICE_PRINCIPAL_ID', 'AZURE_SERVICE_PRINCIPAL_SECRET') {
    if (-not (Get-Item "env:$v" -ErrorAction SilentlyContinue)) { throw "Environment variable $v is not set" }
}
if (-not $Account) { throw 'Storage account not set (use -Account or AZURE_STORAGE_ACCOUNT_NAME)' }

if ($Quick) {
    $Num = 20000
    $Reps = 1
}
$scale = if ($Quick) { 0.2 } else { 1.0 }
function Scaled([int]$n) { [Math]::Max(5, [int]($n * $scale)) }

New-Item -ItemType Directory $OutDir -Force | Out-Null
$env:AVEVA_DB_BENCH_STORAGE_ACCOUNT_URL = "https://$Account.blob.core.windows.net"
$env:AVEVA_DB_BENCH_TENANT_ID = $env:AZURE_TENANT_ID
$env:AVEVA_DB_BENCH_CLIENT_ID = $env:AZURE_SERVICE_PRINCIPAL_ID
$env:AVEVA_DB_BENCH_CLIENT_SECRET = $env:AZURE_SERVICE_PRINCIPAL_SECRET

$backends = [ordered]@{ sdk = $SdkExe; asio = $AsioExe }
# The SDK backend registers as azblobfs<container>; the asio plugin name is derived from the account, so the wrapper
# resolves it when given 'auto'.
function FsUri($b) { if ($b -eq 'sdk') { "azblobfs$ContainerPrefix-sdk" } else { 'auto' } }
function UseContainer($b) { $env:AVEVA_DB_BENCH_CONTAINER = "$ContainerPrefix-$b" }
function NewDb($b, $tag) { "$Account+$ContainerPrefix-$b/cmp-$tag-$([guid]::NewGuid().ToString('N').Substring(0, 6))" }

$common = '--compression_type=zlib', '--value_size=1024', '--histogram=1'
# Small levels spread the data across several LSM levels so reads touch more than one file.
$shape = '--write_buffer_size=4194304', '--target_file_size_base=4194304', '--max_bytes_for_level_base=8388608',
         '--max_bytes_for_level_multiplier=10', '--bloom_bits=10', '--bytes_per_sync=1048576'

# Results: scenario -> backend -> list of values. Order of scenarios is preserved for printing.
$results = [ordered]@{}
function Record($scenario, $backend, $value) {
    if (-not $results.Contains($scenario)) { $results[$scenario] = @{ sdk = @(); asio = @() } }
    if ($null -ne $value) { $results[$scenario][$backend] += $value }
}

$script:logIndex = 0
# Runs db_bench; returns the parsed "<name> : <x> micros/op" values by benchmark name, or $null on failure.
function Invoke-Bench($backend, $label, [string[]]$benchArgs) {
    UseContainer $backend
    $script:logIndex++
    $log = Join-Path $OutDir ("{0:D3}-{1}-{2}.txt" -f $script:logIndex, $backend, ($label -replace '[^\w.-]', '_'))
    Write-Host ("  [{0}] {1} ..." -f $backend, $label) -NoNewline
    $sw = [Diagnostics.Stopwatch]::StartNew()
    & $backends[$backend] @benchArgs --fs_uri=$(FsUri $backend) *> $log
    $rc = $LASTEXITCODE
    Write-Host (" rc={0} {1:N0}s" -f $rc, $sw.Elapsed.TotalSeconds)
    if ($rc -ne 0) { return $null }

    $parsed = @{ Seconds = $sw.Elapsed.TotalSeconds }
    # db_bench wraps long lines, so match the "<name> : <value> micros/op" prefix only.
    foreach ($m in Select-String -Path $log -Pattern '^(\w+)\s+:\s+([\d.]+) micros/op') {
        $parsed[$m.Matches[0].Groups[1].Value] = [double]$m.Matches[0].Groups[2].Value
    }
    $parsed
}

# Loads a DB (retrying: the SDK backend's bulk load occasionally crashes) and returns its path, plus load time.
function New-LoadedDb($backend, $tag) {
    for ($attempt = 1; $attempt -le 3; $attempt++) {
        $db = NewDb $backend $tag
        $r = Invoke-Bench $backend "load-$tag" (@('--benchmarks=fillrandom,waitforcompaction', "--num=$Num",
                '--disable_wal=1', '--statistics=0') + $shape + $common + "--db=$db")
        if ($r) { return @{ Db = $db; Seconds = $r.Seconds } }
        Write-Host "  load failed (attempt $attempt)" -ForegroundColor Yellow
    }
    $null
}

$sw0 = [Diagnostics.Stopwatch]::StartNew()
Write-Host "Output: $OutDir`nAccount: $Account  num=$Num reps=$Reps quick=$Quick`n"

for ($rep = 1; $rep -le $Reps; $rep++) {
    # Alternate which backend goes first so drift in the network or account throttling doesn't favor one.
    $order = if ($rep % 2) { 'sdk', 'asio' } else { 'asio', 'sdk' }
    Write-Host "== Rep $rep (order: $($order -join ', '))"
    $dbs = @{}

    if ('write' -in $Scenarios) {
        foreach ($b in $order) {
            $r = Invoke-Bench $b 'write' (@('--benchmarks=fillseq,fillrandom,overwrite', "--num=$(Scaled 500)",
                    '--threads=1', '--statistics=0', '--disable_wal=0') + $common + "--db=$(NewDb $b 'write')")
            if ($r) { foreach ($k in 'fillseq', 'fillrandom', 'overwrite') { Record "write-$k (ms/op)" $b ($r[$k] / 1000) } }
        }
    }

    if ('load' -in $Scenarios -or 'readrandom' -in $Scenarios -or 'seek' -in $Scenarios -or 'multiget' -in $Scenarios) {
        foreach ($b in $order) {
            $l = New-LoadedDb $b 'data'
            if ($l) {
                $dbs[$b] = $l.Db
                Record 'load+compact (s)' $b $l.Seconds
            }
        }
    }

    $readCommon = @('--use_existing_db=1', "--num=$Num", '--cache_size=8388608', '--statistics=1') + $common

    if ('readrandom' -in $Scenarios) {
        foreach ($b in $order) {
            if (-not $dbs[$b]) { continue }
            $r = Invoke-Bench $b 'readrandom' ($readCommon + @('--benchmarks=readrandom', "--reads=$(Scaled 300)",
                    '--threads=1', "--db=$($dbs[$b])"))
            if ($r) { Record 'readrandom-cold (ms/op)' $b ($r['readrandom'] / 1000) }
        }
    }

    if ('seek' -in $Scenarios) {
        foreach ($threads in 1, 8) {
            foreach ($async in 0, 1) {
                foreach ($b in $order) {
                    if (-not $dbs[$b]) { continue }
                    $r = Invoke-Bench $b "seek-t$threads-async$async" ($readCommon + @('--benchmarks=seekrandom',
                            '--seek_nexts=1000', "--reads=$(Scaled 50)", "--threads=$threads", '--adaptive_readahead=1',
                            "--async_io=$async", "--db=$($dbs[$b])"))
                    if ($r) { Record "seek t$threads async_io=$async (ms/op)" $b ($r['seekrandom'] / 1000) }
                }
            }
        }
    }

    if ('multiget' -in $Scenarios) {
        foreach ($async in 0, 1) {
            foreach ($b in $order) {
                if (-not $dbs[$b]) { continue }
                $r = Invoke-Bench $b "multiget-async$async" ($readCommon + @('--benchmarks=multireadrandom',
                        '--multiread_batched=1', '--batch_size=32', "--reads=$(Scaled 1000)", '--threads=1',
                        "--async_io=$async", "--db=$($dbs[$b])"))
                if ($r) { Record "multiget async_io=$async (ms/key)" $b ($r['multireadrandom'] / 1000) }
            }
        }
    }
}

function Mean($values) { if ($values.Count) { ($values | Measure-Object -Average).Average } else { $null } }

$rows = foreach ($name in $results.Keys) {
    $s = Mean $results[$name].sdk
    $a = Mean $results[$name].asio
    [pscustomobject]@{
        Scenario = $name
        'sdk (main)' = if ($null -ne $s) { '{0:N1}' -f $s } else { 'n/a' }
        'asio (branch)' = if ($null -ne $a) { '{0:N1}' -f $a } else { 'n/a' }
        'asio vs sdk' = if ($null -ne $s -and $null -ne $a -and $s -ne 0) { '{0:+0;-0;0}%' -f (($a - $s) / $s * 100) } else { 'n/a' }
        Samples = '{0}/{1}' -f $results[$name].sdk.Count, $results[$name].asio.Count
    }
}

Write-Host "`n=== Results (mean of reps; lower is better; negative % = asio faster) ==="
$rows | Format-Table -AutoSize | Out-String -Width 200 | Write-Host
$rows | Export-Csv (Join-Path $OutDir 'summary.csv') -NoTypeInformation
Write-Host ("Total time {0:N0} min. Logs and summary.csv: {1}" -f $sw0.Elapsed.TotalMinutes, $OutDir)
Write-Host "Test blobs remain in containers $ContainerPrefix-sdk and $ContainerPrefix-asio; delete them when finished."
