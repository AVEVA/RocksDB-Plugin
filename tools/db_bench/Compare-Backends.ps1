# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright 2026 AVEVA
#Requires -Version 7.2
<#
.SYNOPSIS
    Runs a db_bench suite against real Azure Blob Storage for a baseline build and a candidate build, then prints a
    side-by-side table.

.DESCRIPTION
    Needs two db_bench executables: -BaselineExe (for example built from main) and -CandidateExe (for example this
    branch). Credentials come from AZURE_STORAGE_ACCOUNT_NAME, AZURE_TENANT_ID, AZURE_SERVICE_PRINCIPAL_ID and
    AZURE_SERVICE_PRINCIPAL_SECRET (never printed). Each side uses its own container, <ContainerPrefix>-baseline and
    <ContainerPrefix>-candidate; they must already exist. Every run uses a fresh blob path and the test blobs are left
    behind, so delete the containers when finished.

    -BaselineFsUri / -CandidateFsUri are the --fs_uri values passed to each executable. 'auto' makes the wrapper use
    the plugin name it registered; the default baseline value is the name the Azure SDK backend registers.

    Setup (see tools/db_bench/README.md): configure a preset with -DAVEVA_ROCKSDB_BUILD_DB_BENCH=ON, e.g.
      cmake --preset WindowsRelease -DAVEVA_ROCKSDB_BUILD_DB_BENCH=ON
    which adds the vcpkg overlay infrastructure/vcpkg-overlays (equivalent to VCPKG_OVERLAY_PORTS=<repo>/infrastructure/vcpkg-overlays/rocksdb).
    The service principal needs 'Storage Blob Data Contributor' on the storage account or on both containers.

    Outputs: per-run db_bench logs (NNN-<backend>-<label>.txt) and summary.csv are written to -OutDir (default: a
    db_bench_compare_<timestamp> folder under the user's temp directory). They are never deleted automatically; remove
    the folder when finished. The client secret is passed only through an environment variable, is never echoed, and
    is scrubbed from every log before this script moves on; summary.csv holds only aggregated timings.

    Scenarios (latency in ms/op unless noted; lower is better):
      write-*           single-thread fillseq / fillrandom / overwrite (network round trip bound)
      load+compact      fillrandom then waitforcompaction, wall-clock seconds (SST write pipelining)
      readrandom-cold   point reads with a tiny block cache
      seek-*            iterator scans (seek + 1000 nexts), async_io off/on
      multiget-*        batched MultiGet (32 keys), async_io off/on (exercises MultiRead)

.EXAMPLE
    .\Compare-Backends.ps1 -BaselineExe D:\bench-main\...\db_bench.exe -CandidateExe D:\repo\build\WindowsRelease\tools\db_bench\db_bench.exe

.EXAMPLE
    .\Compare-Backends.ps1 -BaselineExe ... -CandidateExe ... -Quick -Scenarios seek,multiget
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory)] [string] $BaselineExe,
    [Parameter(Mandatory)] [string] $CandidateExe,
    [string] $Account = $env:AZURE_STORAGE_ACCOUNT_NAME,
    [string] $ContainerPrefix = 'bench',
    [string] $BaselineFsUri = "azblobfs$ContainerPrefix-baseline",
    [string] $CandidateFsUri = 'auto',
    [int] $Num = 200000,
    [int] $Reps = 2,
    [ValidateSet('write', 'load', 'readrandom', 'seek', 'multiget')]
    [string[]] $Scenarios = @('write', 'load', 'readrandom', 'seek', 'multiget'),
    # Smaller data set and fewer ops: a smoke run of a few minutes.
    [switch] $Quick,
    [string] $OutDir = (Join-Path ([IO.Path]::GetTempPath()) ("db_bench_compare_" + (Get-Date -Format yyyyMMdd_HHmmss)))
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest
foreach ($exe in $BaselineExe, $CandidateExe) {
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

$backends = [ordered]@{ baseline = $BaselineExe; candidate = $CandidateExe }
function FsUri($b) { if ($b -eq 'baseline') { $BaselineFsUri } else { $CandidateFsUri } }
function UseContainer($b) { $env:AVEVA_DB_BENCH_CONTAINER = "$ContainerPrefix-$b" }
function NewDb($b, $tag) { "$Account+$ContainerPrefix-$b/cmp-$tag-$([guid]::NewGuid().ToString('N').Substring(0, 6))" }

$common = '--compression_type=zlib', '--value_size=1024', '--histogram=1'
# Small levels spread the data across several LSM levels so reads touch more than one file.
$shape = '--write_buffer_size=4194304', '--target_file_size_base=4194304', '--max_bytes_for_level_base=8388608',
         '--max_bytes_for_level_multiplier=10', '--bloom_bits=10', '--bytes_per_sync=1048576'

# Results: scenario -> backend -> list of values. Order of scenarios is preserved for printing.
$results = [ordered]@{}
function Record($scenario, $backend, $value) {
    if (-not $results.Contains($scenario)) { $results[$scenario] = @{ baseline = @(); candidate = @() } }
    if ($null -ne $value) { $results[$scenario][$backend] += $value }
}

$script:logIndex = 0
# Defense in depth: db_bench output is not expected to contain the secret, but a log must never be able to leak it.
function Remove-Secret([string]$path) {
    $secret = $env:AVEVA_DB_BENCH_CLIENT_SECRET
    if (-not $secret -or -not (Test-Path $path)) { return }
    $text = [IO.File]::ReadAllText($path)
    if ($text.Contains($secret)) { [IO.File]::WriteAllText($path, $text.Replace($secret, '***REDACTED***')) }
}
# Every failed or unparsable run is kept here so the comparison is reported as incomplete instead of looking clean.
$script:failures = [System.Collections.Generic.List[string]]::new()
function Add-Failure($backend, $label, $reason) {
    $script:failures.Add("[$backend] $label : $reason")
    Write-Host "  FAILED [$backend] ${label}: $reason" -ForegroundColor Red
}
# Runs db_bench; returns the parsed "<name> : <x> micros/op" values by benchmark name, or $null on failure (nonzero
# exit, or any name in $expect missing or not a finite number). Logs are kept either way.
function Invoke-Bench($backend, $label, [string[]]$benchArgs, [string[]]$expect = @()) {
    UseContainer $backend
    $script:logIndex++
    $log = Join-Path $OutDir ("{0:D3}-{1}-{2}.txt" -f $script:logIndex, $backend, ($label -replace '[^\w.-]', '_'))
    Write-Host ("  [{0}] {1} ..." -f $backend, $label) -NoNewline
    $sw = [Diagnostics.Stopwatch]::StartNew()
    & $backends[$backend] @benchArgs --fs_uri=$(FsUri $backend) *> $log
    $rc = $LASTEXITCODE
    Remove-Secret $log
    Write-Host (" rc={0} {1:N0}s" -f $rc, $sw.Elapsed.TotalSeconds)
    if ($rc -ne 0) { Add-Failure $backend $label "exit code $rc (log: $log)"; return $null }

    $parsed = @{ Seconds = $sw.Elapsed.TotalSeconds }
    # db_bench wraps long lines, so match the "<name> : <value> micros/op" prefix only.
    foreach ($m in Select-String -Path $log -Pattern '^(\w+)\s+:\s+(\d+(?:\.\d+)?) micros/op') {
        $parsed[$m.Matches[0].Groups[1].Value] = [double]$m.Matches[0].Groups[2].Value
    }
    $missing = @($expect | Where-Object { -not $parsed.ContainsKey($_) })
    if ($missing) { Add-Failure $backend $label "missing or malformed metric(s) $($missing -join ', ') (log: $log)"; return $null }
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
    Record 'load+compact (s)' $backend $null
    $null
}

$sw0 = [Diagnostics.Stopwatch]::StartNew()
Write-Host "Output: $OutDir`nAccount: $Account  num=$Num reps=$Reps quick=$Quick`n"

try {
for ($rep = 1; $rep -le $Reps; $rep++) {
    # Alternate which backend goes first so drift in the network or account throttling doesn't favor one.
    $order = if ($rep % 2) { 'baseline', 'candidate' } else { 'candidate', 'baseline' }
    Write-Host "== Rep $rep (order: $($order -join ', '))"
    $dbs = @{}

    if ('write' -in $Scenarios) {
        foreach ($b in $order) {
            $r = Invoke-Bench $b 'write' (@('--benchmarks=fillseq,fillrandom,overwrite', "--num=$(Scaled 500)",
                    '--threads=1',                     '--statistics=0', '--disable_wal=0') + $common + "--db=$(NewDb $b 'write')") 'fillseq', 'fillrandom', 'overwrite'
                                foreach ($k in 'fillseq', 'fillrandom', 'overwrite') { Record "write-$k (ms/op)" $b $(if ($r) { $r[$k] / 1000 }) }
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
            if (-not $dbs[$b]) { Record 'readrandom-cold (ms/op)' $b $null; continue }
            $r = Invoke-Bench $b 'readrandom' ($readCommon + @('--benchmarks=readrandom', "--reads=$(Scaled 300)",
                    '--threads=1', "--db=$($dbs[$b])")) 'readrandom'
            Record 'readrandom-cold (ms/op)' $b $(if ($r) { $r['readrandom'] / 1000 })
        }
    }

    if ('seek' -in $Scenarios) {
        foreach ($threads in 1, 8) {
            foreach ($async in 0, 1) {
                foreach ($b in $order) {
                    $name = "seek t$threads async_io=$async (ms/op)"
                    if (-not $dbs[$b]) { Record $name $b $null; continue }
                    $r = Invoke-Bench $b "seek-t$threads-async$async" ($readCommon + @('--benchmarks=seekrandom',
                            '--seek_nexts=1000', "--reads=$(Scaled 50)", "--threads=$threads", '--adaptive_readahead=1',
                            "--async_io=$async", "--db=$($dbs[$b])")) 'seekrandom'
                    Record $name $b $(if ($r) { $r['seekrandom'] / 1000 })
                }
            }
        }
    }

    if ('multiget' -in $Scenarios) {
        foreach ($async in 0, 1) {
            foreach ($b in $order) {
                $name = "multiget async_io=$async (ms/key)"
                if (-not $dbs[$b]) { Record $name $b $null; continue }
                $r = Invoke-Bench $b "multiget-async$async" ($readCommon + @('--benchmarks=multireadrandom',
                        '--multiread_batched=1', '--batch_size=32', "--reads=$(Scaled 1000)", '--threads=1',
                        "--async_io=$async", "--db=$($dbs[$b])")) 'multireadrandom'
                Record $name $b $(if ($r) { $r['multireadrandom'] / 1000 })
            }
        }
    }
}
}
finally {
    # Don't leave the credentials in the caller's session environment.
    foreach ($v in 'AVEVA_DB_BENCH_CLIENT_SECRET', 'AVEVA_DB_BENCH_CLIENT_ID', 'AVEVA_DB_BENCH_TENANT_ID') {
        Remove-Item "env:$v" -ErrorAction SilentlyContinue
    }
}

function Mean($values) { if ($values.Count) { ($values | Measure-Object -Average).Average } else { $null } }

$rows = foreach ($name in $results.Keys) {
    $s = Mean $results[$name].baseline
    $a = Mean $results[$name].candidate
    [pscustomobject]@{
        Scenario = $name
        'baseline' = if ($null -ne $s) { '{0:N1}' -f $s } else { 'n/a' }
        'candidate' = if ($null -ne $a) { '{0:N1}' -f $a } else { 'n/a' }
        'candidate vs baseline' = if ($null -ne $s -and $null -ne $a -and $s -ne 0) { '{0:+0;-0;0}%' -f (($a - $s) / $s * 100) } else { 'n/a' }
        Samples = '{0}/{1}' -f $results[$name].baseline.Count, $results[$name].candidate.Count
        Status = if ($results[$name].baseline.Count -eq $Reps -and $results[$name].candidate.Count -eq $Reps) { 'complete' } else { 'INCOMPLETE' }
    }
}

Write-Host "`n=== Results (mean of reps; lower is better; negative % = candidate faster) ==="
$rows | Format-Table -AutoSize | Out-String -Width 200 | Write-Host
$rows | Export-Csv (Join-Path $OutDir 'summary.csv') -NoTypeInformation
Write-Host ("Total time {0:N0} min. Logs and summary.csv: {1}" -f $sw0.Elapsed.TotalMinutes, $OutDir)
Write-Host "Test blobs remain in containers $ContainerPrefix-baseline and $ContainerPrefix-candidate; delete them when finished."

$incomplete = @($rows | Where-Object Status -ne 'complete')
if ($script:failures.Count -or $incomplete.Count -or -not $rows) {
    $script:failures | Set-Content (Join-Path $OutDir 'failures.txt')
    Write-Host ("INCOMPLETE comparison: {0} failed run(s), {1} scenario(s) without all {2} paired sample(s). See failures.txt and the logs in {3}." -f
        $script:failures.Count, $incomplete.Count, $Reps, $OutDir) -ForegroundColor Red
    exit 1
}
