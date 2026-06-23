#Requires -Version 7.0
#Requires -Modules Az.Accounts, @{ ModuleName='Az.Storage'; ModuleVersion='6.0.0' }

[CmdletBinding(SupportsShouldProcess)]
param(
    [Parameter(Mandatory)][string]$SourceSubscriptionId,
    [Parameter(Mandatory)][string]$SourceStorageAccount,
    [Parameter(Mandatory)][string]$SourceContainer,
    [Parameter(Mandatory)][string]$DestSubscriptionId,
    [Parameter(Mandatory)][string]$DestStorageAccount,
    [Parameter(Mandatory)][string]$DestContainer,

    # Blobs listed per page from EACH container. Memory holds ~one page of each (so ~2x this),
    # which is a few MB even at the max. Bigger = fewer list round-trips = faster. Azure caps a
    # single list response at 5000, so there's no benefit above 5000.
    [ValidateRange(1, 5000)][int]$BatchSize = 5000,

    # Write the full per-blob ledgers (pre-blob-status.csv / post-blob-status.csv, including
    # DestOnly rows). On by default. The merge-join produces them in the same pass, so this is
    # cheap; set to $false only if you don't want the files.
    [bool]$BlobStatusReport = $true,

    # Cap how many missing blobs are copied in a single run (chunking). 0 = no limit (copy
    [ValidateRange(0, 2147483647)][long]$MaxFilesPerRun = 0,

    [string]$LogDirectory
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'

# ANSI colour codes -- red for failures, yellow for warnings. Headings use ##[section].
$script:Ansi = @{
    Reset  = "`e[0m"
    Red    = "`e[91m"
    Yellow = "`e[93m"
    Green  = "`e[92m"
}

# ── FUNCTIONS ─────────────────────────────────────────────────────────────────

# Emits an ADO ##[section] heading with a blank line prefix.
function Write-Log {
    param([Parameter(Mandatory)][ValidateNotNullOrEmpty()][string]$LogMessage)
    Write-Host ''
    Write-Host "##[section]$LogMessage"
}

# Wraps text in an ANSI color code; returns the text unchanged if no color is provided.
function Colorize {
    [OutputType([string])]
    param([string]$Text, [string]$Color)
    if (-not $Color) { return $Text }
    "$Color$Text$($script:Ansi.Reset)"
}

# Converts a byte count to a human-readable size string (B / KB / MB / GB).
function Format-FileSize {
    [OutputType([string])]
    param([long]$Bytes)
    if ($Bytes -ge 1GB) { return '{0:N2} GB' -f ($Bytes / 1GB) }
    if ($Bytes -ge 1MB) { return '{0:N2} MB' -f ($Bytes / 1MB) }
    if ($Bytes -ge 1KB) { return '{0:N2} KB' -f ($Bytes / 1KB) }
    return '{0} B' -f $Bytes
}

# Resolves and creates a directory from -Path, the ADO staging dir, or a temp path.
function Initialize-Directory {
    [OutputType([string])]
    param([string]$Path)
    if (-not (Test-Path $Path)) { New-Item -ItemType Directory -Path $Path -Force -WhatIf:$false | Out-Null }
    return $Path
}

# Quotes a value for CSV if it contains a comma, quote, or newline.
function ConvertTo-CsvField {
    [OutputType([string])]
    param([string]$Value)
    if (-not $Value) { return '' }
    if ($Value -match '[",\r\n]') { return '"' + ($Value -replace '"', '""') + '"' }
    return $Value
}

# Extracts a blob's Content-MD5 as base64, or $null if the blob has none set. Read straight
# from the bulk listing properties -- no extra call.
function Get-BlobMd5 {
    [OutputType([string])]
    param([Parameter(Mandatory)]$Blob)
    if ($Blob.PSObject.Properties['BlobProperties'] -and $Blob.BlobProperties -and $Blob.BlobProperties.ContentHash) {
        return [Convert]::ToBase64String($Blob.BlobProperties.ContentHash)
    }
    return $null
}

# Best-effort cleanup of our own scratch files left by previous runs. Matters on a
# persistent self-hosted agent: a hard OOM kill skips the finally block, so the NEXT run
# clears the orphans here instead of letting them accumulate on the VM forever.
function Clear-StaleScratch {
    $cutoff = (Get-Date).AddDays(-1)
    foreach ($pattern in @('azcopy-list-*', 'migration-copylist-*')) {
        Get-ChildItem -Path ([IO.Path]::GetTempPath()) -Filter $pattern -File -ErrorAction SilentlyContinue |
            Where-Object { $_.LastWriteTime -lt $cutoff } |
            Remove-Item -Force -ErrorAction SilentlyContinue
    }
}

# Prints available space on the volume backing $Path (informational; never throws).
function Show-FreeSpace {
    param([Parameter(Mandatory)][string]$Path)
    try {
        $root = [System.IO.Path]::GetPathRoot((Resolve-Path -LiteralPath $Path).Path)
        $drive = [System.IO.DriveInfo]::new($root)
        Write-Host ('Scratch volume {0}: {1} free of {2}' -f `
            $root, (Format-FileSize $drive.AvailableFreeSpace), (Format-FileSize $drive.TotalSize))
    }
    catch {
        Write-Host 'Scratch volume free space: unavailable on this platform.'
    }
}

# Throws if the named storage account is not visible in the current subscription.
function Assert-StorageAccountExists {
    param([Parameter(Mandatory)][ValidateNotNullOrEmpty()][string]$AccountName)
    $subscriptionId = (Get-AzContext).Subscription.Id
    $account = Get-AzResource -ResourceType 'Microsoft.Storage/storageAccounts' -Name $AccountName -ErrorAction SilentlyContinue
    if (-not $account) {
        throw "Storage account '$AccountName' does not exist in subscription '$subscriptionId', or the current identity does not have access to it."
    }
    Write-Host ("Storage account '{0}' found in subscription '{1}'." -f $AccountName, $subscriptionId)
}

# Throws if the named container can't be read in the given storage context.
function Assert-ContainerExists {
    param(
        [Parameter(Mandatory)]$Context,
        [Parameter(Mandatory)][ValidateNotNullOrEmpty()][string]$Container,
        [Parameter(Mandatory)][ValidateNotNullOrEmpty()][string]$AccountName
    )
    try {
        Get-AzStorageContainer -Name $Container -Context $Context -ErrorAction Stop | Out-Null
    }
    catch {
        throw ("Could not access container '{0}' in storage account '{1}'. " +
               "Check that the account name is correct and reachable, the container exists, " +
               "and the current identity has at least 'Storage Blob Data Reader' on it " +
               "(Contributor on the destination). Underlying error: {2}" -f $Container, $AccountName, $_.Exception.Message)
    }
    Write-Host ("Container '{0}' found in '{1}'." -f $Container, $AccountName)
}

# Classifies a matched (same-name) source/destination pair. Status strings match the old
# pre/post-blob-status.csv so downstream tooling doesn't change.
function Resolve-BlobStatus {
    [OutputType([string])]
    param(
        [long]$SrcLen,
        [AllowEmptyString()][string]$SrcMd5,
        [long]$DstLen,
        [AllowEmptyString()][string]$DstMd5
    )
    if ($SrcLen -ne $DstLen)           { return 'SizeMismatch' }
    if (-not $SrcMd5 -or -not $DstMd5) { return 'Matched (size only -- no MD5)' }
    if ($SrcMd5 -ne $DstMd5)           { return 'Md5Mismatch' }
    return 'Matched'
}

# Creates a lazy, paged cursor over a container's blobs (returned in sorted name order).
function New-BlobCursor {
    [OutputType([pscustomobject])]
    param(
        [Parameter(Mandatory)]$Context,
        [Parameter(Mandatory)][string]$Container,
        [Parameter(Mandatory)][int]$BatchSize
    )
    [pscustomobject]@{
        Context   = $Context
        Container = $Container
        BatchSize = $BatchSize
        Page      = @()
        Index     = 0
        Token     = $null
        Done      = $false
    }
}

# Returns the next blob from a cursor as a lightweight record (Name/Length/MD5/BlobType), or
# $null when the container is exhausted. Fetches a new page only when the current one runs out,
# so only one page per cursor is ever held in memory.
function Get-NextBlob {
    param([Parameter(Mandatory)]$Cursor)
    while ($true) {
        if ($Cursor.Index -lt $Cursor.Page.Count) {
            $rec = $Cursor.Page[$Cursor.Index]
            $Cursor.Index++
            return $rec
        }
        if ($Cursor.Done) { return $null }

        $page = @(Get-AzStorageBlob -Container $Cursor.Container -Context $Cursor.Context `
                    -MaxCount $Cursor.BatchSize -ContinuationToken $Cursor.Token -ErrorAction Stop)
        if ($page.Count -eq 0) { $Cursor.Done = $true; return $null }

        $last = $page[$page.Count - 1]
        $Cursor.Token = if ($last.PSObject.Properties['ContinuationToken']) { $last.ContinuationToken } else { $null }
        if ($null -eq $Cursor.Token) { $Cursor.Done = $true }

        # Project to small records and drop the heavy blob objects before looping back.
        $Cursor.Page = @($page | ForEach-Object {
            [pscustomobject]@{
                Name     = $_.Name
                Length   = [long]$_.Length
                MD5      = Get-BlobMd5 $_
                BlobType = [string]$_.BlobType
            }
        })
        $Cursor.Index = 0
    }
}

function Invoke-ContainerMergeJoin {
    [OutputType([pscustomobject])]
    param(
        [Parameter(Mandatory)]$SourceContext,
        [Parameter(Mandatory)][string]$SourceContainer,
        [Parameter(Mandatory)]$DestContext,
        [Parameter(Mandatory)][string]$DestContainer,
        [Parameter(Mandatory)][int]$BatchSize,
        [System.IO.StreamWriter]$PlanWriter,      # migration-plan.csv rows (Copy / Skip-Diverged)
        [System.IO.StreamWriter]$CopyListWriter,  # azcopy --list-of-files (names only)
        [System.IO.StreamWriter]$LedgerWriter,    # full per-blob status ledger
        [long]$MaxFilesPerRun = 0,                # cap on blobs added to the copy list (0 = no cap)
        [string]$ProgressLabel = 'scanned'
    )
    $sourceCount = 0L; $copyCount = 0L; $deferredCount = 0L; [long]$copyBytes = 0
    $sizeMismatch = 0L; $md5Mismatch = 0L; $matched = 0L; $destOnly = 0L
    $divergedSample = [System.Collections.Generic.List[string]]::new()
    $scanned = 0L; $reportInterval = 5000L; $nextReport = $reportInterval

    $srcCur = New-BlobCursor -Context $SourceContext -Container $SourceContainer -BatchSize $BatchSize
    $dstCur = New-BlobCursor -Context $DestContext   -Container $DestContainer   -BatchSize $BatchSize
    $src = Get-NextBlob $srcCur
    $dst = Get-NextBlob $dstCur

    while ($null -ne $src -or $null -ne $dst) {
        $cmp = if     ($null -eq $dst) { -1 }
               elseif ($null -eq $src) {  1 }
               else   { [string]::CompareOrdinal($src.Name, $dst.Name) }

        if ($cmp -lt 0) {
            # Source only -> missing from destination.
            $sourceCount++
            if ($MaxFilesPerRun -le 0 -or $copyCount -lt $MaxFilesPerRun) {
                # Within this run's budget -> copy it now.
                $copyCount++; $copyBytes += $src.Length
                if ($PlanWriter)     { $PlanWriter.WriteLine(('{0},Copy,{1},{2},Missing from destination' -f (ConvertTo-CsvField $src.Name), $src.BlobType, $src.Length)) }
                if ($CopyListWriter) { $CopyListWriter.WriteLine($src.Name) }
            }
            else {
                # Over this run's cap -> leave it for a later run.
                $deferredCount++
            }
            if ($LedgerWriter) { $LedgerWriter.WriteLine(('{0},{1},{2},,{3},,Missing' -f (ConvertTo-CsvField $src.Name), $src.BlobType, $src.Length, $src.MD5)) }
            $scanned++
            $src = Get-NextBlob $srcCur
        }
        elseif ($cmp -gt 0) {
            # Destination only -> preserved, not in source.
            $destOnly++
            if ($LedgerWriter) { $LedgerWriter.WriteLine(('{0},{1},,{2},,{3},DestOnly' -f (ConvertTo-CsvField $dst.Name), $dst.BlobType, $dst.Length, $dst.MD5)) }
            $scanned++
            $dst = Get-NextBlob $dstCur
        }
        else {
            # Same name in both -> compare size and MD5.
            $sourceCount++
            $status = Resolve-BlobStatus -SrcLen $src.Length -SrcMd5 $src.MD5 -DstLen $dst.Length -DstMd5 $dst.MD5
            switch ($status) {
                'SizeMismatch' {
                    $sizeMismatch++
                    if ($PlanWriter) { $PlanWriter.WriteLine(('{0},Skip-Diverged,{1},{2},Size mismatch' -f (ConvertTo-CsvField $src.Name), $src.BlobType, $src.Length)) }
                    if ($divergedSample.Count -lt 50) { $divergedSample.Add(("{0}  (source={1}, dest={2})" -f $src.Name, (Format-FileSize $src.Length), (Format-FileSize $dst.Length))) }
                }
                'Md5Mismatch' {
                    $md5Mismatch++
                    if ($PlanWriter) { $PlanWriter.WriteLine(('{0},Skip-Diverged,{1},{2},MD5 mismatch' -f (ConvertTo-CsvField $src.Name), $src.BlobType, $src.Length)) }
                    if ($divergedSample.Count -lt 50) { $divergedSample.Add(("{0}  (source-md5={1}, dest-md5={2})" -f $src.Name, $src.MD5, $dst.MD5)) }
                }
                default {
                    # 'Matched' or 'Matched (size only -- no MD5)'
                    $matched++
                }
            }
            if ($LedgerWriter) {
                $LedgerWriter.WriteLine(('{0},{1},{2},{3},{4},{5},{6}' -f (ConvertTo-CsvField $src.Name), $src.BlobType, $src.Length, $dst.Length, $src.MD5, $dst.MD5, $status))
            }
            $scanned += 2
            $src = Get-NextBlob $srcCur
            $dst = Get-NextBlob $dstCur
        }

        if ($scanned -ge $nextReport) {
            Write-Host ('       {0} {1} source / {2} dest-only blob(s)... (copy {3}, in-sync {4}, diverged {5})' -f `
                $ProgressLabel, $sourceCount, $destOnly, $copyCount, $matched, ($sizeMismatch + $md5Mismatch))
            $nextReport += $reportInterval
        }
    }

    [pscustomobject]@{
        SourceCount       = $sourceCount
        CopyCount         = $copyCount
        DeferredCount     = $deferredCount
        CopyBytes         = $copyBytes
        InSyncCount       = $matched
        SizeMismatchCount = $sizeMismatch
        Md5MismatchCount  = $md5Mismatch
        DivergedCount     = ($sizeMismatch + $md5Mismatch)
        DestOnlyCount     = $destOnly
        DivergedSample    = $divergedSample
    }
}

# Builds the migration plan (and optional pre-migration ledger) by merge-joining the two
# containers. Returns counters plus the paths it wrote.
function Build-MigrationPlan {
    [OutputType([pscustomobject])]
    param(
        [Parameter(Mandatory)]$SourceContext,
        [Parameter(Mandatory)][string]$SourceContainer,
        [Parameter(Mandatory)]$DestContext,
        [Parameter(Mandatory)][string]$DestContainer,
        [Parameter(Mandatory)][int]$BatchSize,
        [Parameter(Mandatory)][string]$PlanDirectory,
        # When set, also write the full per-blob ledger (every blob + DestOnly rows) here.
        [string]$StatusCsvPath,
        # Cap on blobs copied this run (0 = no cap). The rest become DeferredCount.
        [long]$MaxFilesPerRun = 0
    )
    $planCsvPath  = Join-Path $PlanDirectory 'migration-plan.csv'
    $copyListPath = Join-Path $PlanDirectory 'copy-list.txt'        # names only, for azcopy --list-of-files

    $csv      = [System.IO.StreamWriter]::new($planCsvPath, $false)
    $copyList = [System.IO.StreamWriter]::new($copyListPath, $false)
    $ledger   = if ($StatusCsvPath) { [System.IO.StreamWriter]::new($StatusCsvPath, $false) } else { $null }

    try {
        $csv.WriteLine('BlobName,Action,BlobType,SizeBytes,Reason')
        if ($ledger) { $ledger.WriteLine('Name,BlobType,SourceSize,DestSize,SourceMD5,DestMD5,Status') }

        $r = Invoke-ContainerMergeJoin -SourceContext $SourceContext -SourceContainer $SourceContainer `
                                       -DestContext $DestContext -DestContainer $DestContainer `
                                       -BatchSize $BatchSize `
                                       -PlanWriter $csv -CopyListWriter $copyList -LedgerWriter $ledger `
                                       -MaxFilesPerRun $MaxFilesPerRun -ProgressLabel 'scanned'
    }
    finally {
        $csv.Dispose(); $copyList.Dispose()
        if ($ledger) { $ledger.Dispose() }
    }

    [pscustomobject]@{
        SourceCount    = $r.SourceCount
        CopyCount      = $r.CopyCount
        DeferredCount  = $r.DeferredCount
        CopyBytes      = $r.CopyBytes
        DivergedCount  = $r.DivergedCount
        InSyncCount    = $r.InSyncCount
        DestOnlyCount  = $r.DestOnlyCount
        PlanCsvPath    = $planCsvPath
        CopyListPath   = $copyListPath
        StatusCsvPath  = $StatusCsvPath
        DivergedSample = $r.DivergedSample
    }
}

# Re-compares the two containers after the copy (same merge-join) to validate the result and,
# optionally, write the post-migration ledger. Returns the merge-join counters.
function Get-PostMigrationStatus {
    [OutputType([pscustomobject])]
    param(
        [Parameter(Mandatory)]$SourceContext,
        [Parameter(Mandatory)][string]$SourceContainer,
        [Parameter(Mandatory)]$DestContext,
        [Parameter(Mandatory)][string]$DestContainer,
        [Parameter(Mandatory)][int]$BatchSize,
        [string]$StatusCsvPath
    )
    $ledger = if ($StatusCsvPath) { [System.IO.StreamWriter]::new($StatusCsvPath, $false) } else { $null }
    try {
        if ($ledger) { $ledger.WriteLine('Name,BlobType,SourceSize,DestSize,SourceMD5,DestMD5,Status') }
        $r = Invoke-ContainerMergeJoin -SourceContext $SourceContext -SourceContainer $SourceContainer `
                                       -DestContext $DestContext -DestContainer $DestContainer `
                                       -BatchSize $BatchSize -LedgerWriter $ledger -ProgressLabel 'verified'
    }
    finally {
        if ($ledger) { $ledger.Dispose() }
    }
    return $r
}

# Runs a single azcopy job over the whole copy list. azcopy streams the list file and keeps
# its job plan on disk, so memory stays bounded no matter how many blobs are listed.
function Invoke-AzCopyByListFile {
    param(
        [Parameter(Mandatory)][ValidateNotNullOrEmpty()][string]$SourceAccount,
        [Parameter(Mandatory)][ValidateNotNullOrEmpty()][string]$SourceContainer,
        [Parameter(Mandatory)][ValidateNotNullOrEmpty()][string]$DestAccount,
        [Parameter(Mandatory)][ValidateNotNullOrEmpty()][string]$DestContainer,
        [Parameter(Mandatory)][ValidateNotNullOrEmpty()][string]$ListFile
    )
    $sourceUrl = "https://$SourceAccount.blob.core.windows.net/$SourceContainer"
    $destUrl   = "https://$DestAccount.blob.core.windows.net/$DestContainer"

    $state = @{ LastProgress = [datetime]::UtcNow }
    & azcopy copy $sourceUrl $destUrl `
         --list-of-files $ListFile `
         --s2s-preserve-blob-tags 2>&1 | ForEach-Object {
        if ("$_".Trim() -match '^\d+(\.\d+)?\s*%') {
            $now = [datetime]::UtcNow
            if (($now - $state.LastProgress).TotalSeconds -lt 60) { return }
            $state.LastProgress = $now
        }
        Write-Host $_
    }

    $exit = $LASTEXITCODE
    if ($exit -ne 0) {
        throw "AzCopy copy failed with exit code $exit. See logs in $env:AZCOPY_LOG_LOCATION"
    }
}

# ── SCRIPT ────────────────────────────────────────────────────────────────────
$migrationStart = [datetime]::UtcNow

# Plan artifacts go to the published plan folder; the post ledger sits beside the azcopy logs.
# Scratch (copy list) lives under the plan dir and is cleaned up in the finally block.
$planDir = if ($env:BUILD_ARTIFACTSTAGINGDIRECTORY) {
    Join-Path $env:BUILD_ARTIFACTSTAGINGDIRECTORY 'migration-plan'
} else {
    Join-Path ([IO.Path]::GetTempPath()) "migration-plan-$(Get-Date -Format yyyyMMddHHmmss)"
}
$logDir = if ($LogDirectory) { $LogDirectory }
          elseif ($env:BUILD_ARTIFACTSTAGINGDIRECTORY) { Join-Path $env:BUILD_ARTIFACTSTAGINGDIRECTORY 'azcopy-logs' }
          else { Join-Path ([IO.Path]::GetTempPath()) "azcopy-logs-$(Get-Date -Format yyyyMMddHHmmss)" }

$planDir = Initialize-Directory -Path $planDir
$logDir  = Initialize-Directory -Path $logDir

# PSCRED inherits the Az PowerShell session established by AzurePowerShell@5.
$env:AZCOPY_AUTO_LOGIN_TYPE   = 'PSCRED'
$env:AZCOPY_LOG_LOCATION      = $logDir
$env:AZCOPY_JOB_PLAN_LOCATION = $logDir

if (-not (Get-Command azcopy -ErrorAction SilentlyContinue)) {
    throw 'azcopy executable not found in PATH. Install it on the agent (e.g. via the AzureCLI@2 task or a dedicated AzCopy install step) before running this script.'
}

Clear-StaleScratch

try {

Write-Log 'MIGRATION CONFIGURATION -- source and destination details for this run'
Write-Host ('Start time:        {0:yyyy-MM-dd HH:mm:ss} UTC' -f $migrationStart)
Write-Host ('Batch size:        {0} blob(s) per page' -f $BatchSize)
if ($MaxFilesPerRun -gt 0) {
    Write-Host ('Max files/run:     {0} (chunked -- re-run the pipeline until nothing remains)' -f $MaxFilesPerRun)
}
Write-Host ('Plan directory:    {0}' -f $planDir)
Write-Host ('Log directory:     {0}' -f $logDir)
Show-FreeSpace -Path $planDir
Write-Host ''
Write-Host 'Source'
Write-Host ('  Subscription:    {0}' -f $SourceSubscriptionId)
Write-Host ('  Storage account: {0}' -f $SourceStorageAccount)
Write-Host ('  Container:       {0}' -f $SourceContainer)
Write-Host ('  URL:             https://{0}.blob.core.windows.net/{1}' -f $SourceStorageAccount, $SourceContainer)
Write-Host ''
Write-Host 'Destination'
Write-Host ('  Subscription:    {0}' -f $DestSubscriptionId)
Write-Host ('  Storage account: {0}' -f $DestStorageAccount)
Write-Host ('  Container:       {0}' -f $DestContainer)
Write-Host ('  URL:             https://{0}.blob.core.windows.net/{1}' -f $DestStorageAccount, $DestContainer)


# ── PRE-CHECKS ── verify accounts and containers exist ──────────────────────────
Write-Log 'PREPARATION CHECKS -- verifying storage accounts and containers before migration'

$step = 0; $totalSteps = 3

$step++; Write-Host "[$step/$totalSteps] Verifying source storage account and container exist..."
$null = Set-AzContext -SubscriptionId $SourceSubscriptionId -ErrorAction Stop -WarningAction SilentlyContinue -WhatIf:$false
Assert-StorageAccountExists -AccountName $SourceStorageAccount
$sourceCtx = New-AzStorageContext -StorageAccountName $SourceStorageAccount -UseConnectedAccount
Assert-ContainerExists -Context $sourceCtx -Container $SourceContainer -AccountName $SourceStorageAccount

$step++; Write-Host "[$step/$totalSteps] Verifying destination storage account and container exist..."
$null = Set-AzContext -SubscriptionId $DestSubscriptionId -ErrorAction Stop -WarningAction SilentlyContinue -WhatIf:$false
Assert-StorageAccountExists -AccountName $DestStorageAccount
$destCtx = New-AzStorageContext -StorageAccountName $DestStorageAccount -UseConnectedAccount
Assert-ContainerExists -Context $destCtx -Container $DestContainer -AccountName $DestStorageAccount

# ── PLAN ── merge-join the two containers and write the plan to disk ─────────────
$step++; Write-Host "[$step/$totalSteps] Building migration plan (merge-join, $BatchSize blob(s) per page)..."
$preStatusCsv = if ($BlobStatusReport) { Join-Path $planDir 'pre-blob-status.csv' } else { '' }
$plan = Build-MigrationPlan -SourceContext $sourceCtx -SourceContainer $SourceContainer `
                            -DestContext $destCtx -DestContainer $DestContainer `
                            -BatchSize $BatchSize -PlanDirectory $planDir `
                            -StatusCsvPath $preStatusCsv -MaxFilesPerRun $MaxFilesPerRun

Write-Log 'PRE-MIGRATION PLAN -- what this run will do (source vs destination)'
Write-Host ('Source blobs scanned:      {0}' -f $plan.SourceCount)
Write-Host ('  To copy (this run):      {0}  ({1})' -f $plan.CopyCount, (Format-FileSize $plan.CopyBytes))
if ($plan.DeferredCount -gt 0) {
    Write-Host (Colorize ('  Deferred (later runs):   {0} (over the {1}/run cap -- re-run to continue)' -f $plan.DeferredCount, $MaxFilesPerRun) $script:Ansi.Yellow)
}
Write-Host ('  Already in sync:         {0}' -f $plan.InSyncCount)
Write-Host ('  Diverged (preserved):    {0}' -f $plan.DivergedCount)
Write-Host ('  Destination-only:        {0} (preserved, not in source)' -f $plan.DestOnlyCount)
Write-Host ''
Write-Host ('Plan written to: {0}' -f (Split-Path $plan.PlanCsvPath -Leaf))
if ($BlobStatusReport) {
    Write-Host ('Full per-blob ledger:  {0}' -f (Split-Path $plan.StatusCsvPath -Leaf))
}

if ($plan.DivergedCount -gt 0) {
    Write-Log 'DESTINATION DIVERGENCE DETECTED -- these blobs will NOT be copied, pipeline WILL fail'
    Write-Host (Colorize ("{0} blob(s) in '{1}' differ from source by size or MD5 and will be preserved (NOT overwritten)." -f $plan.DivergedCount, $DestContainer) $script:Ansi.Red)
    Write-Host ('Missing blobs are still copied below, but the run fails at the end with these listed.')
    Write-Host ('Full list: the Skip-Diverged rows in {0}' -f (Split-Path $plan.PlanCsvPath -Leaf))
    Write-Host ''
    $plan.DivergedSample | ForEach-Object { Write-Host (Colorize "  $_" $script:Ansi.Yellow) }
    if ($plan.DivergedCount -gt $plan.DivergedSample.Count) {
        Write-Host ('  ... and {0} more (see the Skip-Diverged rows in {1})' -f ($plan.DivergedCount - $plan.DivergedSample.Count), (Split-Path $plan.PlanCsvPath -Leaf))
    }
}

# ── MIGRATE ── copy the missing blobs (diverged ones are skipped/preserved) ─────
$azCopyError = $null

if ($plan.CopyCount -eq 0) {
    Write-Log 'NOTHING TO COPY -- destination already has every source blob (diverged blobs preserved).'
}
elseif ($PSCmdlet.ShouldProcess(
        "$($plan.CopyCount) blob(s) to copy ($($plan.DivergedCount) diverged, preserved)",
        "Copy to $DestStorageAccount/$DestContainer")) {

    Write-Log ('MIGRATING -- copying {0} blob(s) ({1}) from {2} to {3}' -f $plan.CopyCount, (Format-FileSize $plan.CopyBytes), $SourceContainer, $DestContainer)
    try {
        Invoke-AzCopyByListFile -SourceAccount $SourceStorageAccount -SourceContainer $SourceContainer `
                                -DestAccount $DestStorageAccount -DestContainer $DestContainer `
                                -ListFile $plan.CopyListPath
    }
    catch {
        $azCopyError = $_
        Write-Warning $azCopyError.Exception.Message
    }
}
else {
    # -WhatIf: the plan is already on disk for the approver. Stop here without copying.
    Write-Log 'DRY RUN (-WhatIf) -- no data copied. Review the published migration plan and approve to proceed.'
    Write-Host ('Migration plan:  {0}' -f $plan.PlanCsvPath)
    if ($plan.DivergedCount -gt 0) {
        Write-Host ('Diverged blobs:  {0} Skip-Diverged row(s) in {1}' -f $plan.DivergedCount, $plan.PlanCsvPath)
    }
    return
}

# ── VALIDATE ── merge-join again after the copy to confirm the result (and write post ledger) ─
$post = $null
if ($plan.CopyCount -gt 0 -and -not $azCopyError) {
    $postStatusCsv = if ($BlobStatusReport) { Join-Path $logDir 'post-blob-status.csv' } else { '' }
    Write-Log ("VALIDATION -- re-comparing '{0}' to '{1}' after copy (merge-join)" -f $SourceContainer, $DestContainer)
    $post = Get-PostMigrationStatus -SourceContext $sourceCtx -SourceContainer $SourceContainer `
                                    -DestContext $destCtx -DestContainer $DestContainer `
                                    -BatchSize $BatchSize -StatusCsvPath $postStatusCsv
    if ($BlobStatusReport) {
        Write-Host ('  Post-migration ledger:   {0}' -f (Split-Path $postStatusCsv -Leaf))
        Write-Host ('  Destination-only blobs:  {0}' -f $post.DestOnlyCount)
    }
}

# ── SUMMARY ─────────────────────────────────────────────────────────────────────
# In chunked mode (-MaxFilesPerRun), the blobs still missing after a copy are the DEFERRED
# ones (intended -- re-run to continue) plus any that actually failed to land. Anything
# missing beyond the deferred count is a real copy failure.
$deferred      = $plan.DeferredCount
$stillMissing  = if ($post) { $post.CopyCount } else { $plan.DeferredCount }
$failedCount   = [Math]::Max(0, $stillMissing - $deferred)
$succeededCount = $plan.CopyCount - $failedCount
$remaining     = $stillMissing                       # to be copied in future runs
$divergedFinal = if ($post) { $post.DivergedCount } else { $plan.DivergedCount }

Write-Log 'SUMMARY -- migration result'
Write-Host ('Source blobs:        {0}' -f $plan.SourceCount)
Write-Host ('Already in sync:     {0}' -f $plan.InSyncCount)
Write-Host ('Copied this run:     {0}' -f $plan.CopyCount)
if ($post) {
    Write-Host ('  Verified:          {0}' -f $succeededCount)
    if ($failedCount -gt 0) { Write-Host (Colorize ('  Failed:            {0}' -f $failedCount) $script:Ansi.Red) }
}
if ($remaining -gt 0 -and $failedCount -eq 0) {
    Write-Host (Colorize ('Remaining (re-run):  {0}' -f $remaining) $script:Ansi.Yellow)
}
Write-Host ('Skipped (diverged):  {0}' -f $divergedFinal)

$overallPass = (-not $azCopyError) -and ($failedCount -eq 0) -and ($divergedFinal -eq 0)
$resultText = if (-not $overallPass) { Colorize 'FAIL' $script:Ansi.Red }
              elseif ($remaining -gt 0) { Colorize ('PARTIAL -- {0} copied, {1} remaining; re-run the pipeline to continue' -f $plan.CopyCount, $remaining) $script:Ansi.Yellow }
              else { Colorize 'PASS' $script:Ansi.Green }
Write-Host ("Result:              {0}" -f $resultText)

# Throws happen after the summary so the operator sees the full picture before the step exits.
# A non-zero $remaining (deferred) is NOT a failure -- the pipeline succeeds so you can re-run.
if ($azCopyError) {
    Write-Log 'FAILURE: AZCOPY ERROR -- copy step reported a non-zero exit'
    Write-Host (Colorize $azCopyError.Exception.Message $script:Ansi.Red)
    throw $azCopyError.Exception
}
if ($failedCount -gt 0) {
    Write-Log 'FAILURE: VALIDATION MISMATCH -- some copied blobs did not reach the destination'
    throw "Validation failed: $failedCount copied blob(s) are still missing from '$DestContainer' after the copy (beyond the $deferred deferred for later runs). See azcopy logs in $logDir."
}
if ($divergedFinal -gt 0) {
    Write-Log 'FAILURE: DESTINATION DIVERGENCE -- blobs differ from source and were preserved (not overwritten)'
    throw "Destination divergence: $divergedFinal blob(s) in '$DestContainer' differ from source and were preserved. Reconcile manually (delete the diverged dest blob or fix the source) and re-run. See the Skip-Diverged rows in $($plan.PlanCsvPath)."
}

}
finally {
    # Remove machine-only scratch; keep the human-readable plan/ledger CSVs as artifacts.
    Remove-Item -LiteralPath (Join-Path $planDir 'copy-list.txt') -Force -ErrorAction SilentlyContinue

    $migrationEnd = [datetime]::UtcNow
    $elapsed = $migrationEnd - $migrationStart
    $elapsedText = if ($elapsed.TotalDays -ge 1) {
        '{0} day(s) {1:hh\:mm\:ss}' -f [int]$elapsed.TotalDays, $elapsed
    } else {
        '{0:hh\:mm\:ss}' -f $elapsed
    }

    Write-Log 'MIGRATION & VALIDATION TIME -- total elapsed duration for this run'
    Write-Host ('Migration started:  {0:yyyy-MM-dd HH:mm:ss} UTC' -f $migrationStart)
    Write-Host ('Migration ended:    {0:yyyy-MM-dd HH:mm:ss} UTC' -f $migrationEnd)
    Write-Host ('Total time elapsed: {0}' -f $elapsedText)
}
