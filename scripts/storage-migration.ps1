#Requires -Version 7.0
#Requires -Modules Az.Accounts, @{ ModuleName='Az.Storage'; ModuleVersion='6.0.0' }

<#
.SYNOPSIS
    Memory-safe, streaming container-to-container blob migration with a review/approve gate.

.DESCRIPTION
    Earlier versions inventoried BOTH containers fully into in-memory dictionaries before
    diffing. On large production containers that exhausted the agent's RAM (the
    "Free memory is lower than 5%" warnings) and the job was killed.

    This version never holds the whole container in memory. It pages the SOURCE in fixed
    batches (-BatchSize, default 20), checks each source blob against the DESTINATION one at
    a time, writes the verdict straight to disk, then discards the batch and moves on. Peak
    memory is a flat ~one batch regardless of blob count, so it cannot crash the agent the
    way the full-inventory approach did. Because only actionable rows (copy / diverged) are
    written, disk usage is proportional to the change set, not the container size.

    Flow (mirrors the pipeline's plan -> approve -> migrate jobs):
      1. -WhatIf  : stream the plan to migration-plan.csv (+ copy list) and stop. Nothing copied.
      2. (human approves the published plan artifact)
      3. real run : stream the same plan, hand the full copy list to a single azcopy job
                    (azcopy streams the list with bounded memory), then stream-validate only
                    the copied blobs and fail on any divergence or mismatch.
#>

[CmdletBinding(SupportsShouldProcess)]
param(
    [Parameter(Mandatory)][string]$SourceSubscriptionId,
    [Parameter(Mandatory)][string]$SourceStorageAccount,
    [Parameter(Mandatory)][string]$SourceContainer,
    [Parameter(Mandatory)][string]$DestSubscriptionId,
    [Parameter(Mandatory)][string]$DestStorageAccount,
    [Parameter(Mandatory)][string]$DestContainer,

    # Blobs listed/checked per page. Small = lower flat memory, more round-trips (slower
    # planning). Larger = still flat but higher memory, fewer round-trips (faster planning).
    # The copy time is unaffected -- azcopy always gets the full list in one streamed job.
    [ValidateRange(1, 50000)][int]$BatchSize = 20,

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

# Extracts a blob's Content-MD5 as base64, or $null if the blob has none set.
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
    foreach ($pattern in @('azcopy-list-*', 'blobcheck_*', 'migration-copylist-*', 'migration-manifest-*')) {
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

# Looks up a single destination blob; returns the blob object or $null if it doesn't exist.
function Get-DestBlob {
    param(
        [Parameter(Mandatory)]$Context,
        [Parameter(Mandatory)][string]$Container,
        [Parameter(Mandatory)][string]$BlobName
    )
    $blob = Get-AzStorageBlob -Blob $BlobName -Container $Container -Context $Context -ErrorAction SilentlyContinue
    if (-not $blob) { return $null }
    return $blob
}

# Streams the source container in pages, classifies each blob against the destination, and
# writes the plan to disk. Holds only one page in memory at a time -- this is what keeps the
# agent from running out of RAM. Returns counters plus the paths it wrote.
function Build-MigrationPlan {
    [OutputType([pscustomobject])]
    param(
        [Parameter(Mandatory)]$SourceContext,
        [Parameter(Mandatory)][string]$SourceContainer,
        [Parameter(Mandatory)]$DestContext,
        [Parameter(Mandatory)][string]$DestContainer,
        [Parameter(Mandatory)][int]$BatchSize,
        [Parameter(Mandatory)][string]$PlanDirectory
    )
    $planCsvPath  = Join-Path $PlanDirectory 'migration-plan.csv'
    $copyListPath = Join-Path $PlanDirectory 'copy-list.txt'        # names only, for azcopy --list-of-files
    $manifestPath = Join-Path $PlanDirectory 'copy-manifest.tsv'    # name<TAB>size<TAB>md5, for post-copy validation
    $divergedPath = Join-Path $PlanDirectory 'diverged.csv'

    $csv      = [System.IO.StreamWriter]::new($planCsvPath, $false)
    $copyList = [System.IO.StreamWriter]::new($copyListPath, $false)
    $manifest = [System.IO.StreamWriter]::new($manifestPath, $false)
    $diverged = [System.IO.StreamWriter]::new($divergedPath, $false)

    $sourceCount = 0L; $copyCount = 0L; $divergedCount = 0L; $inSyncCount = 0L; [long]$copyBytes = 0
    $batchNumber = 0L
    $divergedSample = [System.Collections.Generic.List[string]]::new()

    try {
        $csv.WriteLine('BlobName,Action,BlobType,SizeBytes,Reason')
        $diverged.WriteLine('BlobName,BlobType,SourceSize,DestSize,Reason')

        $token = $null
        do {
            $page = @(Get-AzStorageBlob -Container $SourceContainer -Context $SourceContext `
                        -MaxCount $BatchSize -ContinuationToken $token -ErrorAction Stop)
            if ($page.Count -eq 0) { break }

            $batchNumber++
            $last = $page[$page.Count - 1]
            $token = if ($last.PSObject.Properties['ContinuationToken']) { $last.ContinuationToken } else { $null }

            foreach ($b in $page) {
                $name    = $b.Name
                $srcLen  = [long]$b.Length
                $srcType = [string]$b.BlobType
                $srcMd5  = Get-BlobMd5 $b
                $sourceCount++

                $dst = Get-DestBlob -Context $DestContext -Container $DestContainer -BlobName $name

                if ($null -eq $dst) {
                    $copyCount++; $copyBytes += $srcLen
                    $csv.WriteLine('{0},Copy,{1},{2},Missing from destination' -f (ConvertTo-CsvField $name), $srcType, $srcLen)
                    $copyList.WriteLine($name)
                    $manifest.WriteLine(("{0}`t{1}`t{2}" -f $name, $srcLen, $srcMd5))
                    continue
                }

                $dstLen = [long]$dst.Length
                $dstMd5 = Get-BlobMd5 $dst

                if ($srcLen -ne $dstLen) {
                    $divergedCount++
                    $reason = 'Size mismatch'
                    $csv.WriteLine('{0},Skip-Diverged,{1},{2},{3}' -f (ConvertTo-CsvField $name), $srcType, $srcLen, $reason)
                    $diverged.WriteLine('{0},{1},{2},{3},{4}' -f (ConvertTo-CsvField $name), $srcType, $srcLen, $dstLen, $reason)
                    if ($divergedSample.Count -lt 50) { $divergedSample.Add(("{0}  (source={1}, dest={2})" -f $name, (Format-FileSize $srcLen), (Format-FileSize $dstLen))) }
                }
                elseif ($srcMd5 -and $dstMd5 -and $srcMd5 -ne $dstMd5) {
                    $divergedCount++
                    $reason = 'MD5 mismatch'
                    $csv.WriteLine('{0},Skip-Diverged,{1},{2},{3}' -f (ConvertTo-CsvField $name), $srcType, $srcLen, $reason)
                    $diverged.WriteLine('{0},{1},{2},{3},{4}' -f (ConvertTo-CsvField $name), $srcType, $srcLen, $dstLen, $reason)
                    if ($divergedSample.Count -lt 50) { $divergedSample.Add(("{0}  (source-md5={1}, dest-md5={2})" -f $name, $srcMd5, $dstMd5)) }
                }
                else {
                    $inSyncCount++
                }
            }

            # Show the first 10 batches in full so you can watch the paging happen,
            # then throttle to roughly every 2,000 blobs so large runs don't flood the log.
            if ($batchNumber -le 10 -or ($sourceCount % ([long]$BatchSize * 100) -lt $page.Count)) {
                Write-Host ('       [batch {0}] {1} blob(s) this page, {2} scanned so far (copy {3}, in-sync {4}, diverged {5})' -f `
                    $batchNumber, $page.Count, $sourceCount, $copyCount, $inSyncCount, $divergedCount)
            }
        } while ($null -ne $token)
    }
    finally {
        $csv.Dispose(); $copyList.Dispose(); $manifest.Dispose(); $diverged.Dispose()
    }

    return [pscustomobject]@{
        SourceCount    = $sourceCount
        CopyCount      = $copyCount
        CopyBytes      = $copyBytes
        DivergedCount  = $divergedCount
        InSyncCount    = $inSyncCount
        PlanCsvPath    = $planCsvPath
        CopyListPath   = $copyListPath
        ManifestPath   = $manifestPath
        DivergedPath   = $divergedPath
        DivergedSample = $divergedSample
    }
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

# Streams the copy manifest and re-checks each copied blob at the destination by size (and
# MD5 when both sides have it). Reads one line at a time -- bounded memory. Returns succeeded
# / failed counts and a sample of failures.
function Test-CopiedBlobs {
    [OutputType([pscustomobject])]
    param(
        [Parameter(Mandatory)][string]$ManifestPath,
        [Parameter(Mandatory)]$DestContext,
        [Parameter(Mandatory)][string]$DestContainer
    )
    $succeeded = 0L; $failed = 0L
    $failedSample = [System.Collections.Generic.List[string]]::new()

    $reader = [System.IO.StreamReader]::new($ManifestPath)
    try {
        while ($null -ne ($line = $reader.ReadLine())) {
            if (-not $line) { continue }
            $parts   = $line -split "`t", 3
            $name    = $parts[0]
            $srcLen  = [long]$parts[1]
            $srcMd5  = if ($parts.Length -ge 3) { $parts[2] } else { '' }

            $dst = Get-DestBlob -Context $DestContext -Container $DestContainer -BlobName $name
            if ($null -eq $dst) {
                $failed++
                if ($failedSample.Count -lt 50) { $failedSample.Add("$name  (not present in destination)") }
                continue
            }
            if ([long]$dst.Length -ne $srcLen) {
                $failed++
                if ($failedSample.Count -lt 50) { $failedSample.Add("$name  (size mismatch after copy)") }
                continue
            }
            $dstMd5 = Get-BlobMd5 $dst
            if ($srcMd5 -and $dstMd5 -and $srcMd5 -ne $dstMd5) {
                $failed++
                if ($failedSample.Count -lt 50) { $failedSample.Add("$name  (MD5 mismatch after copy)") }
                continue
            }
            $succeeded++
        }
    }
    finally {
        $reader.Dispose()
    }

    return [pscustomobject]@{
        Succeeded    = $succeeded
        Failed       = $failed
        FailedSample = $failedSample
    }
}

# ── SCRIPT ────────────────────────────────────────────────────────────────────
$migrationStart = [datetime]::UtcNow

# Plan artifacts go to the published plan folder during -WhatIf so the approver can review
# them; on a real run they sit beside the azcopy logs. Scratch (copy list / manifest) lives
# under the plan dir too and is cleaned up in the finally block.
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

# ── PLAN ── stream the source, classify against the destination, write the plan to disk ─
$step++; Write-Host "[$step/$totalSteps] Building migration plan (streaming, $BatchSize blob(s) per page)..."
$plan = Build-MigrationPlan -SourceContext $sourceCtx -SourceContainer $SourceContainer `
                            -DestContext $destCtx -DestContainer $DestContainer `
                            -BatchSize $BatchSize -PlanDirectory $planDir

Write-Log 'PRE-MIGRATION PLAN -- what this run will do (source vs destination)'
Write-Host ('Source blobs scanned:      {0}' -f $plan.SourceCount)
Write-Host ('  To copy (missing):       {0}  ({1})' -f $plan.CopyCount, (Format-FileSize $plan.CopyBytes))
Write-Host ('  Already in sync:         {0}' -f $plan.InSyncCount)
Write-Host ('  Diverged (preserved):    {0}' -f $plan.DivergedCount)
Write-Host ''
Write-Host ('Plan written to: {0}' -f (Split-Path $plan.PlanCsvPath -Leaf))

if ($plan.DivergedCount -gt 0) {
    Write-Log 'DESTINATION DIVERGENCE DETECTED -- these blobs will NOT be copied, pipeline WILL fail'
    Write-Host (Colorize ("{0} blob(s) in '{1}' differ from source by size or MD5 and will be preserved (NOT overwritten)." -f $plan.DivergedCount, $DestContainer) $script:Ansi.Red)
    Write-Host ('Missing blobs are still copied below, but the run fails at the end with these listed.')
    Write-Host ('Full list: {0}' -f (Split-Path $plan.DivergedPath -Leaf))
    Write-Host ''
    $plan.DivergedSample | ForEach-Object { Write-Host (Colorize "  $_" $script:Ansi.Yellow) }
    if ($plan.DivergedCount -gt $plan.DivergedSample.Count) {
        Write-Host ('  ... and {0} more (see {1})' -f ($plan.DivergedCount - $plan.DivergedSample.Count), (Split-Path $plan.DivergedPath -Leaf))
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
        Write-Host ('Diverged blobs:  {0}' -f $plan.DivergedPath)
    }
    return
}

# ── VALIDATE ── re-check only the blobs we copied; bounded, streamed ────────────
$validation = $null
if ($plan.CopyCount -gt 0 -and -not $azCopyError) {
    Write-Log ("VALIDATION -- re-checking {0} copied blob(s) in '{1}' by size and MD5" -f $plan.CopyCount, $DestContainer)
    $validation = Test-CopiedBlobs -ManifestPath $plan.ManifestPath -DestContext $destCtx -DestContainer $DestContainer
    Write-Host ('  Verified in destination: {0}' -f $validation.Succeeded)
    if ($validation.Failed -gt 0) {
        Write-Host (Colorize ('  Failed to verify:        {0}' -f $validation.Failed) $script:Ansi.Red)
        $validation.FailedSample | ForEach-Object { Write-Host (Colorize "    $_" $script:Ansi.Red) }
        if ($validation.Failed -gt $validation.FailedSample.Count) {
            Write-Host (Colorize ('    ... and {0} more (see azcopy logs in {1})' -f ($validation.Failed - $validation.FailedSample.Count), $logDir) $script:Ansi.Red)
        }
    }
}

# ── SUMMARY ─────────────────────────────────────────────────────────────────────
Write-Log 'SUMMARY -- migration result'
Write-Host ('Source blobs:        {0}' -f $plan.SourceCount)
Write-Host ('Already in sync:     {0}' -f $plan.InSyncCount)
Write-Host ('To copy:             {0}' -f $plan.CopyCount)
if ($validation) {
    Write-Host ('  Migrated:          {0}' -f $validation.Succeeded)
    Write-Host ('  Failed:            {0}' -f $validation.Failed)
}
Write-Host ('Skipped (diverged):  {0}' -f $plan.DivergedCount)

$validationFailed = $validation -and $validation.Failed -gt 0
$overallPass = -not $azCopyError -and -not $validationFailed -and $plan.DivergedCount -eq 0
Write-Host ("Validation:          {0}" -f $(if ($overallPass) { Colorize 'PASS' $script:Ansi.Green } else { Colorize 'FAIL' $script:Ansi.Red }))

# Throws happen after the summary so the operator sees the full picture before the step exits.
if ($azCopyError) {
    Write-Log 'FAILURE: AZCOPY ERROR -- copy step reported a non-zero exit'
    Write-Host (Colorize $azCopyError.Exception.Message $script:Ansi.Red)
    throw $azCopyError.Exception
}
if ($validationFailed) {
    Write-Log 'FAILURE: VALIDATION MISMATCH -- some copied blobs did not reach the destination intact'
    throw "Validation failed: $($validation.Failed) copied blob(s) did not verify in '$DestContainer'. See VALIDATION above and azcopy logs in $logDir."
}
if ($plan.DivergedCount -gt 0) {
    Write-Log 'FAILURE: DESTINATION DIVERGENCE -- blobs differ from source and were preserved (not overwritten)'
    throw "Destination divergence: $($plan.DivergedCount) blob(s) in '$DestContainer' differ from source and were preserved. Reconcile manually (delete the diverged dest blob or fix the source) and re-run. See $($plan.DivergedPath)."
}

}
finally {
    # Remove machine-only scratch; keep the human-readable plan/diverged CSVs as artifacts.
    Remove-Item -LiteralPath (Join-Path $planDir 'copy-list.txt')     -Force -ErrorAction SilentlyContinue
    Remove-Item -LiteralPath (Join-Path $planDir 'copy-manifest.tsv') -Force -ErrorAction SilentlyContinue

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
