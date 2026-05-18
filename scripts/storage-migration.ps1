#Requires -Version 7.0
#Requires -Modules Az.Accounts, Az.Storage

[CmdletBinding(SupportsShouldProcess)]
param(
    [Parameter(Mandatory)][string]$SourceSubscriptionId,
    [Parameter(Mandatory)][string]$SourceStorageAccount,
    [Parameter(Mandatory)][string]$SourceContainer,
    [Parameter(Mandatory)][string]$DestSubscriptionId,
    [Parameter(Mandatory)][string]$DestStorageAccount,
    [Parameter(Mandatory)][string]$DestContainer,

    [string]$LogDirectory
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'

# ANSI escapes for the one color we still use (red, for failures). Section
# headings rely on Azure DevOps's ##[section] marker instead of ANSI so they
# render correctly in artifact viewers / plain-text downloads.
$script:Ansi = @{
    Reset = "`e[0m"
    Red   = "`e[91m"
}

# Functions
function Write-Log {
    param([string]$LogMessage)
    Write-Host ''
    Write-Host "##[section]$LogMessage"
}

function Initialize-LogDirectory {
    param([string]$Log)
    if ($Log) { $dir = $Log }
    elseif ($env:BUILD_ARTIFACTSTAGINGDIRECTORY) {
        $dir = Join-Path $env:BUILD_ARTIFACTSTAGINGDIRECTORY 'azcopy-logs'
    }
    else { $dir = Join-Path ([IO.Path]::GetTempPath()) "azcopy-logs-$(Get-Date -Format yyyyMMddHHmmss)" }

    if (-not (Test-Path $dir)) { New-Item -ItemType Directory -Path $dir -Force | Out-Null }
    return $dir
}

function Format-FileSize {
    param([long]$Bytes)
    if ($Bytes -ge 1GB) { return '{0:N2} GB' -f ($Bytes / 1GB) }
    if ($Bytes -ge 1MB) { return '{0:N2} MB' -f ($Bytes / 1MB) }
    if ($Bytes -ge 1KB) { return '{0:N2} KB' -f ($Bytes / 1KB) }
    return '{0} B' -f $Bytes
}

function Format-Cell {
    param([string]$Text, [int]$Width)
    if ($Text.Length -gt $Width) { return $Text.Substring(0, $Width - 1) + '…' }
    return $Text
}

function Assert-StorageAccountExists {
    param(
        [Parameter(Mandatory)][string]$AccountName,
        [Parameter(Mandatory)][string]$SubscriptionId
    )
    # Targeted ARM lookup -- avoids enumerating every storage account in the
    # subscription (slow + requires broader RBAC than necessary).
    $account = Get-AzResource -ResourceType 'Microsoft.Storage/storageAccounts' -Name $AccountName -ErrorAction SilentlyContinue
    if (-not $account) {
        throw "Storage account '$AccountName' does not exist in subscription '$SubscriptionId', or the current identity does not have access to it."
    }
    Write-Host ("Storage account '{0}' found in subscription '{1}'." -f $AccountName, $SubscriptionId)
}

function Assert-ContainerExists {
    param(
        [Parameter(Mandatory)]$Context,
        [Parameter(Mandatory)][string]$Container,
        [Parameter(Mandatory)][string]$AccountName
    )
    $exists = Get-AzStorageContainer -Name $Container -Context $Context -ErrorAction SilentlyContinue
    if (-not $exists) {
        throw "Container '$Container' does not exist in storage account '$AccountName'. Ensure the container exists before running this migration."
    }
    Write-Host ("Container '{0}' found in '{1}'." -f $Container, $AccountName)
}

function Get-BlobInventory {
    param(
        [Parameter(Mandatory)]$Context,
        [Parameter(Mandatory)][string]$Container
    )
    # Case-sensitive: Azure blob names are case-sensitive but PowerShell's default
    # @{} hashtable is not, which would silently collapse 'File.txt' and 'file.txt'.
    $map = [System.Collections.Generic.Dictionary[string,object]]::new([System.StringComparer]::Ordinal)
    $count = 0
    Get-AzStorageBlob -Container $Container -Context $Context -ErrorAction Stop |
    ForEach-Object {
        $md5 = $null
        if ($_.PSObject.Properties['BlobProperties'] -and $_.BlobProperties -and $_.BlobProperties.ContentHash) {
            $md5 = [Convert]::ToBase64String($_.BlobProperties.ContentHash)
        }
        $map[$_.Name] = [pscustomobject]@{
            Name     = $_.Name
            Length   = $_.Length
            MD5      = $md5
            BlobType = [string]$_.BlobType
        }
        $count++
        if ($count % 10000 -eq 0) { Write-Host " inventoried $count blobs..." }
    }
    return $map
}

function Show-BlobList {
    param(
        [string]$Heading,
        [string[]]$Names,
        [System.Collections.IDictionary]$SizeSource,
        [int]$Limit = 50,
        [string]$Color = ''
    )
    if ($Names.Count -eq 0) { return }
    $shown = [Math]::Min($Names.Count, $Limit)
    Write-Host ''
    if ($Color) {
        Write-Host ("  {0}{1}:{2}" -f $Color, $Heading, $script:Ansi.Reset)
    } else {
        Write-Host ('  {0}:' -f $Heading)
    }
    $Names | Sort-Object | Select-Object -First $shown | ForEach-Object {
        Write-Host ('    {0,-50} {1,-10} {2,10}' -f (Format-Cell $_ 50), $SizeSource[$_].BlobType, (Format-FileSize $SizeSource[$_].Length))
    }
    if ($Names.Count -gt $shown) { Write-Host "    ... and $($Names.Count - $shown) more" }
}

function Get-BlobClassification {
    # Single source of truth for how source vs destination blobs are bucketed.
    # Both Compare-Migration (console reporting) and Test-MigrationCompleteness
    # (validation) consume this so the rules can't drift.
    param(
        [Parameter(Mandatory)][System.Collections.IDictionary]$Source,
        [Parameter(Mandatory)][System.Collections.IDictionary]$Destination
    )
    $missing      = New-Object System.Collections.Generic.List[string]
    $sizeMismatch = New-Object System.Collections.Generic.List[string]
    $md5Mismatch  = New-Object System.Collections.Generic.List[string]
    $noMd5        = New-Object System.Collections.Generic.List[string]
    $matched      = New-Object System.Collections.Generic.List[string]

    foreach ($name in $Source.Keys) {
        if (-not $Destination.ContainsKey($name)) {
            $missing.Add($name)
            continue
        }
        $srcBlob = $Source[$name]
        $dstBlob = $Destination[$name]
        if ($srcBlob.Length -ne $dstBlob.Length) {
            $sizeMismatch.Add($name)
            continue
        }
        if (-not $srcBlob.MD5 -or -not $dstBlob.MD5) {
            $noMd5.Add($name)
            continue
        }
        if ($srcBlob.MD5 -ne $dstBlob.MD5) {
            $md5Mismatch.Add($name)
        } else {
            $matched.Add($name)
        }
    }

    $destOnly = @($Destination.Keys | Where-Object { -not $Source.ContainsKey($_) })

    return [pscustomobject]@{
        SourceCount       = $Source.Count
        DestCount         = $Destination.Count
        MissingNames      = @($missing)
        SizeMismatchNames = @($sizeMismatch)
        Md5MismatchNames  = @($md5Mismatch)
        NoMd5Names        = @($noMd5)
        MatchedNames      = @($matched)
        DestOnlyNames     = $destOnly
    }
}

function Compare-Migration {
    param(
        [System.Collections.IDictionary]$Source,
        [System.Collections.IDictionary]$Destination,
        [string]$Label = 'Status',
        [string]$SourceContainer = 'source',
        [string]$DestContainer = 'destination'
    )
    $c = Get-BlobClassification -Source $Source -Destination $Destination

    # noMd5 blobs (size matches but one side lacks Content-MD5) count as
    # already-in-sync for pending/migrated math -- there is no checksum to
    # disprove a content match, so we treat size as sufficient evidence.
    $alreadyInSync = @($c.MatchedNames) + @($c.NoMd5Names)
    $outOfSync = @($c.SizeMismatchNames) + @($c.Md5MismatchNames)
    $pending = @($c.MissingNames) + $outOfSync

    $pendingPct = if ($c.SourceCount -gt 0) {
        [math]::Round(($pending.Count / $c.SourceCount) * 100, 2)
    } else { 0 }
    $migratedPct = [math]::Round(100 - $pendingPct, 2)

    Write-Log $Label
    Write-Host ("Blobs in '{0}': {1}" -f $SourceContainer, $c.SourceCount)
    Write-Host ("Blobs in '{0}': {1}" -f $DestContainer, $c.DestCount)
    Write-Host ('Already migrated:  {0} ({1}%)' -f $alreadyInSync.Count, $migratedPct)
    Write-Host ('Pending:           {0} ({1}%)  [missing: {2}, size-mismatch: {3}, md5-mismatch: {4}]' -f `
        $pending.Count, $pendingPct, $c.MissingNames.Count, $c.SizeMismatchNames.Count, $c.Md5MismatchNames.Count)

    Show-BlobList -Heading ("Not in $DestContainer")     -Names $c.MissingNames      -SizeSource $Source -Color $script:Ansi.Red
    Show-BlobList -Heading 'Out of sync (size differs)'  -Names $c.SizeMismatchNames -SizeSource $Source
    Show-BlobList -Heading 'Out of sync (MD5 differs)'   -Names $c.Md5MismatchNames  -SizeSource $Source
    Show-BlobList -Heading ("Already in $DestContainer") -Names $alreadyInSync       -SizeSource $Source

    return [pscustomobject]@{
        SourceCount       = $c.SourceCount
        DestCount         = $c.DestCount
        PendingCount      = $pending.Count
        PendingPercent    = $pendingPct
        PendingNames      = $pending
        MissingNames      = $c.MissingNames
        OutOfSyncNames    = $outOfSync
        SizeMismatchNames = $c.SizeMismatchNames
        Md5MismatchNames  = $c.Md5MismatchNames
        MatchedNames      = $alreadyInSync
    }
}

function Invoke-AzCopySync {
    param(
        [string]$SourceAccount,
        [string]$SourceContainer,
        [string]$DestAccount,
        [string]$DestContainer
    )
    $sourceUrl = "https://$SourceAccount.blob.core.windows.net/$SourceContainer"
    $destUrl = "https://$DestAccount.blob.core.windows.net/$DestContainer"

    $azArgs = @(
        'sync', $sourceUrl, $destUrl,
        '--recursive=true',
        '--delete-destination=false',
        '--put-md5'
    )

    # Capture combined stdout+stderr so we can dump it on failure -- without
    # this, AzCopy's actual error reason is invisible in the pipeline log and
    # we only see our own generic "exit code N" message. Full raw output is
    # also preserved in $env:AZCOPY_LOG_LOCATION.
    $output = & azcopy @azArgs 2>&1
    if ($LASTEXITCODE -ne 0) {
        $output | ForEach-Object { Write-Host $_ }
        throw "AzCopy sync failed with exit code $LASTEXITCODE. See logs in $env:AZCOPY_LOG_LOCATION"
    }
}

function Test-MigrationCompleteness {
    param([System.Collections.IDictionary]$Source, [System.Collections.IDictionary]$Destination)

    $c = Get-BlobClassification -Source $Source -Destination $Destination

    $issues = New-Object System.Collections.Generic.List[string]
    if ($c.MissingNames.Count -gt 0) {
        $issues.Add("Missing from destination: $($c.MissingNames.Count) blob(s) -- present in source but never reached destination")
        $c.MissingNames | Select-Object -First 10 | ForEach-Object { $issues.Add(" - $_") }
    }
    if ($c.SizeMismatchNames.Count -gt 0) {
        $issues.Add("Size mismatch: $($c.SizeMismatchNames.Count) blob(s) -- byte count differs between source and destination")
        $c.SizeMismatchNames | Select-Object -First 10 | ForEach-Object { $issues.Add(" - $_") }
    }
    if ($c.Md5MismatchNames.Count -gt 0) {
        $issues.Add("MD5 mismatch: $($c.Md5MismatchNames.Count) blob(s) -- content checksum differs between source and destination")
        $c.Md5MismatchNames | Select-Object -First 10 | ForEach-Object { $issues.Add(" - $_") }
    }

    # Destination-only blobs: present in destination but not in source. Expected
    # with --delete-destination=false, so informational rather than a failure.
    return [pscustomobject]@{
        Passed            = ($issues.Count -eq 0)
        Issues            = $issues
        SourceCount       = $c.SourceCount
        DestCount         = $c.DestCount
        NoMd5Count        = $c.NoMd5Names.Count
        Md5VerifiedCount  = $c.MatchedNames.Count
        Md5MismatchCount  = $c.Md5MismatchNames.Count
        MissingNames      = $c.MissingNames
        SizeMismatchNames = $c.SizeMismatchNames
        Md5MismatchNames  = $c.Md5MismatchNames
        DestOnlyCount     = $c.DestOnlyNames.Count
        DestOnlyNames     = $c.DestOnlyNames
    }
}

function Show-BlobComparison {
    param(
        [System.Collections.IDictionary]$Source,
        [System.Collections.IDictionary]$Destination,
        [string]$SourceContainer = 'source',
        [string]$DestContainer = 'destination',
        [string]$LogDirectory = ''
    )
    $base = '  {0,-50} {1,-10} {2,20} {3,20}'
    $tail = '  {0,-8}  {1,-8}'
    Write-Host (($base -f 'Blob Name', 'Type', (Format-Cell $SourceContainer 20), (Format-Cell $DestContainer 20)) + ($tail -f 'Size', 'MD5'))
    Write-Host (($base -f ('-' * 50), ('-' * 10), ('-' * 20), ('-' * 20)) + ($tail -f ('-' * 8), ('-' * 8)))

    # Totals are computed across all blobs; only the first $limit rows are
    # printed to keep the pipeline log readable on large migrations. Full per-
    # blob detail is in the AzCopy artifact at $env:AZCOPY_LOG_LOCATION.
    [long]$totalSrc = 0
    [long]$totalDst = 0
    foreach ($name in $Source.Keys) {
        $totalSrc += $Source[$name].Length
        if ($Destination.ContainsKey($name)) {
            $totalDst += $Destination[$name].Length
        }
    }

    $limit = 100
    $names = @($Source.Keys | Sort-Object)
    $shown = [Math]::Min($names.Count, $limit)
    for ($i = 0; $i -lt $shown; $i++) {
        $name = $names[$i]
        $srcBlob = $Source[$name]
        $srcSize = $srcBlob.Length

        if ($Destination.ContainsKey($name)) {
            $dstBlob = $Destination[$name]
            $dstSize = $dstBlob.Length

            $sizeCell = if ($srcSize -eq $dstSize) {
                '{0,-8}' -f 'OK'
            } else {
                "$($script:Ansi.Red){0,-8}$($script:Ansi.Reset)" -f 'MISMATCH'
            }

            $md5Cell = if (-not $srcBlob.MD5 -or -not $dstBlob.MD5) {
                '{0,-8}' -f 'N/A'
            } elseif ($srcBlob.MD5 -eq $dstBlob.MD5) {
                '{0,-8}' -f 'OK'
            } else {
                "$($script:Ansi.Red){0,-8}$($script:Ansi.Reset)" -f 'MISMATCH'
            }

            Write-Host (($base -f (Format-Cell $name 50), $srcBlob.BlobType, (Format-FileSize $srcSize), (Format-FileSize $dstSize)) + "  $sizeCell  $md5Cell")
        }
        else {
            $sizeCell = "$($script:Ansi.Red){0,-8}$($script:Ansi.Reset)" -f 'MISSING'
            $md5Cell  = "$($script:Ansi.Red){0,-8}$($script:Ansi.Reset)" -f '-'
            Write-Host (($base -f (Format-Cell $name 50), $srcBlob.BlobType, (Format-FileSize $srcSize), 'MISSING') + "  $sizeCell  $md5Cell")
        }
    }
    if ($names.Count -gt $shown) {
        Write-Host ('    ... and {0} more (see AzCopy logs in {1})' -f ($names.Count - $shown), $LogDirectory)
    }
    Write-Host (($base -f ('-' * 50), ('-' * 10), ('-' * 20), ('-' * 20)) + ($tail -f ('-' * 8), ('-' * 8)))
    Write-Host ($base -f 'TOTAL', '', (Format-FileSize $totalSrc), (Format-FileSize $totalDst))
}

# Script
$migrationStart = [datetime]::UtcNow
$logDir = Initialize-LogDirectory -Log $LogDirectory

# PSCRED inherits the Az PowerShell session established by AzurePowerShell@5.
$env:AZCOPY_AUTO_LOGIN_TYPE = 'PSCRED'
$env:AZCOPY_LOG_LOCATION = $logDir
$env:AZCOPY_JOB_PLAN_LOCATION = $logDir

# All work is wrapped so the MIGRATION TIME block always fires, including on
# early returns ("nothing to migrate", -WhatIf) and on uncaught exceptions.
try {

Write-Log 'MIGRATION CONFIGURATION -- source and destination details for this run'
Write-Host ('Start time:        {0:yyyy-MM-dd HH:mm:ss} UTC' -f $migrationStart)
Write-Host ('Log directory:     {0}' -f $logDir)
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

Write-Log 'PREPARATION CHECKS -- verifying storage accounts, containers, and inventorying blobs before migration'

Write-Host '[1/3] Verifying source storage account and container exist...'
$null = Set-AzContext -SubscriptionId $SourceSubscriptionId -WarningAction SilentlyContinue
Assert-StorageAccountExists -AccountName $SourceStorageAccount -SubscriptionId $SourceSubscriptionId
$sourceCtx = New-AzStorageContext -StorageAccountName $SourceStorageAccount -UseConnectedAccount
Assert-ContainerExists -Context $sourceCtx -Container $SourceContainer -AccountName $SourceStorageAccount

Write-Host '[2/3] Verifying destination storage account and container exist...'
$null = Set-AzContext -SubscriptionId $DestSubscriptionId -WarningAction SilentlyContinue
Assert-StorageAccountExists -AccountName $DestStorageAccount -SubscriptionId $DestSubscriptionId
$destCtx = New-AzStorageContext -StorageAccountName $DestStorageAccount -UseConnectedAccount
Assert-ContainerExists -Context $destCtx -Container $DestContainer -AccountName $DestStorageAccount

Write-Host '[3/3] Inventorying source and destination containers...'
$null = Set-AzContext -SubscriptionId $SourceSubscriptionId -WarningAction SilentlyContinue
$sourceInventory = Get-BlobInventory -Context $sourceCtx -Container $SourceContainer
$null = Set-AzContext -SubscriptionId $DestSubscriptionId -WarningAction SilentlyContinue
$destInventoryBefore = Get-BlobInventory -Context $destCtx -Container $DestContainer
Write-Host ('       Source inventoried:      {0} blob(s)' -f $sourceInventory.Count)
Write-Host ('       Destination inventoried: {0} blob(s)' -f $destInventoryBefore.Count)

$preStatus = Compare-Migration -Source $sourceInventory `
                                  -Destination $destInventoryBefore `
                                  -SourceContainer $SourceContainer `
                                  -DestContainer $DestContainer `
                                  -Label 'PRE-MIGRATION STATUS -- snapshot of source vs destination before any copy operations'

if ($preStatus.PendingCount -eq 0) {
    Write-Log 'Nothing to migrate -- destination already in sync.'
    return
}

# Migration
# Declared outside ShouldProcess so the later `if ($azCopyError)` check is safe
# under Set-StrictMode even if the early-return paths are ever refactored.
$azCopyError = $null
if ($PSCmdlet.ShouldProcess(
    "$($preStatus.PendingCount) blob(s) pending",
    "Copy to $DestStorageAccount/$DestContainer")) {

    Write-Log ('MIGRATING -- copying {0} pending blob(s) from {1} to {2}' -f $preStatus.PendingCount, $SourceContainer, $DestContainer)
    [long]$migrationTotalSize = 0
    $preStatus.PendingNames | Sort-Object | ForEach-Object {
        $size = $sourceInventory[$_].Length
        $migrationTotalSize += $size
        Write-Host ('  {0,-50} {1,-10} {2,10}' -f (Format-Cell $_ 50), $sourceInventory[$_].BlobType, (Format-FileSize $size))
    }
    Write-Host ''
    Write-Host ('  Total: {0} file(s), {1}' -f $preStatus.PendingCount, (Format-FileSize $migrationTotalSize))

    try {
        Invoke-AzCopySync -SourceAccount $SourceStorageAccount `
                          -SourceContainer $SourceContainer `
                          -DestAccount $DestStorageAccount `
                          -DestContainer $DestContainer
    }
    catch {
        $azCopyError = $_
        Write-Warning $azCopyError.Exception.Message
    }
}
else {
    Write-Warning 'Dry run (-WhatIf) -- no data copied.'
    return
}

# Re-inventory destination and validate up front so $failed (used by the
# SUCCESSFULLY TRANSFERRED / FAILED FILES sections) shares the same rules as
# the VALIDATION block below. Previously they diverged: $failed only checked
# size, validation also checked MD5, so an MD5-mismatch blob would appear as
# "Migrated" in the summary but cause validation to FAIL -- contradictory.
$destInventoryAfter = Get-BlobInventory -Context $destCtx -Container $DestContainer
$validation = Test-MigrationCompleteness -Source $sourceInventory -Destination $destInventoryAfter

# Scope $failed to blobs we actually tried to transfer (pending set). A blob
# that was matched at pre-time but mismatched at post-time means source mutated
# during the run -- reported by validation but not counted as a transfer failure.
$pendingSet = [System.Collections.Generic.HashSet[string]]::new([System.StringComparer]::Ordinal)
foreach ($n in $preStatus.PendingNames) { [void]$pendingSet.Add($n) }

$failed = New-Object System.Collections.Generic.List[string]
foreach ($n in $validation.MissingNames)      { if ($pendingSet.Contains($n)) { $failed.Add($n) } }
foreach ($n in $validation.SizeMismatchNames) { if ($pendingSet.Contains($n)) { $failed.Add($n) } }
foreach ($n in $validation.Md5MismatchNames)  { if ($pendingSet.Contains($n)) { $failed.Add($n) } }

$failedSet = [System.Collections.Generic.HashSet[string]]::new([System.StringComparer]::Ordinal)
foreach ($n in $failed) { [void]$failedSet.Add($n) }
$succeeded = @($preStatus.PendingNames | Where-Object { -not $failedSet.Contains($_) })

if ($succeeded.Count -gt 0) {
    Write-Log 'SUCCESSFULLY TRANSFERRED'
    if ($failed.Count -eq 0) {
        Write-Host ('  All {0} pending blob(s) transferred successfully.' -f $succeeded.Count)
    } else {
        Write-Host ('  {0} of {1} pending blob(s) transferred successfully.' -f $succeeded.Count, $preStatus.PendingCount)
        Show-BlobList -Heading ("Transferred to $DestContainer") -Names $succeeded -SizeSource $sourceInventory
    }
}

if ($failed.Count -gt 0) {
    Write-Log 'FAILED FILES'
    Write-Host ("  $($script:Ansi.Red){0} blob(s) were pending but did not reach the destination intact.$($script:Ansi.Reset)" -f $failed.Count)
    Write-Host ('  See AzCopy logs in {0}' -f $logDir)
    Write-Host ''
    $failed | Sort-Object | ForEach-Object {
        $size = $sourceInventory[$_].Length
        Write-Host ("  $($script:Ansi.Red){0,-50}$($script:Ansi.Reset) {1,-10} {2,10}" -f (Format-Cell $_ 50), $sourceInventory[$_].BlobType, (Format-FileSize $size))
    }
}

$null = Compare-Migration -Source $sourceInventory `
                             -Destination $destInventoryAfter `
                             -SourceContainer $SourceContainer `
                             -DestContainer $DestContainer `
                             -Label 'POST-MIGRATION STATUS -- destination state after copy operations complete'

# Final validation -- always runs so the full comparison table (size + MD5)
# is logged, even when there are issues. Test-MigrationCompleteness already
# ran above; we just render its result here. The script throws only after the
# table has rendered so failures are visible before the pipeline step exits.
Write-Log ("VALIDATION -- comparing '{0}' to '{1}' by name, size, and MD5 checksum" -f $SourceContainer, $DestContainer)
Write-Host ("Comparing {0} blob(s) in '{1}' against {2} blob(s) in '{3}'..." -f $validation.SourceCount, $SourceContainer, $validation.DestCount, $DestContainer)
if ($validation.DestOnlyCount -gt 0) {
    Write-Host ('Note: destination contains {0} blob(s) not present in source (preserved by --delete-destination=false).' -f $validation.DestOnlyCount)
    Show-BlobList -Heading ("Only in $DestContainer") -Names $validation.DestOnlyNames -SizeSource $destInventoryAfter
}
Write-Host ''
if ($validation.Passed) {
    if ($validation.NoMd5Count -gt 0) {
        Write-Host ("PASS (sizes only) -- {0} of {1} blob(s) in '{2}' were not checksum-verified (no Content-MD5 set). Names and sizes match for all {1} blob(s)." -f $validation.NoMd5Count, $validation.SourceCount, $DestContainer)
        if ($validation.Md5VerifiedCount -gt 0) {
            Write-Host ('MD5 verified: {0} of {1} blob(s).' -f $validation.Md5VerifiedCount, $validation.SourceCount)
        }
    } else {
        Write-Host ("PASS -- '{0}' has all {1} blob(s) with matching names, sizes, and MD5 checksums." -f $DestContainer, $validation.SourceCount)
    }
    Write-Host ("'{0}' data left untouched." -f $SourceContainer)
    Write-Host ''
    Show-BlobComparison -Source $sourceInventory -Destination $destInventoryAfter -SourceContainer $SourceContainer -DestContainer $DestContainer -LogDirectory $logDir
}
else {
    Write-Host ''
    Show-BlobComparison -Source $sourceInventory -Destination $destInventoryAfter -SourceContainer $SourceContainer -DestContainer $DestContainer -LogDirectory $logDir
    Write-Host ''
    Write-Host "$($script:Ansi.Red)FAIL$($script:Ansi.Reset) -- validation issues found:"
    foreach ($issue in $validation.Issues) {
        Write-Host ("  $($script:Ansi.Red){0}$($script:Ansi.Reset)" -f $issue)
    }
}

Write-Log 'SUMMARY -- migration result'
Write-Host ('Source blobs:        {0}' -f $sourceInventory.Count)
Write-Host ('Already in sync:     {0}' -f $preStatus.MatchedNames.Count)
Write-Host ('Pending:             {0}' -f $preStatus.PendingCount)
Write-Host ('  Migrated:          {0}' -f $succeeded.Count)
Write-Host ('  Failed:            {0}' -f $failed.Count)
if ($validation.DestOnlyCount -gt 0) {
    Write-Host ('Destination extras:  {0} (preserved, not in source)' -f $validation.DestOnlyCount)
}
$validationStatus = if ($validation.Passed) {
    'PASS'
} else {
    "$($script:Ansi.Red)FAIL$($script:Ansi.Reset)"
}
Write-Host ("Validation:          {0}" -f $validationStatus)

# Failure throws happen here, after all the data sections have been logged, so
# the user can see exactly which blob failed before the pipeline step exits.
# The MIGRATION TIME block is emitted from the finally below regardless of
# whether these throws fire or an earlier error/early-return ended the run.
if ($azCopyError) {
    throw $azCopyError.Exception
}
if (-not $validation.Passed) {
    throw "Validation failed:`n$($validation.Issues -join "`n")"
}

}
finally {
    $migrationEnd = [datetime]::UtcNow
    $elapsed = $migrationEnd - $migrationStart
    $elapsedText = if ($elapsed.TotalDays -ge 1) {
        '{0} day(s) {1:hh\:mm\:ss}' -f [int]$elapsed.TotalDays, $elapsed
    } else {
        '{0:hh\:mm\:ss}' -f $elapsed
    }

    Write-Log 'MIGRATION TIME -- total elapsed duration for this run'
    Write-Host ('Migration started:  {0:yyyy-MM-dd HH:mm:ss} UTC' -f $migrationStart)
    Write-Host ('Migration ended:    {0:yyyy-MM-dd HH:mm:ss} UTC' -f $migrationEnd)
    Write-Host ('Total time elapsed: {0}' -f $elapsedText)
}
