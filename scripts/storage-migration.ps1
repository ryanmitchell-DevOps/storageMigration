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

    [string]$LogDirectory
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'

# ANSI escapes for the one color we still use (red, for failures). Section
# headings rely on Azure DevOps's ##[section] marker instead of ANSI so they
# render correctly in artifact viewers / plain-text downloads.
$script:Ansi = @{
    Reset  = "`e[0m"
    Red    = "`e[91m"
    Yellow = "`e[93m"
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

function Get-RemoteBlobMD5 {
    param(
        [Parameter(Mandatory)]$Context,
        [Parameter(Mandatory)][string]$Container,
        [Parameter(Mandatory)][string]$BlobName
    )
    # Backfill helper -- downloads a blob to a temp file and computes MD5 so we
    # have a real checksum when Content-MD5 metadata is missing. Returns the
    # hash as base64 to match the format Azure stores in Content-MD5.
    $tempPath = Join-Path ([IO.Path]::GetTempPath()) ("blobcheck_" + [guid]::NewGuid().ToString() + ".bin")
    try {
        Get-AzStorageBlobContent -Container $Container -Blob $BlobName -Destination $tempPath -Context $Context -Force -ErrorAction Stop | Out-Null
        $hashHex = (Get-FileHash -Path $tempPath -Algorithm MD5).Hash
        $bytes = [byte[]]::new($hashHex.Length / 2)
        for ($i = 0; $i -lt $bytes.Length; $i++) {
            $bytes[$i] = [Convert]::ToByte($hashHex.Substring($i * 2, 2), 16)
        }
        return [Convert]::ToBase64String($bytes)
    }
    finally {
        if (Test-Path $tempPath) { Remove-Item $tempPath -Force -ErrorAction SilentlyContinue }
    }
}

function Update-InventoryMissingMd5 {
    param(
        [Parameter(Mandatory)][System.Collections.IDictionary]$Inventory,
        [Parameter(Mandatory)]$Context,
        [Parameter(Mandatory)][string]$Container,
        [Parameter(Mandatory)][string[]]$Names
    )
    # Mutates inventory entries in place: for each named blob whose MD5 is
    # currently null, hash the content and store the result. Returns the
    # number of MD5s resolved so the caller can report progress.
    $resolved = 0
    foreach ($name in $Names) {
        if (-not $Inventory.ContainsKey($name)) { continue }
        $entry = $Inventory[$name]
        if ($entry.MD5) { continue }
        $entry.MD5 = Get-RemoteBlobMD5 -Context $Context -Container $Container -BlobName $name
        $resolved++
    }
    return $resolved
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
    Show-BlobList -Heading 'Out of sync (size differs)'  -Names $c.SizeMismatchNames -SizeSource $Source -Color $script:Ansi.Yellow
    Show-BlobList -Heading 'Out of sync (MD5 differs)'   -Names $c.Md5MismatchNames  -SizeSource $Source -Color $script:Ansi.Yellow
    Show-BlobList -Heading ("Already in $DestContainer") -Names $alreadyInSync       -SizeSource $Source

    return [pscustomobject]@{
        SourceCount       = $c.SourceCount
        DestCount         = $c.DestCount
        PendingCount      = $pending.Count
        PendingPercent    = $pendingPct
        PendingNames      = $pending
        MissingNames      = $c.MissingNames
        OutOfSyncNames    = $outOfSync
        SizeMismatchNames  = $c.SizeMismatchNames
        Md5MismatchNames   = $c.Md5MismatchNames
        # Includes both MD5-verified matches AND blobs where neither side has a
        # Content-MD5 set (size matched but checksum couldn't be confirmed).
        # Callers that need verified-only matches should use Get-BlobClassification
        # directly and read .MatchedNames.
        AlreadyInSyncNames = $alreadyInSync
    }
}

function Invoke-AzCopyByList {
    param(
        [string]$SourceAccount,
        [string]$SourceContainer,
        [string]$DestAccount,
        [string]$DestContainer,
        [string[]]$BlobNames
    )
    # Uses `azcopy copy --list-of-files` instead of `azcopy sync`. Sync has no
    # clean way to exclude specific blob names from being overwritten, so we
    # do our own comparison up front and explicitly list which blobs to copy.
    # This lets us preserve destination versions of any blob that has diverged
    # from source (size or MD5 mismatch) -- those are reported separately as
    # failures rather than silently overwritten.
    #
    # --put-md5 stamps Content-MD5 on the destination so post-migration
    # validation has a checksum to compare against.
    if ($BlobNames.Count -eq 0) { return }

    $sourceUrl = "https://$SourceAccount.blob.core.windows.net/$SourceContainer"
    $destUrl = "https://$DestAccount.blob.core.windows.net/$DestContainer"

    $listFile = Join-Path ([IO.Path]::GetTempPath()) ("azcopy-list-" + [guid]::NewGuid().ToString() + ".txt")
    try {
        # WriteAllLines emits UTF-8 without BOM, newline-separated -- the format
        # azcopy expects. Avoid Set-Content here: in Windows PowerShell it
        # defaults to UTF-16 with BOM, which azcopy treats as a single garbled
        # filename.
        [System.IO.File]::WriteAllLines($listFile, $BlobNames)

        $azArgs = @(
            'copy', $sourceUrl, $destUrl,
            '--list-of-files', $listFile,
            '--put-md5'
        )

        # Capture combined stdout+stderr so we can dump it on failure -- without
        # this, AzCopy's actual error reason is invisible in the pipeline log and
        # we only see our own generic "exit code N" message. Full raw output is
        # also preserved in $env:AZCOPY_LOG_LOCATION.
        $output = & azcopy @azArgs 2>&1
        if ($LASTEXITCODE -ne 0) {
            $output | ForEach-Object { Write-Host $_ }
            throw "AzCopy copy failed with exit code $LASTEXITCODE. See logs in $env:AZCOPY_LOG_LOCATION"
        }
    }
    finally {
        if (Test-Path $listFile) { Remove-Item $listFile -Force -ErrorAction SilentlyContinue }
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

# Verify the azcopy binary is reachable before doing any work. Without this,
# a missing executable surfaces deep inside Invoke-AzCopyByList as a generic
# PowerShell "command not found" with no clear remediation.
if (-not (Get-Command azcopy -ErrorAction SilentlyContinue)) {
    throw "azcopy executable not found in PATH. Install it on the agent (e.g. via the AzureCLI@2 task or a dedicated AzCopy install step) before running this script."
}

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

# Backfill MD5 by hashing content for blobs where size matches on both sides
# but at least one side lacks Content-MD5 metadata. Without this, those blobs
# fall into the "noMd5" classification and are silently treated as in-sync --
# meaning a destination modification that happens to land at the same byte
# count as source would go undetected. Only same-size candidates get hashed:
# size differences are already conclusive evidence of divergence, no need to
# pay the download cost to confirm.
$md5Candidates = New-Object System.Collections.Generic.List[string]
foreach ($name in $sourceInventory.Keys) {
    if (-not $destInventoryBefore.ContainsKey($name)) { continue }
    $s = $sourceInventory[$name]
    $d = $destInventoryBefore[$name]
    if ($s.Length -ne $d.Length) { continue }
    if (-not $s.MD5 -or -not $d.MD5) { $md5Candidates.Add($name) }
}
if ($md5Candidates.Count -gt 0) {
    Write-Host ('       {0} same-size blob(s) lack Content-MD5 metadata. Hashing content to verify...' -f $md5Candidates.Count)
    $null = Set-AzContext -SubscriptionId $SourceSubscriptionId -WarningAction SilentlyContinue
    $srcResolved = Update-InventoryMissingMd5 -Inventory $sourceInventory -Context $sourceCtx -Container $SourceContainer -Names $md5Candidates
    $null = Set-AzContext -SubscriptionId $DestSubscriptionId -WarningAction SilentlyContinue
    $dstResolved = Update-InventoryMissingMd5 -Inventory $destInventoryBefore -Context $destCtx -Container $DestContainer -Names $md5Candidates
    Write-Host ('       MD5 backfilled: {0} source blob(s), {1} destination blob(s).' -f $srcResolved, $dstResolved)
}

$preStatus = Compare-Migration -Source $sourceInventory `
                                  -Destination $destInventoryBefore `
                                  -SourceContainer $SourceContainer `
                                  -DestContainer $DestContainer `
                                  -Label 'PRE-MIGRATION STATUS -- snapshot of source vs destination before any copy operations'

if ($preStatus.PendingCount -eq 0) {
    Write-Log 'Nothing to migrate -- destination already in sync.'
    return
}

# Diverged blobs (size or MD5 mismatch) are NOT copied -- the destination's
# version stays untouched. They mean something modified the destination outside
# this migration (manual edit, interrupted prior run, a different process
# writing to the same container). Report them up front so the operator knows
# what's being preserved; the script will still fail at the end with these
# listed as skipped, but only after copying the safe (missing-in-destination)
# blobs.
$divergedNames = @($preStatus.SizeMismatchNames) + @($preStatus.Md5MismatchNames)
if ($divergedNames.Count -gt 0) {
    Write-Log ('DESTINATION DIVERGENCE DETECTED -- these blobs will NOT be copied')
    Write-Host ("$($script:Ansi.Red){0} blob(s) in '{1}' differ from source by size or MD5.$($script:Ansi.Reset)" -f $divergedNames.Count, $DestContainer)
    Write-Host ('The destination versions will be preserved (NOT overwritten). Missing-in-destination')
    Write-Host ('blobs will still be copied; the script will fail at the end with these listed.')
    Write-Host ''
    if ($preStatus.SizeMismatchNames.Count -gt 0) {
        Write-Host ('  Size mismatch ({0}):' -f $preStatus.SizeMismatchNames.Count)
        $preStatus.SizeMismatchNames | Sort-Object | ForEach-Object {
            $srcSize = $sourceInventory[$_].Length
            $dstSize = $destInventoryBefore[$_].Length
            Write-Host ("    $($script:Ansi.Yellow){0,-50}$($script:Ansi.Reset)  source={1}  dest={2}" -f (Format-Cell $_ 50), (Format-FileSize $srcSize), (Format-FileSize $dstSize))
        }
    }
    if ($preStatus.Md5MismatchNames.Count -gt 0) {
        Write-Host ('  MD5 mismatch ({0}):' -f $preStatus.Md5MismatchNames.Count)
        $preStatus.Md5MismatchNames | Sort-Object | ForEach-Object {
            Write-Host ("    $($script:Ansi.Yellow){0,-50}$($script:Ansi.Reset)  source-md5={1}  dest-md5={2}" -f (Format-Cell $_ 50), $sourceInventory[$_].MD5, $destInventoryBefore[$_].MD5)
        }
    }
}

# Safe-to-copy set: pending blobs that aren't diverged. These are the ones
# missing from destination -- copying them is purely additive and can't destroy
# any existing destination state.
$divergedSet = [System.Collections.Generic.HashSet[string]]::new([System.StringComparer]::Ordinal)
foreach ($n in $divergedNames) { [void]$divergedSet.Add($n) }
$toCopy = @($preStatus.PendingNames | Where-Object { -not $divergedSet.Contains($_) })

# Migration
# Declared outside ShouldProcess so the later `if ($azCopyError)` check is safe
# under Set-StrictMode even if the early-return paths are ever refactored.
$azCopyError = $null
if ($toCopy.Count -eq 0) {
    Write-Log ('NOTHING TO COPY -- all {0} pending blob(s) are diverged in destination and will be preserved' -f $divergedNames.Count)
}
elseif ($PSCmdlet.ShouldProcess(
    "$($toCopy.Count) blob(s) to copy ($($divergedNames.Count) diverged, preserved)",
    "Copy to $DestStorageAccount/$DestContainer")) {

    $migrateLabel = if ($divergedNames.Count -gt 0) {
        ('MIGRATING -- copying {0} missing blob(s) from {1} to {2} ({3} diverged blob(s) preserved in destination)' -f $toCopy.Count, $SourceContainer, $DestContainer, $divergedNames.Count)
    } else {
        ('MIGRATING -- copying {0} pending blob(s) from {1} to {2}' -f $toCopy.Count, $SourceContainer, $DestContainer)
    }
    Write-Log $migrateLabel
    [long]$migrationTotalSize = 0
    $toCopy | Sort-Object | ForEach-Object {
        $size = $sourceInventory[$_].Length
        $migrationTotalSize += $size
        Write-Host ('  {0,-50} {1,-10} {2,10}' -f (Format-Cell $_ 50), $sourceInventory[$_].BlobType, (Format-FileSize $size))
    }
    Write-Host ''
    Write-Host ('  Total: {0} file(s), {1}' -f $toCopy.Count, (Format-FileSize $migrationTotalSize))

    try {
        Invoke-AzCopyByList -SourceAccount $SourceStorageAccount `
                            -SourceContainer $SourceContainer `
                            -DestAccount $DestStorageAccount `
                            -DestContainer $DestContainer `
                            -BlobNames $toCopy
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

# Scope $failed to blobs we actually tried to copy ($toCopy). Diverged blobs
# weren't part of the copy operation -- they're tracked separately as $skipped.
# A blob that was matched at pre-time but mismatched at post-time means source
# mutated during the run -- reported by validation but not counted as a
# transfer failure.
$toCopySet = [System.Collections.Generic.HashSet[string]]::new([System.StringComparer]::Ordinal)
foreach ($n in $toCopy) { [void]$toCopySet.Add($n) }

$failed = New-Object System.Collections.Generic.List[string]
foreach ($n in $validation.MissingNames)      { if ($toCopySet.Contains($n)) { $failed.Add($n) } }
foreach ($n in $validation.SizeMismatchNames) { if ($toCopySet.Contains($n)) { $failed.Add($n) } }
foreach ($n in $validation.Md5MismatchNames)  { if ($toCopySet.Contains($n)) { $failed.Add($n) } }

$failedSet = [System.Collections.Generic.HashSet[string]]::new([System.StringComparer]::Ordinal)
foreach ($n in $failed) { [void]$failedSet.Add($n) }
$succeeded = @($toCopy | Where-Object { -not $failedSet.Contains($_) })
$skipped = $divergedNames

if ($succeeded.Count -gt 0) {
    Write-Log 'SUCCESSFULLY TRANSFERRED'
    if ($failed.Count -eq 0) {
        Write-Host ('  All {0} copied blob(s) transferred successfully.' -f $succeeded.Count)
    } else {
        Write-Host ('  {0} of {1} blob(s) transferred successfully.' -f $succeeded.Count, $toCopy.Count)
        Show-BlobList -Heading ("Transferred to $DestContainer") -Names $succeeded -SizeSource $sourceInventory
    }
}

if ($failed.Count -gt 0) {
    Write-Log 'FAILED FILES'
    Write-Host ("  $($script:Ansi.Red){0} blob(s) were copied but did not reach the destination intact.$($script:Ansi.Reset)" -f $failed.Count)
    Write-Host ('  See AzCopy logs in {0}' -f $logDir)
    Write-Host ''
    $failed | Sort-Object | ForEach-Object {
        $size = $sourceInventory[$_].Length
        Write-Host ("  $($script:Ansi.Red){0,-50}$($script:Ansi.Reset) {1,-10} {2,10}" -f (Format-Cell $_ 50), $sourceInventory[$_].BlobType, (Format-FileSize $size))
    }
}

if ($skipped.Count -gt 0) {
    Write-Log 'SKIPPED FILES (destination divergence)'
    Write-Host ("  $($script:Ansi.Red){0} blob(s) were preserved in destination because they diverge from source.$($script:Ansi.Reset)" -f $skipped.Count)
    Write-Host ('  These were NOT overwritten. Investigate and reconcile manually before re-running.')
    Write-Host ''
    $skipped | Sort-Object | ForEach-Object {
        $srcSize = $sourceInventory[$_].Length
        $dstSize = $destInventoryBefore[$_].Length
        Write-Host ("  $($script:Ansi.Yellow){0,-50}$($script:Ansi.Reset) {1,-10}  source={2,10}  dest={3,10}" -f (Format-Cell $_ 50), $sourceInventory[$_].BlobType, (Format-FileSize $srcSize), (Format-FileSize $dstSize))
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
Write-Host ('Already in sync:     {0}' -f $preStatus.AlreadyInSyncNames.Count)
Write-Host ('Pending:             {0}' -f $preStatus.PendingCount)
Write-Host ('  Migrated:          {0}' -f $succeeded.Count)
Write-Host ('  Failed:            {0}' -f $failed.Count)
Write-Host ('  Skipped (diverged):{0}' -f $skipped.Count)
if ($validation.DestOnlyCount -gt 0) {
    Write-Host ('Destination extras:  {0} (preserved, not in source)' -f $validation.DestOnlyCount)
}
$overallPass = $validation.Passed -and $skipped.Count -eq 0
$validationStatus = if ($overallPass) {
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
if ($skipped.Count -gt 0) {
    throw "Destination divergence: $($skipped.Count) blob(s) in '$DestContainer' differ from source and were preserved (not overwritten). Reconcile manually before re-running. See SKIPPED FILES above for the list."
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
