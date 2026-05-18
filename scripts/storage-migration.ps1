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

function Write-LogDirectory {
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
    $account = Get-AzStorageAccount -ErrorAction SilentlyContinue |
        Where-Object { $_.StorageAccountName -eq $AccountName } |
        Select-Object -First 1
    if (-not $account) {
        throw "Storage account '$AccountName' does not exist in subscription '$SubscriptionId', or the current identity does not have access to list it."
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

function Get-BlobMD5 {
    param($Blob)
    # Newer Az.Storage: BlobProperties.ContentHash is a byte[]
    if ($Blob.PSObject.Properties['BlobProperties'] -and $Blob.BlobProperties -and $Blob.BlobProperties.ContentHash) {
        return [Convert]::ToBase64String($Blob.BlobProperties.ContentHash)
    }
    # Older Az.Storage: ICloudBlob.Properties.ContentMD5 is already base64
    if ($Blob.PSObject.Properties['ICloudBlob'] -and $Blob.ICloudBlob -and $Blob.ICloudBlob.Properties.ContentMD5) {
        return $Blob.ICloudBlob.Properties.ContentMD5
    }
    return $null
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
        $map[$_.Name] = [pscustomobject]@{
            Name     = $_.Name
            Length   = $_.Length
            MD5      = (Get-BlobMD5 -Blob $_)
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

function Compare-Migration {
    param(
        [System.Collections.IDictionary]$Source,
        [System.Collections.IDictionary]$Destination,
        [string]$Label = 'Status',
        [string]$SourceContainer = 'source',
        [string]$DestContainer = 'destination'
    )
    $sourceCount = $Source.Count
    $destCount = $Destination.Count

    $missing = New-Object System.Collections.Generic.List[string]
    $sizeMismatch = New-Object System.Collections.Generic.List[string]
    $md5Mismatch = New-Object System.Collections.Generic.List[string]
    $matched = New-Object System.Collections.Generic.List[string]
    foreach ($name in $Source.Keys) {
        if (-not $Destination.ContainsKey($name)) {
            $missing.Add($name)
            continue
        }
        $srcBlob = $Source[$name]
        $dstBlob = $Destination[$name]
        if ($srcBlob.Length -ne $dstBlob.Length) {
            $sizeMismatch.Add($name)
        }
        elseif ($srcBlob.MD5 -and $dstBlob.MD5 -and $srcBlob.MD5 -ne $dstBlob.MD5) {
            $md5Mismatch.Add($name)
        }
        else {
            $matched.Add($name)
        }
    }

    $outOfSync = @($sizeMismatch) + @($md5Mismatch)
    $pending = @($missing) + $outOfSync
    $pendingCount = $pending.Count
    $matchedCount = $matched.Count
    $pendingPct = if ($sourceCount -gt 0) {
        [math]::Round(($pendingCount / $sourceCount) * 100, 2)
    } else { 0 }
    $migratedPct = [math]::Round(100 - $pendingPct, 2)

    Write-Log $Label
    Write-Host ("Blobs in '{0}': {1}" -f $SourceContainer, $sourceCount)
    Write-Host ("Blobs in '{0}': {1}" -f $DestContainer, $destCount)
    Write-Host ('Already migrated:  {0} ({1}%)' -f $matchedCount, $migratedPct)
    Write-Host ('Pending:           {0} ({1}%)  [missing: {2}, size-mismatch: {3}, md5-mismatch: {4}]' -f `
        $pendingCount, $pendingPct, $missing.Count, $sizeMismatch.Count, $md5Mismatch.Count)

    Show-BlobList -Heading ("Not in $DestContainer")     -Names $missing      -SizeSource $Source -Color $script:Ansi.Red
    Show-BlobList -Heading 'Out of sync (size differs)'  -Names $sizeMismatch -SizeSource $Source
    Show-BlobList -Heading 'Out of sync (MD5 differs)'   -Names $md5Mismatch  -SizeSource $Source
    Show-BlobList -Heading ("Already in $DestContainer") -Names $matched      -SizeSource $Source

    return [pscustomobject]@{
        SourceCount       = $sourceCount
        DestCount         = $destCount
        PendingCount      = $pendingCount
        PendingPercent    = $pendingPct
        PendingNames      = $pending
        MissingNames      = @($missing)
        OutOfSyncNames    = $outOfSync
        SizeMismatchNames = @($sizeMismatch)
        Md5MismatchNames  = @($md5Mismatch)
        MatchedNames      = @($matched)
    }
}

function Set-AzCopyEnvironment {
    param([string]$LogDirectory)
    # PSCRED inherits the Az PowerShell session established by AzurePowerShell@5.
    $env:AZCOPY_AUTO_LOGIN_TYPE = 'PSCRED'
    $env:AZCOPY_LOG_LOCATION = $LogDirectory
    $env:AZCOPY_JOB_PLAN_LOCATION = $LogDirectory
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

    $missing = New-Object System.Collections.Generic.List[string]
    $sizeMismatch = New-Object System.Collections.Generic.List[string]
    $md5Mismatch = New-Object System.Collections.Generic.List[string]
    $noMd5 = New-Object System.Collections.Generic.List[string]

    # Single pass: classify each source blob into exactly one bucket. Anything
    # not bucketed here is implicitly verified (size matches AND MD5 matches).
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
        }
    }

    $issues = New-Object System.Collections.Generic.List[string]
    if ($missing.Count -gt 0) {
        $issues.Add("Missing from destination: $($missing.Count) blob(s) -- present in source but never reached destination")
        $missing | Select-Object -First 10 | ForEach-Object { $issues.Add(" - $_") }
    }
    if ($sizeMismatch.Count -gt 0) {
        $issues.Add("Size mismatch: $($sizeMismatch.Count) blob(s) -- byte count differs between source and destination")
        $sizeMismatch | Select-Object -First 10 | ForEach-Object { $issues.Add(" - $_") }
    }
    if ($md5Mismatch.Count -gt 0) {
        $issues.Add("MD5 mismatch: $($md5Mismatch.Count) blob(s) -- content checksum differs between source and destination")
        $md5Mismatch | Select-Object -First 10 | ForEach-Object { $issues.Add(" - $_") }
    }

    $md5Verified = $Source.Count - $missing.Count - $sizeMismatch.Count - $noMd5.Count - $md5Mismatch.Count

    # Destination-only blobs: present in destination but not in source. Expected
    # with --delete-destination=false, so informational rather than a failure.
    $destOnly = @($Destination.Keys | Where-Object { -not $Source.ContainsKey($_) })

    return [pscustomobject]@{
        Passed           = ($issues.Count -eq 0)
        Issues           = $issues
        SourceCount      = $Source.Count
        DestCount        = $Destination.Count
        NoMd5Count       = $noMd5.Count
        Md5VerifiedCount = $md5Verified
        Md5MismatchCount = $md5Mismatch.Count
        DestOnlyCount    = $destOnly.Count
        DestOnlyNames    = $destOnly
    }
}

function Show-BlobComparison {
    param(
        [System.Collections.IDictionary]$Source,
        [System.Collections.IDictionary]$Destination,
        [string]$SourceContainer = 'source',
        [string]$DestContainer = 'destination'
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
        Write-Host ('    ... and {0} more (see AzCopy logs in {1})' -f ($names.Count - $shown), $env:AZCOPY_LOG_LOCATION)
    }
    Write-Host (($base -f ('-' * 50), ('-' * 10), ('-' * 20), ('-' * 20)) + ($tail -f ('-' * 8), ('-' * 8)))
    Write-Host ($base -f 'TOTAL', '', (Format-FileSize $totalSrc), (Format-FileSize $totalDst))
}

# Script
$migrationStart = [datetime]::UtcNow
$logDir = Write-LogDirectory -Log $LogDirectory
Set-AzCopyEnvironment -LogDirectory $logDir

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
    Write-Host ''
    Write-Host '##[section]Nothing to migrate -- destination already in sync.'
    return
}

# Migration
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

    $azCopyError = $null
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

# Re-inventory destination to derive real transfer outcome
$destInventoryAfter = Get-BlobInventory -Context $destCtx -Container $DestContainer

$failed = New-Object System.Collections.Generic.List[string]
foreach ($name in $preStatus.PendingNames) {
    if (-not $destInventoryAfter.ContainsKey($name) -or
        $destInventoryAfter[$name].Length -ne $sourceInventory[$name].Length) {
        $failed.Add($name)
    }
}

$succeeded = @($preStatus.PendingNames | Where-Object { -not $failed.Contains($_) })

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
# is logged, even when there are issues. Test-MigrationCompleteness is the
# single source of truth for missing / size / MD5 problems; the script throws
# only after the table has rendered.
Write-Log ("VALIDATION -- comparing '{0}' to '{1}' by name, size, and MD5 checksum" -f $SourceContainer, $DestContainer)
$validation = Test-MigrationCompleteness -Source $sourceInventory `
                                         -Destination $destInventoryAfter
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
    Show-BlobComparison -Source $sourceInventory -Destination $destInventoryAfter -SourceContainer $SourceContainer -DestContainer $DestContainer
}
else {
    Write-Host ''
    Show-BlobComparison -Source $sourceInventory -Destination $destInventoryAfter -SourceContainer $SourceContainer -DestContainer $DestContainer
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
