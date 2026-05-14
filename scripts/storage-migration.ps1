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

$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'

# ANSI color codes — Azure DevOps logs render these; pwsh 7+ supports `e
$script:Ansi = @{
    Reset  = "`e[0m"
    Red    = "`e[91m"
    Green  = "`e[92m"
    Yellow = "`e[93m"
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

function Set-SubscriptionContext {
    param([string]$SubscriptionId)
    $null = Set-AzContext -SubscriptionId $SubscriptionId -WarningAction SilentlyContinue
}

function Get-StorageContext {
    param([string]$AccountName)
    return New-AzStorageContext -StorageAccountName $AccountName -UseConnectedAccount
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
    Write-Host ("$($script:Ansi.Green)Container '{0}' found in '{1}'.$($script:Ansi.Reset)" -f $Container, $AccountName)
}

function Get-BlobMD5 {
    param($Blob)
    try {
        # Newer Az.Storage: BlobProperties.ContentHash is a byte[]
        if ($Blob.PSObject.Properties['BlobProperties'] -and $Blob.BlobProperties -and $Blob.BlobProperties.ContentHash) {
            return [Convert]::ToBase64String($Blob.BlobProperties.ContentHash)
        }
        # Older Az.Storage: ICloudBlob.Properties.ContentMD5 is already base64
        if ($Blob.PSObject.Properties['ICloudBlob'] -and $Blob.ICloudBlob -and $Blob.ICloudBlob.Properties.ContentMD5) {
            return $Blob.ICloudBlob.Properties.ContentMD5
        }
    } catch { }
    return $null
}

function Get-BlobInventory {
    param(
        [Parameter(Mandatory)]$Context,
        [Parameter(Mandatory)][string]$Container
    )
    $map = @{}
    $count = 0
    Get-AzStorageBlob -Container $Container -Context $Context -ErrorAction Stop |
    ForEach-Object {
        $map[$_.Name] = [pscustomobject]@{
            Name   = $_.Name
            Length = $_.Length
            MD5    = (Get-BlobMD5 -Blob $_)
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
        [hashtable]$SizeSource,
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
        Write-Host ('    {0,-60} {1,10}' -f $_, (Format-FileSize $SizeSource[$_].Length))
    }
    if ($Names.Count -gt $shown) { Write-Host "    ... and $($Names.Count - $shown) more" }
}

function Show-MigrationStatus {
    param(
        [hashtable]$Source,
        [hashtable]$Destination,
        [string]$Label = 'Status'
    )
    $sourceCount = $Source.Count
    $destCount = $Destination.Count

    $missing = New-Object System.Collections.Generic.List[string]
    $outOfSync = New-Object System.Collections.Generic.List[string]
    $matched = New-Object System.Collections.Generic.List[string]
    foreach ($name in $Source.Keys) {
        if (-not $Destination.ContainsKey($name)) {
            $missing.Add($name)
        }
        elseif ($Source[$name].Length -ne $Destination[$name].Length) {
            $outOfSync.Add($name)
        }
        else {
            $matched.Add($name)
        }
    }

    $pending = @($missing) + @($outOfSync)
    $pendingCount = $pending.Count
    $matchedCount = $matched.Count
    $pendingPct = if ($sourceCount -gt 0) {
        [math]::Round(($pendingCount / $sourceCount) * 100, 2)
    } else { 0 }
    $migratedPct = [math]::Round(100 - $pendingPct, 2)

    Write-Log $Label
    Write-Host ('Source blobs:      {0}' -f $sourceCount)
    Write-Host ('Destination blobs: {0}' -f $destCount)
    Write-Host ('Already migrated:  {0} ({1}%)' -f $matchedCount, $migratedPct)
    Write-Host ('Pending:           {0} ({1}%)  [missing: {2}, out-of-sync: {3}]' -f `
        $pendingCount, $pendingPct, $missing.Count, $outOfSync.Count)

    Show-BlobList -Heading 'Missing from destination'   -Names $missing   -SizeSource $Source -Color $script:Ansi.Red
    Show-BlobList -Heading 'Out of sync (size differs)' -Names $outOfSync -SizeSource $Source -Color $script:Ansi.Yellow
    Show-BlobList -Heading 'Already in destination'     -Names $matched   -SizeSource $Source -Color $script:Ansi.Green

    return [pscustomobject]@{
        SourceCount    = $sourceCount
        DestCount      = $destCount
        PendingCount   = $pendingCount
        PendingPercent = $pendingPct
        PendingNames   = $pending
        MissingNames   = @($missing)
        OutOfSyncNames = @($outOfSync)
        MatchedNames   = @($matched)
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
        '--delete-destination=false'
    )

    Write-Host "##[command]azcopy $($azArgs -join ' ')"
    & azcopy @azArgs
    if ($LASTEXITCODE -ne 0) {
        throw "AzCopy sync failed with exit code $LASTEXITCODE. See logs in $env:AZCOPY_LOG_LOCATION"
    }
}

function Test-MigrationCompleteness {
    param([hashtable]$Source, [hashtable]$Destination)

    $issues = New-Object System.Collections.Generic.List[string]

    $missing = @($Source.Keys | Where-Object { -not $Destination.ContainsKey($_) })
    if ($missing.Count -gt 0) {
        $issues.Add("Missing from destination: $($missing.Count) blob(s)")
        $missing | Select-Object -First 10 | ForEach-Object { $issues.Add(" - $_") }
    }

    $sizeMismatch = foreach ($name in $Source.Keys) {
        if ($Destination.ContainsKey($name) -and
            $Source[$name].Length -ne $Destination[$name].Length) { $name }
    }
    $sizeMismatch = @($sizeMismatch)
    if ($sizeMismatch.Count -gt 0) {
        $issues.Add("Size mismatch: $($sizeMismatch.Count) blob(s)")
        $sizeMismatch | Select-Object -First 10 | ForEach-Object { $issues.Add(" - $_") }
    }

    $md5Mismatch = foreach ($name in $Source.Keys) {
        if ($Destination.ContainsKey($name) -and
            $Source[$name].MD5 -and $Destination[$name].MD5 -and
            $Source[$name].MD5 -ne $Destination[$name].MD5) { $name }
    }
    $md5Mismatch = @($md5Mismatch)
    if ($md5Mismatch.Count -gt 0) {
        $issues.Add("MD5 mismatch: $($md5Mismatch.Count) blob(s)")
        $md5Mismatch | Select-Object -First 10 | ForEach-Object { $issues.Add(" - $_") }
    }

    $noMd5 = @($Source.Keys | Where-Object {
        -not $Source[$_].MD5 -or
        ($Destination.ContainsKey($_) -and -not $Destination[$_].MD5)
    })

    $md5Verified = $Source.Count - $noMd5.Count - $md5Mismatch.Count

    return [pscustomobject]@{
        Passed         = ($issues.Count -eq 0)
        Issues         = $issues
        SourceCount    = $Source.Count
        DestCount      = $Destination.Count
        NoMd5Count     = $noMd5.Count
        Md5VerifiedCount = $md5Verified
        Md5MismatchCount = $md5Mismatch.Count
    }
}

function Show-BlobComparison {
    param([hashtable]$Source, [hashtable]$Destination)
    $base = '  {0,-60} {1,12} {2,12}'
    Write-Host (($base -f 'Blob Name', 'Source', 'Destination') + '  Size      MD5')
    Write-Host (($base -f ('-' * 60), ('-' * 12), ('-' * 12)) + '  --------  --------')
    [long]$totalSrc = 0
    [long]$totalDst = 0
    foreach ($name in ($Source.Keys | Sort-Object)) {
        $srcBlob = $Source[$name]
        $srcSize = $srcBlob.Length
        $totalSrc += $srcSize

        if ($Destination.ContainsKey($name)) {
            $dstBlob = $Destination[$name]
            $dstSize = $dstBlob.Length
            $totalDst += $dstSize

            $sizeCell = if ($srcSize -eq $dstSize) {
                "$($script:Ansi.Green){0,-8}$($script:Ansi.Reset)" -f 'OK'
            } else {
                "$($script:Ansi.Red){0,-8}$($script:Ansi.Reset)" -f 'MISMATCH'
            }

            $md5Cell = if (-not $srcBlob.MD5 -or -not $dstBlob.MD5) {
                "$($script:Ansi.Yellow){0,-8}$($script:Ansi.Reset)" -f 'N/A'
            } elseif ($srcBlob.MD5 -eq $dstBlob.MD5) {
                "$($script:Ansi.Green){0,-8}$($script:Ansi.Reset)" -f 'OK'
            } else {
                "$($script:Ansi.Red){0,-8}$($script:Ansi.Reset)" -f 'MISMATCH'
            }

            Write-Host (($base -f $name, (Format-FileSize $srcSize), (Format-FileSize $dstSize)) + "  $sizeCell  $md5Cell")
        }
        else {
            $sizeCell = "$($script:Ansi.Red){0,-8}$($script:Ansi.Reset)" -f 'MISSING'
            $md5Cell  = "$($script:Ansi.Red){0,-8}$($script:Ansi.Reset)" -f '-'
            Write-Host (($base -f $name, (Format-FileSize $srcSize), 'MISSING') + "  $sizeCell  $md5Cell")
        }
    }
    Write-Host (($base -f ('-' * 60), ('-' * 12), ('-' * 12)) + '  --------  --------')
    Write-Host ($base -f 'TOTAL', (Format-FileSize $totalSrc), (Format-FileSize $totalDst))
}

# Script
$logDir = Write-LogDirectory -Log $LogDirectory
Set-AzCopyEnvironment -LogDirectory $logDir

Write-Log 'MIGRATION CONFIGURATION'
Write-Host ('Start time:        {0:yyyy-MM-dd HH:mm:ss} UTC' -f [datetime]::UtcNow)
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

Write-Log 'PREPARATION CHECKS'

Write-Host '[1/3] Verifying source container exists...'
Set-SubscriptionContext -SubscriptionId $SourceSubscriptionId
$sourceCtx = Get-StorageContext -AccountName $SourceStorageAccount
Assert-ContainerExists -Context $sourceCtx -Container $SourceContainer -AccountName $SourceStorageAccount

Write-Host '[2/3] Verifying destination container exists...'
Set-SubscriptionContext -SubscriptionId $DestSubscriptionId
$destCtx = Get-StorageContext -AccountName $DestStorageAccount
Assert-ContainerExists -Context $destCtx -Container $DestContainer -AccountName $DestStorageAccount

Write-Host '[3/3] Inventorying source and destination containers...'
Set-SubscriptionContext -SubscriptionId $SourceSubscriptionId
$sourceInventory = Get-BlobInventory -Context $sourceCtx -Container $SourceContainer
Set-SubscriptionContext -SubscriptionId $DestSubscriptionId
$destInventoryBefore = Get-BlobInventory -Context $destCtx -Container $DestContainer
Write-Host ('       Source inventoried:      {0} blob(s)' -f $sourceInventory.Count)
Write-Host ('       Destination inventoried: {0} blob(s)' -f $destInventoryBefore.Count)

$preStatus = Show-MigrationStatus -Source $sourceInventory `
                                  -Destination $destInventoryBefore `
                                  -Label 'PRE-MIGRATION STATUS'

if ($preStatus.PendingCount -eq 0) {
    Write-Host ''
    Write-Host '##[section]Nothing to migrate -- destination already in sync.'
    return
}

# Migration
if ($PSCmdlet.ShouldProcess(
    "$($preStatus.PendingCount) blob(s) pending",
    "Copy to $DestStorageAccount/$DestContainer")) {

    Write-Log 'MIGRATING'
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

$transferred = New-Object System.Collections.Generic.List[string]
$failed = New-Object System.Collections.Generic.List[string]
foreach ($name in $preStatus.PendingNames) {
    if ($destInventoryAfter.ContainsKey($name) -and
        $destInventoryAfter[$name].Length -eq $sourceInventory[$name].Length) {
        $transferred.Add($name)
    }
    else {
        $failed.Add($name)
    }
}

if ($transferred.Count -gt 0) {
    Write-Log 'TRANSFERRED FILES'
    [long]$transferTotal = 0
    $transferred | Sort-Object | ForEach-Object {
        $size = $sourceInventory[$_].Length
        $transferTotal += $size
        Write-Host ('  {0,-60} {1,10}' -f $_, (Format-FileSize $size))
    }
    Write-Host ''
    Write-Host ('  Total: {0} file(s), {1}' -f $transferred.Count, (Format-FileSize $transferTotal))
}

if ($failed.Count -gt 0) {
    Write-Log 'FAILED FILES'
    Write-Host ("  $($script:Ansi.Red){0} blob(s) were pending but did not reach the destination intact.$($script:Ansi.Reset)" -f $failed.Count)
    Write-Host ('  See AzCopy logs in {0}' -f $logDir)
    Write-Host ''
    $failed | Sort-Object | ForEach-Object {
        $size = $sourceInventory[$_].Length
        Write-Host ("  $($script:Ansi.Red){0,-60}$($script:Ansi.Reset) {1,10}" -f $_, (Format-FileSize $size))
    }
}

$postStatus = Show-MigrationStatus -Source $sourceInventory `
                                   -Destination $destInventoryAfter `
                                   -Label 'POST-MIGRATION STATUS'

if ($azCopyError) {
    throw $azCopyError
}
if ($postStatus.PendingCount -ne 0) {
    throw "Post-migration pending is not zero ($($postStatus.PendingCount)). See logs in $logDir"
}

# Final validation
Write-Log 'VALIDATION'
$validation = Test-MigrationCompleteness -Source $sourceInventory `
                                         -Destination $destInventoryAfter
Write-Host ('Comparing {0} source blob(s) against {1} destination blob(s)...' -f $validation.SourceCount, $validation.DestCount)
Write-Host ''
if ($validation.Passed) {
    Write-Host ("$($script:Ansi.Green)PASS$($script:Ansi.Reset) -- destination has all {0} blob(s) with matching names and sizes." -f $validation.SourceCount)
    if ($validation.Md5VerifiedCount -gt 0) {
        Write-Host ("$($script:Ansi.Green)MD5 verified:$($script:Ansi.Reset) {0} of {1} blob(s)." -f $validation.Md5VerifiedCount, $validation.SourceCount)
    }
    if ($validation.NoMd5Count -gt 0) {
        Write-Host ("$($script:Ansi.Yellow)NOTE:$($script:Ansi.Reset) {0} blob(s) have no Content-MD5 set; checksum couldn't be compared for those." -f $validation.NoMd5Count)
    }
    Write-Host 'Source data left untouched.'
    Write-Host ''
    Show-BlobComparison -Source $sourceInventory -Destination $destInventoryAfter
}
else {
    Write-Host ''
    Show-BlobComparison -Source $sourceInventory -Destination $destInventoryAfter
    Write-Host ''
    Write-Host "$($script:Ansi.Red)FAIL$($script:Ansi.Reset) -- validation issues found:"
    foreach ($issue in $validation.Issues) {
        Write-Host ("  $($script:Ansi.Red){0}$($script:Ansi.Reset)" -f $issue)
    }
    throw "Validation failed:`n$($validation.Issues -join "`n")"
}

Write-Host ''
Write-Host ('Completed at: {0:yyyy-MM-dd HH:mm:ss} UTC' -f [datetime]::UtcNow)
