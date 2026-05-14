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
    Write-Host "Container '$Container' found in '$AccountName'."
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
        $map[$_.Name] = [pscustomobject]@{ Name = $_.Name; Length = $_.Length }
        $count++
        if ($count % 10000 -eq 0) { Write-Host " inventoried $count blobs..." }
    }
    return $map
}

function Show-MigrationStatus {
    param(
        [hashtable]$Source,
        [hashtable]$Destination,
        [string]$Label = 'Status'
    )
    $sourceCount = $Source.Count
    $destCount = $Destination.Count
    $pending = @($Source.Keys | Where-Object { -not $Destination.ContainsKey($_) })
    $pendingCount = $pending.Count
    $pendingPct = if ($sourceCount -gt 0) {
        [math]::Round(($pendingCount / $sourceCount) * 100, 2)
    } else { 0 }
    $migratedPct = [math]::Round(100 - $pendingPct, 2)

    Write-Log $Label
    Write-Host ('Source blobs:      {0}' -f $sourceCount)
    Write-Host ('Destination blobs: {0}' -f $destCount)
    Write-Host ('Already migrated:  {0} ({1}%)' -f ($sourceCount - $pendingCount), $migratedPct)
    Write-Host ('Pending:           {0} ({1}%)' -f $pendingCount, $pendingPct)
    if ($pendingCount -gt 0) {
        $listLimit = [Math]::Min($pendingCount, 50)
        Write-Host ''
        Write-Host '  Pending blobs:'
        $pending | Sort-Object | Select-Object -First $listLimit | ForEach-Object {
            Write-Host ('    {0,-60} {1,10}' -f $_, (Format-FileSize $Source[$_].Length))
        }
        if ($pendingCount -gt $listLimit) { Write-Host "    ... and $($pendingCount - $listLimit) more" }
    }

    return [pscustomobject]@{
        SourceCount    = $sourceCount
        DestCount      = $destCount
        PendingCount   = $pendingCount
        PendingPercent = $pendingPct
        PendingNames   = $pending
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

    return [pscustomobject]@{
        Passed      = ($issues.Count -eq 0)
        Issues      = $issues
        SourceCount = $Source.Count
        DestCount   = $Destination.Count
    }
}

function Show-BlobComparison {
    param([hashtable]$Source, [hashtable]$Destination)
    $fmt = '  {0,-60} {1,12} {2,12}  {3}'
    Write-Host ($fmt -f 'Blob Name', 'Source', 'Destination', 'Match')
    Write-Host ($fmt -f ('-' * 60), ('-' * 12), ('-' * 12), '-----')
    [long]$totalSrc = 0
    [long]$totalDst = 0
    foreach ($name in ($Source.Keys | Sort-Object)) {
        $srcSize = $Source[$name].Length
        $totalSrc += $srcSize
        if ($Destination.ContainsKey($name)) {
            $dstSize = $Destination[$name].Length
            $totalDst += $dstSize
            $match = if ($srcSize -eq $dstSize) { 'OK' } else { 'MISMATCH' }
            Write-Host ($fmt -f $name, (Format-FileSize $srcSize), (Format-FileSize $dstSize), $match)
        }
        else {
            Write-Host ($fmt -f $name, (Format-FileSize $srcSize), 'MISSING', 'MISSING')
        }
    }
    Write-Host ($fmt -f ('-' * 60), ('-' * 12), ('-' * 12), '-----')
    Write-Host ($fmt -f 'TOTAL', (Format-FileSize $totalSrc), (Format-FileSize $totalDst), '')
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
    Invoke-AzCopySync -SourceAccount $SourceStorageAccount `
                      -SourceContainer $SourceContainer `
                      -DestAccount $DestStorageAccount `
                      -DestContainer $DestContainer

    Write-Log 'TRANSFERRED FILES'
    [long]$transferTotal = 0
    $preStatus.PendingNames | Sort-Object | ForEach-Object {
        $size = $sourceInventory[$_].Length
        $transferTotal += $size
        Write-Host ('  {0,-60} {1,10}' -f $_, (Format-FileSize $size))
    }
    Write-Host ''
    Write-Host ('  Total: {0} file(s), {1}' -f $preStatus.PendingNames.Count, (Format-FileSize $transferTotal))
}
else {
    Write-Warning 'Dry run (-WhatIf) -- no data copied.'
    return
}

# Post-check
$destInventoryAfter = Get-BlobInventory -Context $destCtx -Container $DestContainer
$postStatus = Show-MigrationStatus -Source $sourceInventory `
                                   -Destination $destInventoryAfter `
                                   -Label 'POST-MIGRATION STATUS'

if ($postStatus.PendingCount -ne 0) {
    throw "Post-migration pending is not zero ($($postStatus.PendingCount)). See logs in $logDir"
}

# Final validation
Write-Log 'VALIDATION'
$validation = Test-MigrationCompleteness -Source $sourceInventory `
                                         -Destination $destInventoryAfter
if ($validation.Passed) {
    Write-Host ("PASS -- destination has all {0} blob(s) with matching names and sizes." -f $validation.SourceCount)
    Write-Host 'Source data left untouched.'
    Write-Host ''
    Show-BlobComparison -Source $sourceInventory -Destination $destInventoryAfter
}
else {
    Write-Host ''
    Show-BlobComparison -Source $sourceInventory -Destination $destInventoryAfter
    throw "Validation failed:`n$($validation.Issues -join "`n")"
}

Write-Host ''
Write-Host ('Completed at: {0:yyyy-MM-dd HH:mm:ss} UTC' -f [datetime]::UtcNow)
