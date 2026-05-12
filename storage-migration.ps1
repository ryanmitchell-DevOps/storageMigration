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
    Write-Host "Log directory: $dir"
    return $dir
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
    Write-Host ('Source blobs: {0}' -f $sourceCount)
    Write-Host ('Destination blobs: {0}' -f $destCount)
    Write-Host ('Already migrated: {0} ({1}%)' -f ($sourceCount - $pendingCount), $migratedPct)
    Write-Host ('Pending: {0} ({1}%)' -f $pendingCount, $pendingPct)

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

# Script
$logDir = Write-LogDirectory -Log $LogDirectory
Set-AzCopyEnvironment -LogDirectory $logDir

Write-Log "Migration: $SourceStorageAccount/$SourceContainer -> $DestStorageAccount/$DestContainer"
Write-Host "Source sub: $SourceSubscriptionId"
Write-Host "Dest sub: $DestSubscriptionId"

# Pre-check: verify both containers exist before doing any work
Set-SubscriptionContext -SubscriptionId $SourceSubscriptionId
$sourceCtx = Get-StorageContext -AccountName $SourceStorageAccount
Assert-ContainerExists -Context $sourceCtx -Container $SourceContainer -AccountName $SourceStorageAccount

Set-SubscriptionContext -SubscriptionId $DestSubscriptionId
$destCtx = Get-StorageContext -AccountName $DestStorageAccount
Assert-ContainerExists -Context $destCtx -Container $DestContainer -AccountName $DestStorageAccount

# Pre-check: inventory both containers
Set-SubscriptionContext -SubscriptionId $SourceSubscriptionId
$sourceInventory = Get-BlobInventory -Context $sourceCtx -Container $SourceContainer

Set-SubscriptionContext -SubscriptionId $DestSubscriptionId
$destInventoryBefore = Get-BlobInventory -Context $destCtx -Container $DestContainer

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
}
else {
    throw "Validation failed:`n$($validation.Issues -join "`n")"
}
