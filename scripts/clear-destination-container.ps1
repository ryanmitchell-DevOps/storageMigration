#Requires -Version 7.0
#Requires -PSEdition Core
#Requires -Modules Az.Accounts, Az.Storage

[CmdletBinding(SupportsShouldProcess)]
param(
    [Parameter(Mandatory)][string]$SubscriptionId,
    [Parameter(Mandatory)][string]$StorageAccount,
    [Parameter(Mandatory)][string]$Container,

    [string]$LogDirectory
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'

# ANSI color codes -- Azure DevOps logs render these; pwsh 7+ supports `e
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
        throw "Container '$Container' does not exist in storage account '$AccountName'. Ensure the container exists before running cleanup."
    }
    Write-Host ("$($script:Ansi.Green)Container '{0}' found in '{1}'.$($script:Ansi.Reset)" -f $Container, $AccountName)
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
            Name     = $_.Name
            Length   = $_.Length
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
        Write-Host ('    {0,-50} {1,-10} {2,10}' -f $_, $SizeSource[$_].BlobType, (Format-FileSize $SizeSource[$_].Length))
    }
    if ($Names.Count -gt $shown) { Write-Host "    ... and $($Names.Count - $shown) more" }
}

function Set-AzCopyEnvironment {
    param([string]$LogDirectory)
    # PSCRED inherits the Az PowerShell session established by AzurePowerShell@5.
    $env:AZCOPY_AUTO_LOGIN_TYPE = 'PSCRED'
    $env:AZCOPY_LOG_LOCATION = $LogDirectory
    $env:AZCOPY_JOB_PLAN_LOCATION = $LogDirectory
}

function Invoke-AzCopyRemove {
    param(
        [string]$Account,
        [string]$Container
    )
    $url = "https://$Account.blob.core.windows.net/$Container"

    $azArgs = @(
        'rm', $url,
        '--recursive=true'
    )

    Write-Host "##[command]azcopy $($azArgs -join ' ')"
    & azcopy @azArgs | ForEach-Object { Write-Host $_ }
    if ($LASTEXITCODE -ne 0) {
        throw "AzCopy rm failed with exit code $LASTEXITCODE. See logs in $env:AZCOPY_LOG_LOCATION"
    }
}

# Script
$cleanupStart = [datetime]::UtcNow
$logDir = Write-LogDirectory -Log $LogDirectory
Set-AzCopyEnvironment -LogDirectory $logDir

Write-Log 'CLEANUP CONFIGURATION -- target container details for this run'
Write-Host ('Start time:        {0:yyyy-MM-dd HH:mm:ss} UTC' -f $cleanupStart)
Write-Host ('Log directory:     {0}' -f $logDir)
Write-Host ''
Write-Host 'Target (destination to be cleared)'
Write-Host ('  Subscription:    {0}' -f $SubscriptionId)
Write-Host ('  Storage account: {0}' -f $StorageAccount)
Write-Host ('  Container:       {0}' -f $Container)
Write-Host ('  URL:             https://{0}.blob.core.windows.net/{1}' -f $StorageAccount, $Container)

Write-Log 'PREPARATION CHECKS -- verifying container exists and inventorying blobs to be deleted'

Write-Host '[1/2] Verifying target container exists...'
Set-SubscriptionContext -SubscriptionId $SubscriptionId
$ctx = Get-StorageContext -AccountName $StorageAccount
Assert-ContainerExists -Context $ctx -Container $Container -AccountName $StorageAccount

Write-Host '[2/2] Inventorying target container...'
$inventoryBefore = Get-BlobInventory -Context $ctx -Container $Container
Write-Host ('       Inventoried: {0} blob(s)' -f $inventoryBefore.Count)

if ($inventoryBefore.Count -eq 0) {
    Write-Log 'NOTHING TO DELETE -- container is already empty'
    Write-Host ("$($script:Ansi.Green)PASS$($script:Ansi.Reset) -- '{0}/{1}' has no blobs; nothing to clean up." -f $StorageAccount, $Container)
    return
}

$totalSize = 0L
foreach ($v in $inventoryBefore.Values) { $totalSize += $v.Length }

Write-Log 'PRE-CLEANUP SUMMARY -- blobs that will be deleted'
Write-Host ('Blobs to delete:   {0}' -f $inventoryBefore.Count)
Write-Host ('Total size:        {0}' -f (Format-FileSize $totalSize))
Show-BlobList -Heading 'Scheduled for deletion' `
              -Names @($inventoryBefore.Keys) `
              -SizeSource $inventoryBefore `
              -Color $script:Ansi.Yellow

# Deletion
if ($PSCmdlet.ShouldProcess(
    "$($inventoryBefore.Count) blob(s) totalling $(Format-FileSize $totalSize)",
    "Delete from $StorageAccount/$Container")) {

    Write-Log 'DELETING -- removing blobs from target container'
    $azCopyError = $null
    try {
        Invoke-AzCopyRemove -Account $StorageAccount -Container $Container
    }
    catch {
        $azCopyError = $_
        Write-Warning $azCopyError.Exception.Message
    }
}
else {
    Write-Warning 'Dry run (-WhatIf) -- no data deleted.'
    return
}

# Re-inventory to verify outcome
$inventoryAfter = Get-BlobInventory -Context $ctx -Container $Container

$deletedCount = $inventoryBefore.Count - $inventoryAfter.Count
$remaining = @($inventoryAfter.Keys)

Write-Log 'POST-CLEANUP VERIFICATION -- confirming container state after deletion'
Write-Host ('Before:  {0} blob(s)' -f $inventoryBefore.Count)
Write-Host ('After:   {0} blob(s)' -f $inventoryAfter.Count)
Write-Host ('Deleted: {0} blob(s)' -f $deletedCount)
Write-Host ''

if ($remaining.Count -eq 0) {
    Write-Host ("$($script:Ansi.Green)PASS$($script:Ansi.Reset) -- container '{0}/{1}' is empty. All {2} blob(s) deleted." -f $StorageAccount, $Container, $inventoryBefore.Count)
}
else {
    Write-Host ("$($script:Ansi.Red)FAIL$($script:Ansi.Reset) -- {0} blob(s) remain in '{1}/{2}' after deletion attempt:" -f $remaining.Count, $StorageAccount, $Container)
    Show-BlobList -Heading 'Remaining blobs' `
                  -Names $remaining `
                  -SizeSource $inventoryAfter `
                  -Color $script:Ansi.Red
}

$cleanupEnd = [datetime]::UtcNow
$elapsed = $cleanupEnd - $cleanupStart
$elapsedText = if ($elapsed.TotalDays -ge 1) {
    '{0} day(s) {1:hh\:mm\:ss}' -f [int]$elapsed.TotalDays, $elapsed
} else {
    '{0:hh\:mm\:ss}' -f $elapsed
}

Write-Log 'CLEANUP TIME -- total elapsed duration for this run'
Write-Host ('Cleanup started:  {0:yyyy-MM-dd HH:mm:ss} UTC' -f $cleanupStart)
Write-Host ('Cleanup ended:    {0:yyyy-MM-dd HH:mm:ss} UTC' -f $cleanupEnd)
Write-Host ('Total time elapsed: {0}' -f $elapsedText)

# Failure throws happen here, after the summary and timing have been logged, so
# the user can see what was deleted and what (if anything) remains before the
# pipeline step exits.
if ($azCopyError) {
    throw $azCopyError
}
if ($remaining.Count -gt 0) {
    throw "Cleanup incomplete -- $($remaining.Count) blob(s) still present in '$StorageAccount/$Container'."
}
