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

# ANSI colour codes — red for failures, yellow for warnings. Section headings use ##[section] instead.
$script:Ansi = @{
    Reset  = "`e[0m"
    Red    = "`e[91m"
    Yellow = "`e[93m"
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

# Resolves and creates the AzCopy log directory from -Path, ADO env var, or a temp path.
function Initialize-LogDirectory {
    [OutputType([string])]
    param([string]$Path)
    if ($Path) { $dir = $Path }
    elseif ($env:BUILD_ARTIFACTSTAGINGDIRECTORY) {
        $dir = Join-Path $env:BUILD_ARTIFACTSTAGINGDIRECTORY 'azcopy-logs'
    }
    else { $dir = Join-Path ([IO.Path]::GetTempPath()) "azcopy-logs-$(Get-Date -Format yyyyMMddHHmmss)" }

    if (-not (Test-Path $dir)) { New-Item -ItemType Directory -Path $dir -Force -WhatIf:$false | Out-Null }
    return $dir
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

# Truncates text to a fixed column width, appending '…' if it overflows.
function Format-Cell {
    [OutputType([string])]
    param([string]$Text, [int]$Width)
    if ($Text.Length -gt $Width) { return $Text.Substring(0, $Width - 1) + '…' }
    return $Text
}

# Throws if the named storage account is not visible in the current subscription.
function Assert-StorageAccountExists {
    param(
        [Parameter(Mandatory)][ValidateNotNullOrEmpty()][string]$AccountName
    )
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

# Builds an ordinal (case-sensitive) name → blob metadata dictionary for a container.
function Get-BlobInventory {
    [OutputType([System.Collections.Generic.Dictionary[string,object]])]
    param(
        [Parameter(Mandatory)]$Context,
        [Parameter(Mandatory)][ValidateNotNullOrEmpty()][string]$Container
    )
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

# Downloads a blob to a temp file and returns its MD5 checksum as base64.
function Get-RemoteBlobMD5 {
    [OutputType([string])]
    param(
        [Parameter(Mandatory)]$Context,
        [Parameter(Mandatory)][ValidateNotNullOrEmpty()][string]$Container,
        [Parameter(Mandatory)][ValidateNotNullOrEmpty()][string]$BlobName
    )
    $tempPath = Join-Path ([IO.Path]::GetTempPath()) ('blobcheck_' + [guid]::NewGuid().ToString() + '.bin')
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
        Remove-Item -LiteralPath $tempPath -Force -ErrorAction SilentlyContinue
    }
}

# Backfills MD5 in-place for inventory entries that lack Content-MD5 metadata; warns on failures.
function Update-InventoryMissingMd5 {
    [OutputType([int])]
    param(
        [Parameter(Mandatory)][System.Collections.IDictionary]$Inventory,
        [Parameter(Mandatory)]$Context,
        [Parameter(Mandatory)][ValidateNotNullOrEmpty()][string]$Container,
        [Parameter(Mandatory)][string[]]$Names
    )
    $resolved = 0
    $failures = [System.Collections.Generic.List[string]]::new()
    foreach ($name in $Names) {
        if (-not $Inventory.ContainsKey($name)) { continue }
        $entry = $Inventory[$name]
        if ($entry.MD5) { continue }
        try {
            $entry.MD5 = Get-RemoteBlobMD5 -Context $Context -Container $Container -BlobName $name
            $resolved++
        }
        catch {
            $failures.Add("$name -- $($_.Exception.Message)")
        }
    }
    if ($failures.Count -gt 0) {
        Write-Warning ('MD5 backfill failed for {0} blob(s); they will fall back to size-only comparison:' -f $failures.Count)
        $failures | Select-Object -First 10 | ForEach-Object { Write-Warning "  $_" }
        if ($failures.Count -gt 10) {
            Write-Warning ('  ... and {0} more (see {1} for full AzCopy logs if related)' -f ($failures.Count - 10), $env:AZCOPY_LOG_LOCATION)
        }
    }
    return $resolved
}

# Prints a truncated, sorted blob list with type and size columns.
function Show-BlobList {
    param(
        [string]$Heading,
        [string[]]$Names,
        [System.Collections.IDictionary]$SizeSource,
        [int]$Limit = 50,
        [string]$Color = '',
        [string]$FilePath = ''
    )
    if ($Names.Count -eq 0) { return }
    $sorted = @($Names | Sort-Object)
    $shown = [Math]::Min($Names.Count, $Limit)
    Write-Host ''
    Write-Host "  $(Colorize "$Heading`:" $Color)"
    $sorted | Select-Object -First $shown | ForEach-Object {
        Write-Host ('    {0,-50} {1,-10} {2,10}' -f (Format-Cell $_ 50), $SizeSource[$_].BlobType, (Format-FileSize $SizeSource[$_].Length))
    }
    if ($Names.Count -gt $shown) {
        $more = " ..... and $($Names.Count - $shown) more"
        if ($FilePath) { $more += "(full list: $(Split-Path $FilePath -Leaf))" }
        Write-Host "    $more"
    }
    if ($FilePath) {
        $lines = $sorted | ForEach-Object { 
            ('{0,-80} {1,-12} {2,15}' -f $_, $SizeSource[$_].BlobType, (Format-FileSize $SizeSource[$_].Length))
        }
        Set-Content -Path $FilePath -Value $lines -WhatIf:$false
    }
}

# Single pass: buckets every source blob as missing, size-mismatched, MD5-mismatched, noMd5, or matched.
function Get-BlobClassification {
    [OutputType([pscustomobject])]
    param(
        [Parameter(Mandatory)][System.Collections.IDictionary]$Source,
        [Parameter(Mandatory)][System.Collections.IDictionary]$Destination
    )
    $missing       = [System.Collections.Generic.List[string]]::new()
    $sizeMismatch  = [System.Collections.Generic.List[string]]::new()
    $md5Mismatch   = [System.Collections.Generic.List[string]]::new()
    $md5Differing  = [System.Collections.Generic.List[string]]::new()
    $noMd5         = [System.Collections.Generic.List[string]]::new()
    $matched       = [System.Collections.Generic.List[string]]::new()

    foreach ($name in $Source.Keys) {
        if (-not $Destination.ContainsKey($name)) {
            $missing.Add($name)
            continue
        }
        $srcBlob = $Source[$name]
        $dstBlob = $Destination[$name]

        if ($srcBlob.MD5 -and $dstBlob.MD5 -and $srcBlob.MD5 -ne $dstBlob.MD5) {
            $md5Differing.Add($name)
        }

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
        SourceCount        = $Source.Count
        DestCount          = $Destination.Count
        MissingNames       = @($missing)
        SizeMismatchNames  = @($sizeMismatch)
        Md5MismatchNames   = @($md5Mismatch)
        Md5DifferingNames  = @($md5Differing)
        NoMd5Names         = @($noMd5)
        MatchedNames       = @($matched)
        DestOnlyNames      = $destOnly
    }
}

# Classifies source vs destination blobs and returns a single rich status object
# used for both the printed status block and the pass/fail decision.
function Get-MigrationStatus {
    [OutputType([pscustomobject])]
    param(
        [Parameter(Mandatory)][System.Collections.IDictionary]$Source,
        [Parameter(Mandatory)][System.Collections.IDictionary]$Destination
    )
    $c = Get-BlobClassification -Source $Source -Destination $Destination

    $alreadyInSync = @($c.MatchedNames) + @($c.NoMd5Names)
    $pending       = @($c.MissingNames) + @($c.SizeMismatchNames) + @($c.Md5MismatchNames)
    $diverged      = @($c.SizeMismatchNames) + @($c.Md5MismatchNames)

    $pendingPct = if ($c.SourceCount -gt 0) {
        [math]::Round(($pending.Count / $c.SourceCount) * 100, 2)
    } else { 0 }
    $migratedPct = [math]::Round(100 - $pendingPct, 2)

    $issues = [System.Collections.Generic.List[string]]::new()
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

    return [pscustomobject]@{
        SourceCount        = $c.SourceCount
        DestCount          = $c.DestCount
        MissingNames       = $c.MissingNames
        SizeMismatchNames  = $c.SizeMismatchNames
        Md5MismatchNames   = $c.Md5MismatchNames
        DestOnlyNames      = $c.DestOnlyNames
        DestOnlyCount      = $c.DestOnlyNames.Count
        AlreadyInSyncNames = $alreadyInSync
        PendingNames       = $pending
        PendingCount       = $pending.Count
        DivergedNames      = $diverged
        DivergedCount      = $diverged.Count
        MigratedPct        = $migratedPct
        PendingPct         = $pendingPct
        Md5VerifiedCount   = $c.MatchedNames.Count
        NoMd5Count         = $c.NoMd5Names.Count
        Passed             = ($issues.Count -eq 0)
        Issues             = $issues
    }
}

# Renders a status object to console and (when LogDirectory + FilePrefix given) to a per-blob CSV.
function Show-MigrationStatus {
    param(
        [Parameter(Mandatory)][pscustomobject]$Status,
        [Parameter(Mandatory)][string]$Label,
        [Parameter(Mandatory)][string]$SourceContainer,
        [Parameter(Mandatory)][string]$DestContainer,
        [Parameter(Mandatory)][System.Collections.IDictionary]$Source,
        [Parameter(Mandatory)][System.Collections.IDictionary]$Destination,
        [string]$LogDirectory = '',
        [string]$FilePrefix = ''
    )
    Write-Log $Label
    Write-Host ("Blobs in source '{0}': {1}" -f $SourceContainer, $Status.SourceCount)
    Write-Host ("Blobs in destination '{0}': {1}" -f $DestContainer, $Status.DestCount)
    Write-Host ('Source blobs found in destination (migrated): {0} ({1}%)' -f $Status.AlreadyInSyncNames.Count, $Status.MigratedPct)
    Write-Host ('Source blobs pending (not yet in destination): {0} ({1}%)  [missing: {2}, size-mismatch: {3}, md5-mismatch: {4}]' -f `
        $Status.PendingCount, $Status.PendingPct, $Status.MissingNames.Count, $Status.SizeMismatchNames.Count, $Status.Md5MismatchNames.Count)
    if ($Status.DestOnlyCount -gt 0) {
        Write-Host ('Destination-only blobs (not in source, preserved): {0}' -f $Status.DestOnlyCount)
    }

    if (-not ($LogDirectory -and $FilePrefix)) { return }

    $csvPath = Join-Path $LogDirectory "$FilePrefix-blob-status.csv"
    $rows = [System.Collections.Generic.List[object]]::new()
    foreach ($name in (@($Source.Keys) | Sort-Object)) {
        $src = $Source[$name]
        $hasDst = $Destination.ContainsKey($name)
        $dst = if ($hasDst) { $Destination[$name] } else { $null }
        $rowStatus = if (-not $hasDst)                          { 'Missing' }
                     elseif ($src.Length -ne $dst.Length)       { 'SizeMismatch' }
                     elseif (-not $src.MD5 -or -not $dst.MD5)   { 'Matched (size only -- no MD5)' }
                     elseif ($src.MD5 -ne $dst.MD5)             { 'Md5Mismatch' }
                     else                                       { 'Matched' }
        $rows.Add([pscustomobject]@{
            Name       = $name
            BlobType   = $src.BlobType
            SourceSize = $src.Length
            DestSize   = if ($hasDst) { $dst.Length } else { '' }
            SourceMD5  = $src.MD5
            DestMD5    = if ($hasDst) { $dst.MD5 } else { '' }
            Status     = $rowStatus
        })
    }
    foreach ($name in ($Status.DestOnlyNames | Sort-Object)) {
        $dst = $Destination[$name]
        $rows.Add([pscustomobject]@{
            Name       = $name
            BlobType   = $dst.BlobType
            SourceSize = ''
            DestSize   = $dst.Length
            SourceMD5  = ''
            DestMD5    = $dst.MD5
            Status     = 'DestOnly'
        })
    }
    $rows | Export-Csv -NoTypeInformation -Path $csvPath -WhatIf:$false
    Write-Host ''
    Write-Host ('Per-blob status written to: {0}' -f (Split-Path $csvPath -Leaf))
}

# Runs azcopy copy for an explicit blob list.
function Invoke-AzCopyByList {
    param(
        [Parameter(Mandatory)][ValidateNotNullOrEmpty()][string]$SourceAccount,
        [Parameter(Mandatory)][ValidateNotNullOrEmpty()][string]$SourceContainer,
        [Parameter(Mandatory)][ValidateNotNullOrEmpty()][string]$DestAccount,
        [Parameter(Mandatory)][ValidateNotNullOrEmpty()][string]$DestContainer,
        [string[]]$BlobNames
    )
    if ($BlobNames.Count -eq 0) { return }

    $sourceUrl = "https://$SourceAccount.blob.core.windows.net/$SourceContainer"
    $destUrl   = "https://$DestAccount.blob.core.windows.net/$DestContainer"

    $listFile = Join-Path ([IO.Path]::GetTempPath()) ('azcopy-list-' + [guid]::NewGuid().ToString() + '.txt')
    try {
        [System.IO.File]::WriteAllLines($listFile, $BlobNames)

        $state = @{ LastProgress = [datetime]::UtcNow }
        & azcopy copy $sourceUrl $destUrl `
             --list-of-files $listFile `
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
    finally {
        Remove-Item -LiteralPath $listFile -Force -ErrorAction SilentlyContinue
    }
}

# Prints a per-blob comparison table showing size and MD5 match status for every source blob.
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
                Colorize ('{0,-8}' -f 'MISMATCH') $script:Ansi.Red
            }

            $md5Cell = if (-not $srcBlob.MD5 -or -not $dstBlob.MD5) {
                '{0,-8}' -f 'N/A'
            } elseif ($srcBlob.MD5 -eq $dstBlob.MD5) {
                '{0,-8}' -f 'OK'
            } else {
                Colorize ('{0,-8}' -f 'MISMATCH') $script:Ansi.Red
            }

            Write-Host (($base -f (Format-Cell $name 50), $srcBlob.BlobType, (Format-FileSize $srcSize), (Format-FileSize $dstSize)) + "  $sizeCell  $md5Cell")
        }
        else {
            $sizeCell = Colorize ('{0,-8}' -f 'MISSING') $script:Ansi.Red
            $md5Cell  = Colorize ('{0,-8}' -f '-')       $script:Ansi.Red
            Write-Host (($base -f (Format-Cell $name 50), $srcBlob.BlobType, (Format-FileSize $srcSize), 'MISSING') + "  $sizeCell  $md5Cell")
        }
    }
    if ($names.Count -gt $shown) {
        Write-Host ('    ... and {0} more (per-blob detail in post-blob-status.csv)' -f ($names.Count - $shown))
    }
    Write-Host (($base -f ('-' * 50), ('-' * 10), ('-' * 20), ('-' * 20)) + ($tail -f ('-' * 8), ('-' * 8)))
    Write-Host ($base -f 'TOTAL', '', (Format-FileSize $totalSrc), (Format-FileSize $totalDst))
}

# ── SCRIPT ────────────────────────────────────────────────────────────────────
$migrationStart = [datetime]::UtcNow
$logDir = Initialize-LogDirectory -Path $LogDirectory

# PSCRED inherits the Az PowerShell session established by AzurePowerShell@5.
$env:AZCOPY_AUTO_LOGIN_TYPE = 'PSCRED'
$env:AZCOPY_LOG_LOCATION = $logDir
$env:AZCOPY_JOB_PLAN_LOCATION = $logDir

if (-not (Get-Command azcopy -ErrorAction SilentlyContinue)) {
    throw 'azcopy executable not found in PATH. Install it on the agent (e.g. via the AzureCLI@2 task or a dedicated AzCopy install step) before running this script.'
}

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


# ── PRE-CHECKS ── verify accounts/containers exist and snapshot blob inventories ─
Write-Log 'PREPARATION CHECKS -- verifying storage accounts, containers, and inventorying blobs before migration'

$step = 0; $totalSteps = 3

$step++; Write-Host "[$step/$totalSteps] Verifying source storage account and container exist..."
$srcContext = Set-AzContext -SubscriptionId $SourceSubscriptionId -ErrorAction Stop -WarningAction SilentlyContinue -WhatIf:$false
Assert-StorageAccountExists -AccountName $SourceStorageAccount
$sourceCtx = New-AzStorageContext -StorageAccountName $SourceStorageAccount -UseConnectedAccount
Assert-ContainerExists -Context $sourceCtx -Container $SourceContainer -AccountName $SourceStorageAccount

$step++; Write-Host "[$step/$totalSteps] Verifying destination storage account and container exist..."
$null = Set-AzContext -SubscriptionId $DestSubscriptionId -ErrorAction Stop -WarningAction SilentlyContinue -WhatIf:$false
Assert-StorageAccountExists -AccountName $DestStorageAccount
$destCtx = New-AzStorageContext -StorageAccountName $DestStorageAccount -UseConnectedAccount
Assert-ContainerExists -Context $destCtx -Container $DestContainer -AccountName $DestStorageAccount

$step++; Write-Host "[$step/$totalSteps] Inventorying source and destination containers..."
# Data-plane calls use the storage context's own AAD token, so the active
$sourceInventory = Get-BlobInventory -Context $sourceCtx -Container $SourceContainer
$destInventoryBefore = Get-BlobInventory -Context $destCtx -Container $DestContainer
Write-Host ('       Source inventoried:      {0} blob(s)' -f $sourceInventory.Count)
Write-Host ('       Destination inventoried: {0} blob(s)' -f $destInventoryBefore.Count)

$md5Candidates = [System.Collections.Generic.List[string]]::new()
foreach ($name in $sourceInventory.Keys) {
    if (-not $destInventoryBefore.ContainsKey($name)) { continue }
    $s = $sourceInventory[$name]
    $d = $destInventoryBefore[$name]
    if ($s.Length -ne $d.Length) { continue }
    if (-not $s.MD5 -or -not $d.MD5) { $md5Candidates.Add($name) }
}
if ($md5Candidates.Count -gt 0) {
    Write-Host ('       {0} same-size blob(s) lack Content-MD5 metadata. Hashing content to verify...' -f $md5Candidates.Count)
    $srcResolved = Update-InventoryMissingMd5 -Inventory $sourceInventory -Context $sourceCtx -Container $SourceContainer -Names $md5Candidates
    $dstResolved = Update-InventoryMissingMd5 -Inventory $destInventoryBefore -Context $destCtx -Container $DestContainer -Names $md5Candidates
    Write-Host ('       MD5 backfilled: {0} source blob(s), {1} destination blob(s).' -f $srcResolved, $dstResolved)
}


# ── MIGRATION ── compare source vs destination; identify pending and diverged blobs ─
$preStatus = Get-MigrationStatus -Source $sourceInventory -Destination $destInventoryBefore
Show-MigrationStatus -Status $preStatus `
                     -Label 'PRE-MIGRATION STATUS -- snapshot of source vs destination before any copy operations' `
                     -SourceContainer $SourceContainer `
                     -DestContainer $DestContainer `
                     -Source $sourceInventory `
                     -Destination $destInventoryBefore `
                     -LogDirectory $logDir `
                     -FilePrefix 'pre'

$divergedNames = @($preStatus.SizeMismatchNames) + @($preStatus.Md5MismatchNames)
if ($divergedNames.Count -gt 0) {
    Write-Log ('DESTINATION DIVERGENCE DETECTED -- these blobs will NOT be copied, pipeline WILL fail')
    Write-Host (Colorize ("{0} blob(s) in '{1}' differ from source by size or MD5." -f $divergedNames.Count, $DestContainer) $script:Ansi.Red)
    Write-Host ('The destination versions will be preserved (NOT overwritten). Missing-in-destination')
    Write-Host ('blobs will still be copied below, but the pipeline will FAIL at the end with these')
    Write-Host ('blobs listed. Reconcile manually and re-run to clear the divergence.')
    Write-Host ''
    if ($preStatus.SizeMismatchNames.Count -gt 0) {
        Write-Host ('  Size mismatch ({0}):' -f $preStatus.SizeMismatchNames.Count)
        $preStatus.SizeMismatchNames | Sort-Object | ForEach-Object {
            $nameCol = Colorize ('{0,-50}' -f (Format-Cell $_ 50)) $script:Ansi.Yellow
            Write-Host ("    $nameCol  source={0}  dest={1}" -f (Format-FileSize $sourceInventory[$_].Length), (Format-FileSize $destInventoryBefore[$_].Length))
        }
    }
    if ($preStatus.Md5MismatchNames.Count -gt 0) {
        Write-Host ('  MD5 mismatch ({0}):' -f $preStatus.Md5MismatchNames.Count)
        $preStatus.Md5MismatchNames | Sort-Object | ForEach-Object {
            $nameCol = Colorize ('{0,-50}' -f (Format-Cell $_ 50)) $script:Ansi.Yellow
            Write-Host ("    $nameCol  source-md5=$($sourceInventory[$_].MD5)  dest-md5=$($destInventoryBefore[$_].MD5)")
        }
    }
}

$divergedSet = [System.Collections.Generic.HashSet[string]]::new([string[]]$divergedNames, [System.StringComparer]::Ordinal)
$toCopy = @($preStatus.PendingNames | Where-Object { -not $divergedSet.Contains($_) })

# ── MIGRATING ── copy missing blobs via azcopy; diverged blobs are skipped ────
$azCopyError = $null
if ($preStatus.PendingCount -eq 0) {
    Write-Log 'NOTHING TO COPY -- destination already in sync with source. Running validation anyway to confirm.'
}
elseif ($toCopy.Count -eq 0) {
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
    foreach ($name in $toCopy) { $migrationTotalSize += $sourceInventory[$name].Length }
    Write-Host ('  Migrating: {0} blobs(s), total size: {1}' -f $toCopy.Count, (Format-FileSize $migrationTotalSize))

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

    $planDir = if ($env:BUILD_ARTIFACTSTAGINGDIRECTORY) {
        Join-Path $env:BUILD_ARTIFACTSTAGINGDIRECTORY 'migration-plan'
    } else {
        Join-Path ([IO.Path]::GetTempPath()) 'migration-plan'
    }
    if (-not (Test-Path $planDir)) { New-Item -ItemType Directory -Path $planDir -Force -WhatIf:$false | Out-Null }

    $planFile = Join-Path $planDir 'migration-plan.csv'
    $rows = [System.Collections.Generic.List[object]]::new()
    foreach ($name in ($preStatus.PendingNames | Sort-Object)) {
        $blob = $sourceInventory[$name]
        $rows.Add([pscustomobject]@{
            BlobName        = $name
            Action          = 'Copy'
            BlobType        = $blob.BlobType
            SizeBytes       = $blob.Length
            SourceAccount   = $SourceStorageAccount
            SourceContainer = $SourceContainer
            DestAccount     = $DestStorageAccount
            DestContainer   = $DestContainer
        })
    }
    foreach ($name in ($preStatus.SizeMismatchNames + $preStatus.Md5MismatchNames | Sort-Object)) {
        $blob = $sourceInventory[$name]
        $rows.Add([pscustomobject]@{
            BlobName        = $name
            Action          = 'Skip-Diverged'
            BlobType        = $blob.BlobType
            SizeBytes       = $blob.Length
            SourceAccount   = $SourceStorageAccount
            SourceContainer = $SourceContainer
            DestAccount     = $DestStorageAccount
            DestContainer   = $DestContainer
        })
    }
    $rows | Export-Csv -Path $planFile -NoTypeInformation -Encoding utf8 -WhatIf:$false

    Write-Host "Migration plan written to: $planFile"
    return
}

# ── POST-MIGRATION ── re-inventory destination; derive succeeded/failed/skipped ─
if ($toCopy.Count -gt 0) {
    $destInventoryAfter = Get-BlobInventory -Context $destCtx -Container $DestContainer
} else {
    $destInventoryAfter = $destInventoryBefore
}
$validation = Get-MigrationStatus -Source $sourceInventory -Destination $destInventoryAfter

$toCopySet = [System.Collections.Generic.HashSet[string]]::new([string[]]$toCopy, [System.StringComparer]::Ordinal)

$failed = [System.Collections.Generic.List[string]]::new()
foreach ($n in $validation.MissingNames)      { if ($toCopySet.Contains($n)) { $failed.Add($n) } }
foreach ($n in $validation.SizeMismatchNames) { if ($toCopySet.Contains($n)) { $failed.Add($n) } }
foreach ($n in $validation.Md5MismatchNames)  { if ($toCopySet.Contains($n)) { $failed.Add($n) } }

$failedSet = [System.Collections.Generic.HashSet[string]]::new([string[]]$failed, [System.StringComparer]::Ordinal)
$succeeded = @($toCopy | Where-Object { -not $failedSet.Contains($_) })
$skipped = $divergedNames

if ($succeeded.Count -gt 0) {
    Write-Log 'SUCCESSFULLY TRANSFERRED'
    if ($failed.Count -eq 0) {
        Write-Host ('  All {0} copied blob(s) transferred successfully.' -f $succeeded.Count)
    } else {
        Write-Host ('  {0} of {1} blob(s) transferred successfully.' -f $succeeded.Count, $toCopy.Count)
        Show-BlobList -Heading ("Transferred to $DestContainer") -Names $succeeded -SizeSource $sourceInventory -FilePath(Join-Path $logDir 'transferred.txt')
    }
}

if ($failed.Count -gt 0) {
    Write-Log 'FAILED FILES'
    Write-Host (Colorize ("  {0} blob(s) were copied but did not reach the destination intact." -f $failed.Count) $script:Ansi.Red)
    Write-Host ('  See AzCopy logs in {0}' -f $logDir)
    Write-Host ''
    $failed | Sort-Object | ForEach-Object {
        $nameCol = Colorize ('{0,-50}' -f (Format-Cell $_ 50)) $script:Ansi.Red
        Write-Host ("  $nameCol {0,-10} {1,10}" -f $sourceInventory[$_].BlobType, (Format-FileSize $sourceInventory[$_].Length))
    }
}

if ($skipped.Count -gt 0) {
    Write-Log 'SKIPPED FILES (destination divergence -- pipeline will FAIL)'
    Write-Host (Colorize ("  {0} blob(s) were preserved in destination because they diverge from source." -f $skipped.Count) $script:Ansi.Red)
    Write-Host ('  These were NOT overwritten. The pipeline will fail below.')
    Write-Host ('  Reconcile manually (delete the diverged dest blob or fix the source) and re-run.')
    Write-Host ''
    $skipped | Sort-Object | ForEach-Object {
        $nameCol = Colorize ('{0,-50}' -f (Format-Cell $_ 50)) $script:Ansi.Yellow
        Write-Host ("  $nameCol {0,-10}  source={1,10}  dest={2,10}" -f $sourceInventory[$_].BlobType, (Format-FileSize $sourceInventory[$_].Length), (Format-FileSize $destInventoryBefore[$_].Length))
    }
}

Show-MigrationStatus -Status $validation `
                     -Label 'POST-MIGRATION STATUS -- destination state after copy operations complete' `
                     -SourceContainer $SourceContainer `
                     -DestContainer $DestContainer `
                     -Source $sourceInventory `
                     -Destination $destInventoryAfter `
                     -LogDirectory $logDir `
                     -FilePrefix 'post'

# ── VALIDATION ── compare source vs destination by name, size, and MD5; fail on any mismatch ─
Write-Log ("VALIDATION -- comparing '{0}' to '{1}' by name, size, and MD5 checksum" -f $SourceContainer, $DestContainer)
Write-Host ("Comparing {0} blob(s) in '{1}' against {2} blob(s) in '{3}'..." -f $validation.SourceCount, $SourceContainer, $validation.DestCount, $DestContainer)
if ($validation.DestOnlyCount -gt 0) {
    Write-Host ('Note: destination contains {0} blob(s) not present in source (preserved -- only blobs listed in $toCopy are copied).' -f $validation.DestOnlyCount)
    Show-BlobList -Heading ("Only in $DestContainer") -Names $validation.DestOnlyNames -SizeSource $destInventoryAfter -FilePath(Join-Path $logDir 'dest-only.txt')
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
    Write-Host "$(Colorize 'FAIL' $script:Ansi.Red) -- validation issues found:"
    foreach ($issue in $validation.Issues) {
        Write-Host "  $(Colorize $issue $script:Ansi.Red)"
    }
}

Write-Log 'SUMMARY -- migration result'
Write-Host ('Source blobs:        {0}' -f $sourceInventory.Count)
Write-Host ('Already in sync:     {0}' -f $preStatus.AlreadyInSyncNames.Count)
Write-Host ('Pending:             {0}' -f $preStatus.PendingCount)
Write-Host ('  Migrated:          {0}' -f $succeeded.Count)
Write-Host ('  Failed:            {0}' -f $failed.Count)
Write-Host ('  Skipped (diverged): {0}' -f $skipped.Count)
if ($validation.DestOnlyCount -gt 0) {
    Write-Host ('Destination extras:  {0} (preserved, not in source)' -f $validation.DestOnlyCount)
}
$overallPass = $validation.Passed -and $skipped.Count -eq 0
$validationStatus = if ($overallPass) { 'PASS' } else { Colorize 'FAIL' $script:Ansi.Red }
Write-Host ("Validation:          {0}" -f $validationStatus)

if ($azCopyError) {
    Write-Log 'FAILURE: AZCOPY ERROR -- copy step reported a non-zero exit; raised after post-migration validation'
    Write-Host (Colorize $azCopyError.Exception.Message $script:Ansi.Red)
    throw $azCopyError.Exception
}
if ($skipped.Count -gt 0) {
    Write-Log 'FAILURE: DESTINATION DIVERGENCE -- blobs differ from source and were preserved (not overwritten)'
    throw "Destination divergence: $($skipped.Count) blob(s) in '$DestContainer' differ from source and were preserved (not overwritten). Reconcile manually before re-running. See SKIPPED FILES above for the list."
}
if (-not $validation.Passed) {
    Write-Log 'FAILURE: VALIDATION MISMATCH -- destination does not match source after copy'
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

    Write-Log 'MIGRATION & VALIDATION TIME -- total elapsed duration for this run'
    Write-Host ('Migration started:  {0:yyyy-MM-dd HH:mm:ss} UTC' -f $migrationStart)
    Write-Host ('Migration ended:    {0:yyyy-MM-dd HH:mm:ss} UTC' -f $migrationEnd)
    Write-Host ('Total time elapsed: {0}' -f $elapsedText)
}
