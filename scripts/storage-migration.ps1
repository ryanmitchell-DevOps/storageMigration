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

    # Write the full per-blob ledgers (pre-blob-status.csv / post-blob-status.csv, including
    # DestOnly rows). On by default. The merge-join produces them in the same pass, so this is
    # cheap; set to $false only if you don't want the files.
    [bool]$BlobStatusReport = $true,

    # Cap how many missing blobs are copied in a single run (chunking). 0 = no limit (copy
    [ValidateRange(0, 2147483647)][long]$MaxFilesPerRun = 0,

    # First-run fast path for an EMPTY destination: skip the merge-join plan entirely and have
    # azcopy copy the WHOLE container recursively (it lists and copies in one streamed pass,
    # far faster than pre-scanning every source blob when all of them are going to be copied
    # anyway). No divergence check or validation is done -- re-run WITHOUT this switch afterwards
    # to reconcile anything missed and write the validation ledger.
    [switch]$CopyEntireContainer,

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

# Launches 'azcopy list' over one container as a child process, streaming JSON lines to a
# scratch file, and returns a handle so both containers can be listed IN PARALLEL. azcopy
# enumerates blobs far faster than paged Get-AzStorageBlob, and Name/size/Content-MD5/BlobType
# all ride along in the same listing -- no extra per-blob calls.
function Start-ContainerListing {
    [OutputType([pscustomobject])]
    param(
        [Parameter(Mandatory)][string]$Account,
        [Parameter(Mandatory)][string]$Container,
        # 'source' / 'destination' -- names the scratch files and error messages.
        [Parameter(Mandatory)][string]$Label
    )
    # The plan/preview run (-WhatIf) still needs real listings, and Start-Process would
    # otherwise no-op under an inherited -WhatIf.
    $WhatIfPreference = $false

    $stamp   = [guid]::NewGuid().ToString('N')
    $outFile = Join-Path ([IO.Path]::GetTempPath()) "azcopy-list-$Label-$stamp.jsonl"
    $errFile = Join-Path ([IO.Path]::GetTempPath()) "azcopy-list-$Label-$stamp.err"
    $url     = "https://$Account.blob.core.windows.net/$Container"

    # --machine-readable = exact byte counts. JSON output so blob names containing ';' (the
    # text format's field separator) can't corrupt the parse.
    $process = Start-Process -FilePath 'azcopy' `
        -ArgumentList @('list', $url, '--machine-readable', '--properties', 'ContentMD5;BlobType', '--output-type', 'json') `
        -RedirectStandardOutput $outFile -RedirectStandardError $errFile `
        -NoNewWindow -PassThru
    [pscustomobject]@{ Process = $process; OutFile = $outFile; ErrFile = $errFile; Url = $url; Label = $Label }
}

# Blocks until every parallel listing process exits, printing a heartbeat with the growing
# scratch-file sizes. Throws (with the tail of azcopy's stderr) if any listing failed.
function Wait-ContainerListing {
    param([Parameter(Mandatory)][pscustomobject[]]$Listing)
    $waitStart = [datetime]::UtcNow
    while (@($Listing | Where-Object { -not $_.Process.HasExited }).Count -gt 0) {
        Start-Sleep -Seconds 15
        $sizes = foreach ($l in $Listing) {
            $bytes = if (Test-Path -LiteralPath $l.OutFile) { (Get-Item -LiteralPath $l.OutFile).Length } else { 0 }
            '{0} {1}' -f $l.Label, (Format-FileSize $bytes)
        }
        Write-Host ('       listing... {0:hh\:mm\:ss} elapsed ({1})' -f ([datetime]::UtcNow - $waitStart), ($sizes -join ', '))
    }
    foreach ($l in $Listing) {
        if ($l.Process.ExitCode -ne 0) {
            $stderrTail = ''
            try { $stderrTail = (Get-Content -LiteralPath $l.ErrFile -Tail 5 -ErrorAction Stop) -join ' | ' } catch { }
            throw ('azcopy list of the {0} container failed with exit code {1} ({2}). {3}' -f $l.Label, $l.Process.ExitCode, $l.Url, $stderrTail)
        }
    }
}

# ── COMPILED MERGE-JOIN CORE ──────────────────────────────────────────────────
# The per-record cost of an interpreted PowerShell loop (~0.4 ms) turns a 900k-blob scan into
# 10+ minutes; the same merge-join compiled runs in seconds. Statuses and CSV formats are
# identical to the previous in-script implementation. Compiled once per process (guarded
# below); Add-Type costs ~1s at script start.
$script:MergeJoinSource = @'
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Text.Json;

namespace StorageMigration
{
    // Counters returned to PowerShell.
    public sealed class MergeJoinResult
    {
        public long SourceCount;
        public long CopyCount;
        public long DeferredCount;
        public long CopyBytes;
        public long InSyncCount;
        public long SizeMismatchCount;
        public long Md5MismatchCount;
        public long DestOnlyCount;
        public List<string> DivergedSample = new List<string>();
    }

    // One blob row from a listing.
    internal sealed class BlobRecord
    {
        public string Name;
        public long Length;
        public string Md5;
        public string BlobType;
    }

    // Streams one azcopy JSON listing file as blob records, in whatever order azcopy emitted
    // them (NOT necessarily sorted -- azcopy parallelizes enumeration on large containers).
    // Skips azcopy's non-blob envelopes (Init/Info/ListSummary); anything unparseable throws
    // with the offending line, so an azcopy output-format change fails the run loudly instead
    // of silently mis-planning.
    internal sealed class ListingReader : IDisposable
    {
        private readonly StreamReader reader;
        private readonly string label;
        private long objectCount;
        private bool sawLegacyRows;

        public string Name;
        public long Length;
        public string Md5;
        public string BlobType;

        public ListingReader(string path, string label)
        {
            this.reader = new StreamReader(path);
            this.label = label;
        }

        public bool MoveNext()
        {
            string line;
            while ((line = reader.ReadLine()) != null)
            {
                if (line.Length == 0) { continue; }
                try
                {
                    using (JsonDocument envelope = JsonDocument.Parse(line))
                    {
                        string messageType = envelope.RootElement.GetProperty("MessageType").GetString();
                        if (messageType != "ListObject")
                        {
                            // Old azcopy (< 10.21) emitted blob rows as plain Info text; note
                            // that so end-of-file can fail loudly instead of reporting 0 blobs.
                            if (messageType == "Info" && line.Contains("Content Length")) { sawLegacyRows = true; }
                            continue;
                        }

                        // MessageContent is a JSON document in its own right (azcopy double-
                        // encodes it). ContentMD5/BlobType are omitted entirely when unset.
                        using (JsonDocument body = JsonDocument.Parse(envelope.RootElement.GetProperty("MessageContent").GetString()))
                        {
                            string name = null; long length = 0; bool haveLength = false;
                            string md5 = ""; string blobType = "";
                            foreach (JsonProperty prop in body.RootElement.EnumerateObject())
                            {
                                switch (prop.Name)
                                {
                                    case "Path":
                                        name = prop.Value.GetString();
                                        break;
                                    case "ContentLength":
                                        length = prop.Value.ValueKind == JsonValueKind.Number
                                            ? prop.Value.GetInt64()
                                            : long.Parse(prop.Value.GetString(), CultureInfo.InvariantCulture);
                                        haveLength = true;
                                        break;
                                    case "ContentMD5":
                                        if (prop.Value.ValueKind == JsonValueKind.String) { md5 = prop.Value.GetString(); }
                                        break;
                                    case "BlobType":
                                        if (prop.Value.ValueKind == JsonValueKind.String) { blobType = prop.Value.GetString(); }
                                        break;
                                }
                            }
                            if (name == null || !haveLength) { throw new InvalidDataException("ListObject row is missing Path or ContentLength"); }
                            Name = name; Length = length; Md5 = md5; BlobType = blobType;
                        }
                    }
                }
                catch (Exception ex)
                {
                    throw new InvalidDataException(string.Format(
                        "Unrecognized azcopy list output in the {0} listing ({1}): {2}", label, ex.Message, line));
                }

                objectCount++;
                return true;
            }
            if (objectCount == 0 && sawLegacyRows)
            {
                throw new InvalidDataException(string.Format(
                    "The {0} listing contains azcopy's legacy text rows instead of structured ListObject rows. Upgrade azcopy on the agent (>= 10.21).", label));
            }
            return false;
        }

        public void Dispose() { reader.Dispose(); }
    }

    // Yields a listing's records in ascending ordinal name order regardless of the order
    // azcopy emitted them, with duplicate rows dropped (azcopy's parallel enumeration has
    // produced both out-of-order and repeated rows on large containers). Records are sorted
    // in chunks that spill to disk, then k-way merged, so memory stays bounded (~one chunk)
    // no matter how many blobs are listed.
    internal sealed class SortedListingCursor : IDisposable
    {
        private const int ChunkSize = 250000;
        private readonly string label;
        private readonly List<string> chunkFiles = new List<string>();
        private readonly List<BinaryReader> chunkReaders = new List<BinaryReader>();
        private BlobRecord[] heads;          // current front record of each chunk (null = drained)
        private List<BlobRecord> single;     // in-memory path when everything fits in one chunk
        private int singleIndex;
        private string prevName;

        public string Name;
        public long Length;
        public string Md5;
        public string BlobType;

        public SortedListingCursor(string listingPath, string label)
        {
            this.label = label;
            List<BlobRecord> chunk = new List<BlobRecord>();
            try
            {
                using (ListingReader lr = new ListingReader(listingPath, label))
                {
                    while (lr.MoveNext())
                    {
                        chunk.Add(new BlobRecord { Name = lr.Name, Length = lr.Length, Md5 = lr.Md5, BlobType = lr.BlobType });
                        if (chunk.Count >= ChunkSize) { FlushChunk(chunk); }
                    }
                }
                if (chunkFiles.Count == 0)
                {
                    chunk.Sort(CompareByName);
                    single = chunk;
                }
                else
                {
                    if (chunk.Count > 0) { FlushChunk(chunk); }
                    heads = new BlobRecord[chunkFiles.Count];
                    for (int i = 0; i < chunkFiles.Count; i++)
                    {
                        chunkReaders.Add(new BinaryReader(new FileStream(chunkFiles[i], FileMode.Open, FileAccess.Read, FileShare.None, 1 << 16)));
                        heads[i] = ReadRecord(chunkReaders[i]);
                    }
                }
            }
            catch
            {
                Dispose();
                throw;
            }
        }

        public bool MoveNext()
        {
            while (true)
            {
                BlobRecord rec = NextRaw();
                if (rec == null) { return false; }
                // azcopy has been seen to repeat rows; identical names are the same blob.
                if (prevName != null && string.CompareOrdinal(prevName, rec.Name) == 0) { continue; }
                prevName = rec.Name;
                Name = rec.Name; Length = rec.Length; Md5 = rec.Md5; BlobType = rec.BlobType;
                return true;
            }
        }

        private BlobRecord NextRaw()
        {
            if (single != null)
            {
                return singleIndex < single.Count ? single[singleIndex++] : null;
            }
            int min = -1;
            for (int i = 0; i < heads.Length; i++)
            {
                if (heads[i] == null) { continue; }
                if (min < 0 || string.CompareOrdinal(heads[i].Name, heads[min].Name) < 0) { min = i; }
            }
            if (min < 0) { return null; }
            BlobRecord rec = heads[min];
            heads[min] = ReadRecord(chunkReaders[min]);
            return rec;
        }

        private void FlushChunk(List<BlobRecord> chunk)
        {
            chunk.Sort(CompareByName);
            // Named azcopy-list-* so the script's stale-scratch sweep reaps orphans.
            string path = Path.Combine(Path.GetTempPath(),
                "azcopy-list-sort-" + label + "-" + Guid.NewGuid().ToString("N") + ".bin");
            using (BinaryWriter w = new BinaryWriter(new FileStream(path, FileMode.CreateNew, FileAccess.Write, FileShare.None, 1 << 16)))
            {
                foreach (BlobRecord rec in chunk)
                {
                    w.Write(rec.Name);
                    w.Write(rec.Length);
                    w.Write(rec.Md5);
                    w.Write(rec.BlobType);
                }
            }
            chunkFiles.Add(path);
            chunk.Clear();
        }

        private static BlobRecord ReadRecord(BinaryReader r)
        {
            if (r.BaseStream.Position >= r.BaseStream.Length) { return null; }
            return new BlobRecord
            {
                Name = r.ReadString(),
                Length = r.ReadInt64(),
                Md5 = r.ReadString(),
                BlobType = r.ReadString()
            };
        }

        private static int CompareByName(BlobRecord a, BlobRecord b)
        {
            return string.CompareOrdinal(a.Name, b.Name);
        }

        public void Dispose()
        {
            foreach (BinaryReader r in chunkReaders) { try { r.Dispose(); } catch { } }
            foreach (string f in chunkFiles) { try { File.Delete(f); } catch { } }
        }
    }

    public static class ContainerMergeJoin
    {
        // Merge-joins two sorted listing files, writing the plan / copy list / ledger CSVs
        // (pass null or empty for any output to skip it).
        public static MergeJoinResult Run(
            string sourceListingPath, string destListingPath,
            string planCsvPath, string copyListPath, string ledgerCsvPath,
            long maxFilesPerRun, string progressLabel)
        {
            MergeJoinResult r = new MergeJoinResult();
            long scanned = 0;
            const long reportInterval = 500000;
            long nextReport = reportInterval;

            SortedListingCursor src = null;
            SortedListingCursor dst = null;
            StreamWriter plan = null;
            StreamWriter copyList = null;
            StreamWriter ledger = null;
            try
            {
                src = new SortedListingCursor(sourceListingPath, "source");
                dst = new SortedListingCursor(destListingPath, "destination");
                plan = string.IsNullOrEmpty(planCsvPath) ? null : new StreamWriter(planCsvPath, false);
                copyList = string.IsNullOrEmpty(copyListPath) ? null : new StreamWriter(copyListPath, false);
                ledger = string.IsNullOrEmpty(ledgerCsvPath) ? null : new StreamWriter(ledgerCsvPath, false);

                if (plan != null) { plan.WriteLine("BlobName,Action,BlobType,SizeBytes,Reason"); }
                if (ledger != null) { ledger.WriteLine("Name,BlobType,SourceSize,DestSize,SourceMD5,DestMD5,Status"); }

                bool haveSrc = src.MoveNext();
                bool haveDst = dst.MoveNext();
                while (haveSrc || haveDst)
                {
                    int cmp = !haveDst ? -1 : (!haveSrc ? 1 : string.CompareOrdinal(src.Name, dst.Name));
                    if (cmp < 0)
                    {
                        // Source only -> missing from destination.
                        r.SourceCount++;
                        if (maxFilesPerRun <= 0 || r.CopyCount < maxFilesPerRun)
                        {
                            // Within this run's budget -> copy it now.
                            r.CopyCount++;
                            r.CopyBytes += src.Length;
                            if (plan != null) { plan.WriteLine(Csv(src.Name) + ",Copy," + src.BlobType + "," + src.Length + ",Missing from destination"); }
                            if (copyList != null) { copyList.WriteLine(src.Name); }
                        }
                        else
                        {
                            // Over this run's cap -> leave it for a later run.
                            r.DeferredCount++;
                        }
                        if (ledger != null) { ledger.WriteLine(Csv(src.Name) + "," + src.BlobType + "," + src.Length + ",," + src.Md5 + ",,Missing"); }
                        scanned++;
                        haveSrc = src.MoveNext();
                    }
                    else if (cmp > 0)
                    {
                        // Destination only -> preserved, not in source.
                        r.DestOnlyCount++;
                        if (ledger != null) { ledger.WriteLine(Csv(dst.Name) + "," + dst.BlobType + ",," + dst.Length + ",," + dst.Md5 + ",DestOnly"); }
                        scanned++;
                        haveDst = dst.MoveNext();
                    }
                    else
                    {
                        // Same name in both -> compare size, then MD5 when both sides have one.
                        r.SourceCount++;
                        string status;
                        if (src.Length != dst.Length)
                        {
                            status = "SizeMismatch";
                            r.SizeMismatchCount++;
                            if (plan != null) { plan.WriteLine(Csv(src.Name) + ",Skip-Diverged," + src.BlobType + "," + src.Length + ",Size mismatch"); }
                            if (r.DivergedSample.Count < 50) { r.DivergedSample.Add(src.Name + "  (source=" + FormatSize(src.Length) + ", dest=" + FormatSize(dst.Length) + ")"); }
                        }
                        else if (string.IsNullOrEmpty(src.Md5) || string.IsNullOrEmpty(dst.Md5))
                        {
                            status = "Matched (size only -- no MD5)";
                            r.InSyncCount++;
                        }
                        else if (src.Md5 != dst.Md5)
                        {
                            status = "Md5Mismatch";
                            r.Md5MismatchCount++;
                            if (plan != null) { plan.WriteLine(Csv(src.Name) + ",Skip-Diverged," + src.BlobType + "," + src.Length + ",MD5 mismatch"); }
                            if (r.DivergedSample.Count < 50) { r.DivergedSample.Add(src.Name + "  (source-md5=" + src.Md5 + ", dest-md5=" + dst.Md5 + ")"); }
                        }
                        else
                        {
                            status = "Matched";
                            r.InSyncCount++;
                        }
                        if (ledger != null) { ledger.WriteLine(Csv(src.Name) + "," + src.BlobType + "," + src.Length + "," + dst.Length + "," + src.Md5 + "," + dst.Md5 + "," + status); }
                        scanned += 2;
                        haveSrc = src.MoveNext();
                        haveDst = dst.MoveNext();
                    }

                    if (scanned >= nextReport)
                    {
                        Console.WriteLine("       " + progressLabel + " " + r.SourceCount + " source / " + r.DestOnlyCount +
                            " dest-only blob(s)... (copy " + r.CopyCount + ", in-sync " + r.InSyncCount +
                            ", diverged " + (r.SizeMismatchCount + r.Md5MismatchCount) + ")");
                        nextReport += reportInterval;
                    }
                }
            }
            finally
            {
                if (src != null) { src.Dispose(); }
                if (dst != null) { dst.Dispose(); }
                if (plan != null) { plan.Dispose(); }
                if (copyList != null) { copyList.Dispose(); }
                if (ledger != null) { ledger.Dispose(); }
            }
            return r;
        }

        private static readonly char[] CsvSpecials = { ',', '"', '\r', '\n' };

        // Quotes a CSV field when it contains a comma, quote, or newline.
        private static string Csv(string value)
        {
            if (string.IsNullOrEmpty(value)) { return ""; }
            if (value.IndexOfAny(CsvSpecials) >= 0) { return "\"" + value.Replace("\"", "\"\"") + "\""; }
            return value;
        }

        // Matches the PowerShell Format-FileSize output (N2 GB / MB / KB, plain bytes).
        private static string FormatSize(long bytes)
        {
            if (bytes >= 1073741824L) { return ((double)bytes / 1073741824).ToString("N2") + " GB"; }
            if (bytes >= 1048576L) { return ((double)bytes / 1048576).ToString("N2") + " MB"; }
            if (bytes >= 1024L) { return ((double)bytes / 1024).ToString("N2") + " KB"; }
            return bytes + " B";
        }
    }
}
'@
if (-not ('StorageMigration.ContainerMergeJoin' -as [type])) {
    Add-Type -TypeDefinition $script:MergeJoinSource -ReferencedAssemblies @(
        'System.Runtime', 'System.Collections', 'System.Console', 'System.Text.Json', 'System.Memory', 'System.IO.FileSystem'
    )
}

function Invoke-ContainerMergeJoin {
    [OutputType([pscustomobject])]
    param(
        [Parameter(Mandatory)][string]$SourceAccount,
        [Parameter(Mandatory)][string]$SourceContainer,
        [Parameter(Mandatory)][string]$DestAccount,
        [Parameter(Mandatory)][string]$DestContainer,
        [string]$PlanCsvPath,      # migration-plan.csv (Copy / Skip-Diverged rows); '' = skip
        [string]$CopyListPath,     # azcopy --list-of-files (names only); '' = skip
        [string]$LedgerCsvPath,    # full per-blob status ledger; '' = skip
        [long]$MaxFilesPerRun = 0, # cap on blobs added to the copy list (0 = no cap)
        [string]$ProgressLabel = 'scanned'
    )
    # Enumerate both containers IN PARALLEL with azcopy, then merge-join the listing files in
    # the compiled core. The two listings overlap in time instead of alternating page-by-page.
    Write-Host '       listing source and destination containers in parallel (azcopy list)...'
    $listings = @(
        (Start-ContainerListing -Account $SourceAccount -Container $SourceContainer -Label 'source'),
        (Start-ContainerListing -Account $DestAccount   -Container $DestContainer   -Label 'destination')
    )
    try {
        Wait-ContainerListing -Listing $listings
        Write-Host '       merge-joining the listings...'
        $r = [StorageMigration.ContainerMergeJoin]::Run(
            $listings[0].OutFile, $listings[1].OutFile,
            $PlanCsvPath, $CopyListPath, $LedgerCsvPath, $MaxFilesPerRun, $ProgressLabel)
    }
    finally {
        # Reap the listing processes and scratch files even on failure; a hard kill that skips
        # this is covered by Clear-StaleScratch on the next run.
        foreach ($l in $listings) {
            if (-not $l.Process.HasExited) { try { $l.Process.Kill() } catch { } }
        }
        Remove-Item -LiteralPath (@($listings.OutFile) + @($listings.ErrFile)) -Force -ErrorAction SilentlyContinue -WhatIf:$false
    }

    [pscustomobject]@{
        SourceCount       = $r.SourceCount
        CopyCount         = $r.CopyCount
        DeferredCount     = $r.DeferredCount
        CopyBytes         = $r.CopyBytes
        InSyncCount       = $r.InSyncCount
        SizeMismatchCount = $r.SizeMismatchCount
        Md5MismatchCount  = $r.Md5MismatchCount
        DivergedCount     = ($r.SizeMismatchCount + $r.Md5MismatchCount)
        DestOnlyCount     = $r.DestOnlyCount
        DivergedSample    = $r.DivergedSample
    }
}

# Builds the migration plan (and optional pre-migration ledger) by merge-joining the two
# containers. Returns counters plus the paths it wrote.
function Build-MigrationPlan {
    [OutputType([pscustomobject])]
    param(
        [Parameter(Mandatory)][string]$SourceAccount,
        [Parameter(Mandatory)][string]$SourceContainer,
        [Parameter(Mandatory)][string]$DestAccount,
        [Parameter(Mandatory)][string]$DestContainer,
        [Parameter(Mandatory)][string]$PlanDirectory,
        # When set, also write the full per-blob ledger (every blob + DestOnly rows) here.
        [string]$StatusCsvPath,
        # Cap on blobs copied this run (0 = no cap). The rest become DeferredCount.
        [long]$MaxFilesPerRun = 0
    )
    $planCsvPath  = Join-Path $PlanDirectory 'migration-plan.csv'
    $copyListPath = Join-Path $PlanDirectory 'copy-list.txt'        # names only, for azcopy --list-of-files

    $r = Invoke-ContainerMergeJoin -SourceAccount $SourceAccount -SourceContainer $SourceContainer `
                                   -DestAccount $DestAccount -DestContainer $DestContainer `
                                   -PlanCsvPath $planCsvPath -CopyListPath $copyListPath -LedgerCsvPath $StatusCsvPath `
                                   -MaxFilesPerRun $MaxFilesPerRun -ProgressLabel 'scanned'

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
        [Parameter(Mandatory)][string]$SourceAccount,
        [Parameter(Mandatory)][string]$SourceContainer,
        [Parameter(Mandatory)][string]$DestAccount,
        [Parameter(Mandatory)][string]$DestContainer,
        [string]$StatusCsvPath
    )
    return Invoke-ContainerMergeJoin -SourceAccount $SourceAccount -SourceContainer $SourceContainer `
                                     -DestAccount $DestAccount -DestContainer $DestContainer `
                                     -LedgerCsvPath $StatusCsvPath -ProgressLabel 'verified'
}

# Runs a single azcopy job over the whole copy list. azcopy streams the list file and keeps
# its job plan on disk, so memory stays bounded no matter how many blobs are listed.
function Invoke-AzCopy {
    param(
        [Parameter(Mandatory)][ValidateNotNullOrEmpty()][string]$SourceAccount,
        [Parameter(Mandatory)][ValidateNotNullOrEmpty()][string]$SourceContainer,
        [Parameter(Mandatory)][ValidateNotNullOrEmpty()][string]$DestAccount,
        [Parameter(Mandatory)][ValidateNotNullOrEmpty()][string]$DestContainer,
        # Names-only copy list for an incremental run. Omit to copy the WHOLE container
        # recursively (the empty-destination fast path -- azcopy lists and copies in one pass).
        [string]$ListFile
    )
    $sourceUrl = "https://$SourceAccount.blob.core.windows.net/$SourceContainer"
    $destUrl   = "https://$DestAccount.blob.core.windows.net/$DestContainer"

    $azCopyArgs = @('copy', $sourceUrl, $destUrl, '--s2s-preserve-blob-tags')
    if ($ListFile) { $azCopyArgs += @('--list-of-files', $ListFile) }
    else           { $azCopyArgs += '--recursive' }

    $state = @{ LastProgress = [datetime]::UtcNow }
    & azcopy @azCopyArgs 2>&1 | ForEach-Object {
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
# Open more parallel transfers than azcopy's core-count default (~32 on a 2-vCPU agent).
# The copy is Azure-to-Azure (S2S), so the account bandwidth, not the agent, is the real
# limit -- raising this uses bandwidth that the cautious default leaves idle.
$env:AZCOPY_CONCURRENCY_VALUE = 'AUTO'

if (-not (Get-Command azcopy -ErrorAction SilentlyContinue)) {
    throw 'azcopy executable not found in PATH. Install it on the agent (e.g. via the AzureCLI@2 task or a dedicated AzCopy install step) before running this script.'
}

Clear-StaleScratch

try {

Write-Log 'MIGRATION CONFIGURATION -- source and destination details for this run'
Write-Host ('Start time:        {0:yyyy-MM-dd HH:mm:ss} UTC' -f $migrationStart)
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

# ── FAST PATH ── empty destination: copy the whole container, skip planning/validation ──
# For the very first run into an EMPTY destination, every blob is going to be copied anyway,
# so the full source merge-join is pure dead time. azcopy lists and copies the whole container
# in one streamed pass instead. Re-run WITHOUT -CopyEntireContainer afterwards to reconcile
# anything missed and write the validation ledger.
if ($CopyEntireContainer) {
    Write-Log 'COPY ENTIRE CONTAINER -- empty-destination fast path (no merge-join, no validation)'
    Write-Host 'Skipping plan, divergence and validation. Re-run in normal mode afterwards to catch anything missed and validate.'

    if ($PSCmdlet.ShouldProcess("entire '$SourceContainer' container", "Copy to $DestStorageAccount/$DestContainer")) {
        Write-Log ('MIGRATING -- copying the entire {0} container to {1} (recursive)' -f $SourceContainer, $DestContainer)
        Invoke-AzCopy -SourceAccount $SourceStorageAccount -SourceContainer $SourceContainer `
                      -DestAccount $DestStorageAccount -DestContainer $DestContainer
        Write-Log 'SUMMARY -- entire-container copy finished'
        Write-Host (Colorize 'azcopy reported success for every transfer (see the azcopy log for the per-file breakdown).' $script:Ansi.Green)
        Write-Host 'Next: re-run this script WITHOUT -CopyEntireContainer to reconcile anything missed and write the validation ledger.'
    }
    else {
        Write-Log 'DRY RUN (-WhatIf) -- no data copied. Remove -WhatIf (or approve) to copy the whole container.'
    }
    return
}

# ── PLAN ── merge-join the two containers and write the plan to disk ─────────────
$step++; Write-Host "[$step/$totalSteps] Building migration plan (parallel azcopy listings + merge-join)..."
$preStatusCsv = if ($BlobStatusReport) { Join-Path $planDir 'pre-blob-status.csv' } else { '' }
$plan = Build-MigrationPlan -SourceAccount $SourceStorageAccount -SourceContainer $SourceContainer `
                            -DestAccount $DestStorageAccount -DestContainer $DestContainer `
                            -PlanDirectory $planDir `
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
        Invoke-AzCopy -SourceAccount $SourceStorageAccount -SourceContainer $SourceContainer `
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
    $post = Get-PostMigrationStatus -SourceAccount $SourceStorageAccount -SourceContainer $SourceContainer `
                                    -DestAccount $DestStorageAccount -DestContainer $DestContainer `
                                    -StatusCsvPath $postStatusCsv
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
