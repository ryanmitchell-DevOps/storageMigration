# storageMigration

Azure DevOps pipeline + PowerShell script for migrating blobs between Azure
Storage containers. Uses `azcopy sync` for the transfer and `Az.Storage` for
inventory, validation, and reporting.

## Known limitations

- **Per-file AzCopy error detail.** `azcopy sync` does not emit structured
  per-blob error reasons (HTTP status, server message) to stdout. When a
  transfer fails, the script lists the affected blob names in the
  `FAILED FILES` section, but the per-blob error reason is only available in
  the AzCopy log file. The log directory is published as a pipeline artifact
  (`azcopy-logs`) by `pipeline.yml`.

## Function inventory and rationale

Every function in `scripts/storage-migration.ps1` exists for a specific reason.
This section exists so the design can be defended in review.

| Function | Call sites | Why it exists |
|---|---|---|
| `Write-Log` | ~10 | Enforces a single section-header format (`##[section]<msg>` preceded by a blank line). Without it, section markers drift in spacing and prefix, breaking Azure DevOps log folding. One-liner, but the discipline it enforces is the point. |
| `Initialize-LogDirectory` | 1 | Resolves the log directory from three possible sources (explicit param, `$BUILD_ARTIFACTSTAGINGDIRECTORY`, OS temp) and creates it if missing. Single caller, but the branching logic is non-trivial enough that inlining would add noise to the main flow. The name now reflects what it does (creates a directory) rather than what it doesn't (write to it). |
| `Format-FileSize` | many | Converts bytes to human-readable units (B / KB / MB / GB). Used in every blob list and summary output. Trivial to call, tedious to inline. |
| `Format-Cell` | many | Truncates blob names with an ellipsis when they exceed the column width. Without it, long blob names wrap and destroy column alignment in the tables. |
| `Assert-StorageAccountExists` | 2 | Required by **requirement 1** ("check source and destination storage accounts exist"). Uses a targeted ARM lookup (`Get-AzResource`) to avoid enumerating every storage account in the subscription. Called once for source, once for destination. |
| `Assert-ContainerExists` | 2 | Required by **requirement 1** ("check source and destination containers exist"). Called once for source, once for destination. |
| `Get-BlobInventory` | 3 | Builds a case-sensitive in-memory map of `{name → {Length, MD5, BlobType}}` for a container. Uses `Dictionary[string,object]` with `StringComparer.Ordinal` because PowerShell's default `@{}` hashtable is case-insensitive and would silently collapse `File.txt` and `file.txt`. Called for source inventory, pre-migration destination, and post-migration destination. |
| `Show-BlobList` | 6 | Renders a sorted, truncated list of blob names with type and size columns. Used by every section that lists blobs (missing, out-of-sync, already-in-sync, transferred, failed, dest-only). Centralises the column-width and "... and N more" truncation logic. |
| `Get-BlobClassification` | 2 | **Single source of truth** for how source and destination blobs are bucketed (missing / size-mismatch / md5-mismatch / no-md5 / matched / dest-only). Consumed by `Compare-Migration` (console reporting) and `Test-MigrationCompleteness` (validation) so the rules can't drift between the two consumers. Replaced two parallel classification loops that previously existed. |
| `Compare-Migration` | 2 | Required by **requirements 2, 3, 4** (source summary, files already in destination, files still to transfer). Renders the PRE-MIGRATION and POST-MIGRATION status blocks. Returns a status object whose `PendingNames` drives the migration decision. Same function called twice with different `-Label` values keeps the two snapshots visually consistent. |
| `Invoke-AzCopySync` | 1 | Wraps the `azcopy sync` invocation, captures combined stdout+stderr, and surfaces it on failure. Without this wrapper, AzCopy's actual failure reason is invisible in the pipeline log -- you'd see only "exit code N". Single caller, but isolating the AzCopy boundary makes the failure-handling logic testable and the main flow easier to read. |
| `Test-MigrationCompleteness` | 1 | Required by **requirement 7** ("validation -- comparison between source and destination including file name, count, size, checksum"). Returns a structured validation result (`Passed`, `Issues`, plus per-bucket name lists) that drives both the VALIDATION section render and the `$failed` calculation in the post-migration block. Single caller, but the validation rules are the contract for migration success -- worth a named function. |
| `Show-BlobComparison` | 2 | Renders the side-by-side source-vs-destination table with size and MD5 OK/MISMATCH/N/A indicators per row. Required by **requirement 7** ("log a comparison ... including file name, count, & size for comparison, checksum"). Called from both the PASS and FAIL branches of validation so the table always appears regardless of outcome. |

### Functions deliberately removed during review

- **`Write-LogDirectory`** -- misleading name (it didn't write anything). Renamed to `Initialize-LogDirectory`.
- **`Get-BlobMD5`** -- 6-line helper used once. Inlined into `Get-BlobInventory`. The historical dual-API fallback (newer `BlobProperties.ContentHash` vs older `ICloudBlob.Properties.ContentMD5`) was dropped; the script targets current `Az.Storage`.
- **`Set-AzCopyEnvironment`** -- 3-line env-var setter used once. Inlined at the call site.
