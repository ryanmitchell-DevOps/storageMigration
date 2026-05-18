# storageMigration

ok can you please re-review again with the below in mind but also: 

Review this script as a senior engineer, please be:

hyper critical - I mean in the sense that someome reviewing will be looking for errors etc
is it good? structure wise etc
Could it be simplified? is it overkill?
does it meet the requirements below:
Script will include:

Script checks to ensure that the source and destination storage accounts and containers exist.

Output a summary of the source container including total number of files & file names in log.

Identify and log which files are already present in the destination container (useful when the script is re-run).

Output the number of files that are not yet present in the destination container. Providing number of files still to transfer.

Data transfer is idempotent (so it does not duplicate data). Only transfers new or changed files.

Post migration after execution – log all successfully transferred files. Log failed transfers with errors. Provide a summary of total files migrated & total failures.

Validation – perform & log a comparison between source & destination including file name, count, & size for comparison, checksum to verify data integrity. (manual validation check should also be performed as a final verification step).

Script will output the time the migration & validation takes.

but also are all functions necessary etc?
