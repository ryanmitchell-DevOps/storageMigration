2026/06/25 09:47:30 Job-Command copy https://dncstorage33cdprd.blob.core.windows.net/dnc-backups-prd https://stdncstorage3ok0r.blob.core.windows.net/data-migration-trial-prd --s2s-preserve-blob-tags --recursive 
2026/06/25 09:47:30 Number of CPUs: 2
2026/06/25 09:47:30 Max file buffer RAM 1.000 GB
2026/06/25 09:47:30 Max concurrent network operations: 256 (Based on AZCOPY_CONCURRENCY_VALUE environment variable)
2026/06/25 09:47:30 Check CPU usage when dynamically tuning concurrency: true (Based on hard-coded default. Set AZCOPY_TUNE_TO_CPU environment variable to true or false override)
2026/06/25 09:47:30 Max concurrent transfer initiation routines: 64 (Based on hard-coded default. Set AZCOPY_CONCURRENT_FILES environment variable to override)
2026/06/25 09:47:30 Max enumeration routines: 16 (Based on hard-coded default. Set AZCOPY_CONCURRENT_SCAN environment variable to override)
2026/06/25 09:47:30 Parallelize getting file properties (file.Stat): false (Based on AZCOPY_PARALLEL_STAT_FILES environment variable)
2026/06/25 09:47:30 Max open files when downloading: 523618 (auto-computed)
2026/06/25 09:47:30 INFO: [P#0-T#0] Starting transfer: Source "https://dncstorage33cdprd.blob.core.windows.net/dnc-backups-prd/25005862/dairynet-servicebackups-46_0_10-20260622-122805.zip" Destination "https://stdncstorage3ok0r.blob.core.windows.net/data-migration-trial-prd/25005862/dairynet-servicebackups-46_0_10-20260622-122805.zip". Specified chunk size 8388608
2026/06/25 09:47:30 INFO: [P#0-T#1] Starting transfer: Source "https://dncstorage33cdprd.blob.core.windows.net/dnc-backups-prd/a1f2ff8f-0362-4117-947b-82cfce2cb471/20260607_042403_dairybox-supervision-47_0_7-20260607-042403.zip" Destination "https://stdncstorage3ok0r.blob.core.windows.net/data-migration-trial-prd/a1f2ff8f-0362-4117-947b-82cfce2cb471/20260607_042403_dairybox-supervision-47_0_7-20260607-042403.zip". Specified chunk size 8388608
2026/06/25 09:47:30 INFO: [P#0-T#2] Starting transfer: Source "https://dncstorage33cdprd.blob.core.windows.net/dnc-backups-prd/16db1b3e-83ca-4336-a192-7cebb5ce5537/20260616_103418_dairybox-database-47_0_4-20260616-103308-csv.zip" Destination "https://stdncstorage3ok0r.blob.core.windows.net/data-migration-trial-prd/16db1b3e-83ca-4336-a192-7cebb5ce5537/20260616_103418_dairybox-database-47_0_4-20260616-103308-csv.zip". Specified chunk size 8388608
2026/06/25 09:47:30 INFO: [P#0-T#3] Starting transfer: Source "https://dncstorage33cdprd.blob.core.windows.net/dnc-backups-prd/844d9b86-5666-4f0a-90a1-67d7df07bdf7/dairybox-database-47_0_7-20260623-200803-csv.zip" Destination "https://stdncstorage3ok0r.blob.core.windows.net/data-migration-trial-prd/844d9b86-5666-4f0a-90a1-67d7df07bdf7/dairybox-database-47_0_7-20260623-200803-csv.zip". Specified chunk size 8388608
2026/06/25 09:47:30 INFO: [P#0-T#4] Starting transfer: Source "https://dncstorage33cdprd.blob.core.windows.net/dnc-backups-prd/3a0bf10f-380a-4e97-afc9-49453329db1a/dairybox-database-46_0_16-20260623-155506-csv.zip" Destination "https://stdncstorage3ok0r.blob.core.windows.net/data-migration-trial-prd/3a0bf10f-380a-4e97-afc9-49453329db1a/dairybox-database-46_0_16-20260623-155506-csv.zip". Specified chunk size 8388608



Job e4b108a0-84a1-fa44-541e-ad20f394aac9 has started
Log file is located at: /home/devopsagent/_work/109/a/azcopy-logs/e4b108a0-84a1-fa44-541e-ad20f394aac9.log

INFO: Trying 4 concurrent connections (initial starting point)

INFO: Trying 16 concurrent connections (seeking optimum)                                           
INFO: Trying 64 concurrent connections (seeking optimum)         
INFO: Trying 256 concurrent connections (seeking optimum)                                         
0.8 %, 99 Done, 0 Failed, 15299 Pending, 0 Skipped, 15398 Total, 2-sec Throughput (Mb/s): 9497.7501
INFO: Trying 64 concurrent connections (backing off)                                                
INFO: Trying 128 concurrent connections (seeking optimum)                                           
INFO: Trying 64 concurrent connections (backing off)                                                
2.1 %, 293 Done, 0 Failed, 15105 Pending, 0 Skipped, 15398 Total, 2-sec Throughput (Mb/s): 4149.7775
INFO: Trying 76 concurrent connections (seeking optimum)                                            
INFO: Trying 64 concurrent connections (at optimum)                                                 
INFO:                                                                                               
INFO: Automatic concurrency tuning completed.                                                       
INFO:                                                                                               
3.5 %, 531 Done, 0 Failed, 14867 Pending, 0 Skipped, 15398 Total, 2-sec Throughput (Mb/s): 3827.5803
4.8 %, 692 Done, 0 Failed, 14706 Pending, 0 Skipped, 15398 Total, 2-sec Throughput (Mb/s): 3220.3281
