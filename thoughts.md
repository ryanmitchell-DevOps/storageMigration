##[section]PREPARATION CHECKS -- verifying storage accounts and containers before migration
2026-06-15T10:50:00.2587709Z [1/3] Verifying source storage account and container exist...
2026-06-15T10:50:02.3755163Z Storage account 'dncstorage33cdprd' found in subscription '3cd7b099-993c-4d72-bc13-1408376a88a6'.
2026-06-15T10:50:02.6594485Z Container 'dnc-directmethod-lib-prd' found in 'dncstorage33cdprd'.
2026-06-15T10:50:02.6633240Z [2/3] Verifying destination storage account and container exist...
2026-06-15T10:50:03.5771457Z Storage account 'stdncstorage3ok0r' found in subscription '1dc32f4d-fc9d-4077-a568-777d2ccfcdb2'.
2026-06-15T10:50:04.1614164Z Container 'data-migration-trial-prd' found in 'stdncstorage3ok0r'.
2026-06-15T10:50:04.1633912Z [3/3] Building migration plan (streaming, 20 blob(s) per page)...
2026-06-15T10:50:04.3545476Z What if: Performing the operation "Remove File" on target "/home/devopsagent/_work/109/a/migration-plan/copy-list.txt".
2026-06-15T10:50:04.3548978Z What if: Performing the operation "Remove File" on target "/home/devopsagent/_work/109/a/migration-plan/copy-manifest.tsv".
2026-06-15T10:50:04.3632935Z 
2026-06-15T10:50:04.3635556Z ##[section]MIGRATION & VALIDATION TIME -- total elapsed duration for this run
2026-06-15T10:50:04.3636342Z Migration started:  2026-06-15 10:50:00 UTC
2026-06-15T10:50:04.3644606Z Migration ended:    2026-06-15 10:50:04 UTC
2026-06-15T10:50:04.3648926Z Total time elapsed: 00:00:04
2026-06-15T10:50:04.5624115Z Error formatting a string: Index (zero based) must be greater than or equal to zero and less than the size of the argument list..
2026-06-15T10:50:04.5655434Z At /home/devopsagent/_work/109/s/migrations/storage/storage-migration.ps1:542 char:9
2026-06-15T10:50:04.5656199Z + $plan = Build-MigrationPlan -SourceContext $sourceCtx -SourceContaine …
2026-06-15T10:50:04.5656513Z +         ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
2026-06-15T10:50:04.6483205Z 
2026-06-15T10:50:04.6676777Z ##[error]PowerShell exited with code '1'.
2026-06-15T10:50:04.6685493Z [command]/home/devopsagent/.local/powershell/pwsh -NoLogo -NoProfile -NonInteractive -ExecutionPolicy Unrestricted -Command . '/home/devopsagent/_work/_tasks/AzurePowerShell_72a1931b-effb-4d2e-8fd8-f8472a07cb62/5.274.5/RemoveAzContext.ps1'
2026-06-15T10:50:05.8832978Z ##[command]Disconnect-AzAccount -Scope CurrentUser -ErrorAction Stop
2026-06-15T10:50:05.9656280Z ##[command]Disconnect-AzAccount -Scope Process -ErrorAction Stop
2026-06-15T10:50:05.9702142Z ##[command]Clear-AzContext -Scope Process -ErrorAction Stop
2026-06-15T10:50:06.7292877Z 
2026-06-15T10:50:06.7410086Z ##[section]Finishing: Preview Storage Migration (-WhatIf)
