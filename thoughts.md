2026-05-20T12:29:38.7961411Z ##[section]MIGRATION CONFIGURATION -- source and destination details for this run
2026-05-20T12:29:38.7962273Z Start time:        2026-05-20 12:29:38 UTC
2026-05-20T12:29:38.7962714Z Log directory:     /home/devopsagent/_work/97/a/azcopy-logs
2026-05-20T12:29:38.7962874Z 
2026-05-20T12:29:38.7963032Z Source
2026-05-20T12:29:38.7969532Z   Subscription:    5da85cc1-3781-4b0a-91ed-88eada0a607e
2026-05-20T12:29:38.7970194Z   Storage account: dncstorage12acdev
2026-05-20T12:29:38.7971143Z   Container:       dnc-billing-dev
2026-05-20T12:29:38.7976751Z   URL:             https://dncstorage12acdev.blob.core.windows.net/dnc-billing-dev
2026-05-20T12:29:38.7981457Z 
2026-05-20T12:29:38.7996163Z Destination
2026-05-20T12:29:38.7999107Z   Subscription:    f126b605-e800-4bf0-b24c-84f03684f2c5
2026-05-20T12:29:38.8003355Z   Storage account: stdncstoragecvfmy
2026-05-20T12:29:38.8006463Z   Container:       data-migration-trial-dev
2026-05-20T12:29:38.8010362Z   URL:             https://stdncstoragecvfmy.blob.core.windows.net/data-migration-trial-dev
2026-05-20T12:29:38.8015783Z 
2026-05-20T12:29:38.8019740Z ##[section]PREPARATION CHECKS -- verifying storage accounts, containers, and inventorying blobs before migration
2026-05-20T12:29:38.8024396Z [1/3] Verifying source storage account and container exist...
2026-05-20T12:29:41.1155482Z Storage account 'dncstorage12acdev' found in subscription '5da85cc1-3781-4b0a-91ed-88eada0a607e'.
2026-05-20T12:29:41.5368493Z Container 'dnc-billing-dev' found in 'dncstorage12acdev'.
2026-05-20T12:29:41.5377790Z [2/3] Verifying destination storage account and container exist...
2026-05-20T12:29:42.3818103Z Storage account 'stdncstoragecvfmy' found in subscription 'f126b605-e800-4bf0-b24c-84f03684f2c5'.
2026-05-20T12:29:42.4356866Z Container 'data-migration-trial-dev' found in 'stdncstoragecvfmy'.
2026-05-20T12:29:42.4371591Z [3/3] Inventorying source and destination containers...
2026-05-20T12:29:43.3346712Z        Source inventoried:      7 blob(s)
2026-05-20T12:29:43.3350274Z        Destination inventoried: 0 blob(s)
2026-05-20T12:29:43.3866832Z 
2026-05-20T12:29:43.3869548Z ##[section]PRE-MIGRATION STATUS -- snapshot of source vs destination before any copy operations
2026-05-20T12:29:43.3870867Z Blobs in 'dnc-billing-dev': 7
2026-05-20T12:29:43.3875581Z Blobs in 'data-migration-trial-dev': 0
2026-05-20T12:29:43.3884644Z Already migrated:  0 (0%)
2026-05-20T12:29:43.3887057Z Pending:           7 (100%)  [missing: 7, size-mismatch: 0, md5-mismatch: 0]
2026-05-20T12:29:43.3976172Z 
2026-05-20T12:29:43.3986993Z   [91mNot in data-migration-trial-dev:[0m
2026-05-20T12:29:43.4107278Z     billing1803148753361935425.csv                     BlockBlob       288 B
2026-05-20T12:29:43.4114042Z     error16773026657017950879.csv                      BlockBlob       307 B
2026-05-20T12:29:43.4120494Z     error3490327123649062262.csv                       BlockBlob       403 B
2026-05-20T12:29:43.4126913Z     error6323316504446404084.csv                       BlockBlob       352 B
2026-05-20T12:29:43.4133369Z     error7875216139428465992.csv                       BlockBlob       307 B
2026-05-20T12:29:43.4139461Z     error939087354504292420.csv                        BlockBlob       307 B
2026-05-20T12:29:43.4145858Z     file should stay.txt                               BlockBlob         0 B
2026-05-20T12:29:43.4237269Z 
2026-05-20T12:29:43.4241136Z ##[section]MIGRATING -- copying 7 pending blob(s) from dnc-billing-dev to data-migration-trial-dev
2026-05-20T12:29:43.4262239Z   billing1803148753361935425.csv                     BlockBlob       288 B
2026-05-20T12:29:43.4267757Z   error16773026657017950879.csv                      BlockBlob       307 B
2026-05-20T12:29:43.4274356Z   error3490327123649062262.csv                       BlockBlob       403 B
2026-05-20T12:29:43.4281645Z   error6323316504446404084.csv                       BlockBlob       352 B
2026-05-20T12:29:43.4309164Z   error7875216139428465992.csv                       BlockBlob       307 B
2026-05-20T12:29:43.4312903Z   error939087354504292420.csv                        BlockBlob       307 B
2026-05-20T12:29:43.4313152Z   file should stay.txt                               BlockBlob         0 B
2026-05-20T12:29:43.4313242Z 
2026-05-20T12:29:43.4329036Z   Total: 7 file(s), 1.92 KB
2026-05-20T12:29:43.4545832Z [33;1mWARNING: The variable '$LASTEXITCODE' cannot be retrieved because it has not been set.[0m
2026-05-20T12:29:43.4836512Z 
2026-05-20T12:29:43.4843876Z ##[section]FAILED FILES
2026-05-20T12:29:43.4846341Z   [91m7 blob(s) were copied but did not reach the destination intact.[0m
2026-05-20T12:29:43.4849127Z   See AzCopy logs in /home/devopsagent/_work/97/a/azcopy-logs
2026-05-20T12:29:43.4851332Z 
2026-05-20T12:29:43.4865144Z   [91mbilling1803148753361935425.csv                    [0m BlockBlob       288 B
2026-05-20T12:29:43.4870788Z   [91merror16773026657017950879.csv                     [0m BlockBlob       307 B
2026-05-20T12:29:43.4877076Z   [91merror3490327123649062262.csv                      [0m BlockBlob       403 B
2026-05-20T12:29:43.4885255Z   [91merror6323316504446404084.csv                      [0m BlockBlob       352 B
2026-05-20T12:29:43.4890224Z   [91merror7875216139428465992.csv                      [0m BlockBlob       307 B
2026-05-20T12:29:43.4896445Z   [91merror939087354504292420.csv                       [0m BlockBlob       307 B
2026-05-20T12:29:43.4903163Z   [91mfile should stay.txt                              [0m BlockBlob         0 B
2026-05-20T12:29:43.4962179Z 
2026-05-20T12:29:43.4972349Z ##[section]POST-MIGRATION STATUS -- destination state after copy operations complete
2026-05-20T12:29:43.4978326Z Blobs in 'dnc-billing-dev': 7
2026-05-20T12:29:43.4978810Z Blobs in 'data-migration-trial-dev': 0
2026-05-20T12:29:43.4979204Z Already migrated:  0 (0%)
2026-05-20T12:29:43.4979663Z Pending:           7 (100%)  [missing: 7, size-mismatch: 0, md5-mismatch: 0]
2026-05-20T12:29:43.4981093Z 
2026-05-20T12:29:43.4981787Z   [91mNot in data-migration-trial-dev:[0m
2026-05-20T12:29:43.5002252Z     billing1803148753361935425.csv                     BlockBlob       288 B
2026-05-20T12:29:43.5002850Z     error16773026657017950879.csv                      BlockBlob       307 B
2026-05-20T12:29:43.5008057Z     error3490327123649062262.csv                       BlockBlob       403 B
2026-05-20T12:29:43.5013649Z     error6323316504446404084.csv                       BlockBlob       352 B
2026-05-20T12:29:43.5053418Z     error7875216139428465992.csv                       BlockBlob       307 B
2026-05-20T12:29:43.5054172Z     error939087354504292420.csv                        BlockBlob       307 B
2026-05-20T12:29:43.5054533Z     file should stay.txt                               BlockBlob         0 B
2026-05-20T12:29:43.5054732Z 
2026-05-20T12:29:43.5055104Z ##[section]VALIDATION -- comparing 'dnc-billing-dev' to 'data-migration-trial-dev' by name, size, and MD5 checksum
2026-05-20T12:29:43.5055938Z Comparing 7 blob(s) in 'dnc-billing-dev' against 0 blob(s) in 'data-migration-trial-dev'...
2026-05-20T12:29:43.5059958Z 
2026-05-20T12:29:43.5081903Z 
2026-05-20T12:29:43.5282546Z   Blob Name                                          Type            dnc-billing-dev data-migration-tria…  Size      MD5     
2026-05-20T12:29:43.5287072Z   -------------------------------------------------- ---------- -------------------- --------------------  --------  --------
2026-05-20T12:29:43.5373320Z   billing1803148753361935425.csv                     BlockBlob                 288 B              MISSING  [91mMISSING [0m  [91m-       [0m
2026-05-20T12:29:43.5381147Z   error16773026657017950879.csv                      BlockBlob                 307 B              MISSING  [91mMISSING [0m  [91m-       [0m
2026-05-20T12:29:43.5392597Z   error3490327123649062262.csv                       BlockBlob                 403 B              MISSING  [91mMISSING [0m  [91m-       [0m
2026-05-20T12:29:43.5393501Z   error6323316504446404084.csv                       BlockBlob                 352 B              MISSING  [91mMISSING [0m  [91m-       [0m
2026-05-20T12:29:43.5398752Z   error7875216139428465992.csv                       BlockBlob                 307 B              MISSING  [91mMISSING [0m  [91m-       [0m
2026-05-20T12:29:43.5404570Z   error939087354504292420.csv                        BlockBlob                 307 B              MISSING  [91mMISSING [0m  [91m-       [0m
2026-05-20T12:29:43.5423606Z   file should stay.txt                               BlockBlob                   0 B              MISSING  [91mMISSING [0m  [91m-       [0m
2026-05-20T12:29:43.5424190Z   -------------------------------------------------- ---------- -------------------- --------------------  --------  --------
2026-05-20T12:29:43.5425681Z   TOTAL                                                                      1.92 KB                  0 B
2026-05-20T12:29:43.5426811Z 
2026-05-20T12:29:43.5427208Z [91mFAIL[0m -- validation issues found:
2026-05-20T12:29:43.5436685Z   [91mMissing from destination: 7 blob(s) -- present in source but never reached destination[0m
2026-05-20T12:29:43.5439923Z   [91m - billing1803148753361935425.csv[0m
2026-05-20T12:29:43.5443299Z   [91m - error16773026657017950879.csv[0m
2026-05-20T12:29:43.5449893Z   [91m - error3490327123649062262.csv[0m
2026-05-20T12:29:43.5451409Z   [91m - error6323316504446404084.csv[0m
2026-05-20T12:29:43.5453461Z   [91m - error7875216139428465992.csv[0m
2026-05-20T12:29:43.5461810Z   [91m - error939087354504292420.csv[0m
2026-05-20T12:29:43.5462752Z   [91m - file should stay.txt[0m
2026-05-20T12:29:43.5468206Z 
2026-05-20T12:29:43.5469124Z ##[section]SUMMARY -- migration result
2026-05-20T12:29:43.5469612Z Source blobs:        7
2026-05-20T12:29:43.5483556Z Already in sync:     0
2026-05-20T12:29:43.5484196Z Pending:             7
2026-05-20T12:29:43.5484904Z   Migrated:          0
2026-05-20T12:29:43.5486406Z   Failed:            7
2026-05-20T12:29:43.5486954Z   Skipped (diverged): 0
2026-05-20T12:29:43.5491975Z Validation:          [91mFAIL[0m
2026-05-20T12:29:43.6844927Z The variable '$LASTEXITCODE' cannot be retrieved because it has not been set.
2026-05-20T12:29:43.6847080Z At /home/devopsagent/_work/97/s/scripts/storage-migration.ps1:314 char:13
2026-05-20T12:29:43.6847427Z +         if ($LASTEXITCODE -ne 0) {
2026-05-20T12:29:43.6847673Z +             ~~~~~~~~~~~~~
2026-05-20T12:29:43.7705956Z 
2026-05-20T12:29:43.7846999Z ##[error]PowerShell exited with code '1'.
2026-05-20T12:29:43.7856291Z [command]/home/devopsagent/.local/powershell/pwsh -NoLogo -NoProfile -NonInteractive -ExecutionPolicy Unrestricted -Command . '/home/devopsagent/_work/_tasks/AzurePowerShell_72a1931b-effb-4d2e-8fd8-f8472a07cb62/5.273.3/RemoveAzContext.ps1'
2026-05-20T12:29:44.9632091Z ##[command]Disconnect-AzAccount -Scope CurrentUser -ErrorAction Stop
2026-05-20T12:29:45.0422171Z ##[command]Disconnect-AzAccount -Scope Process -ErrorAction Stop
2026-05-20T12:29:45.0464161Z ##[command]Clear-AzContext -Scope Process -ErrorAction Stop
2026-05-20T12:29:45.8474268Z 
