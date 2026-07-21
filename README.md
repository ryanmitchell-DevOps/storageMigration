[ERROR] Failed to execute goal com.diffplug.spotless:spotless-maven-plugin:2.44.3:check (default) on project app: The following files had format violations:
[ERROR]     src/main/java/com/gea/ft/service/dnbconnector/config/BackupBlobServiceClient.java
[ERROR]         @@ -1,6 +1,6 @@
[ERROR]          package·com.gea.ft.service.dnbconnector.config;
[ERROR]          
[ERROR]         -import·com.azure.core.credential.AzureNamedKeyCredential;
[ERROR]         +import·com.azure.identity.DefaultAzureCredentialBuilder;
[ERROR]          import·com.azure.storage.blob.BlobContainerClient;
[ERROR]          import·com.azure.storage.blob.BlobServiceClient;
[ERROR]          import·com.azure.storage.blob.BlobServiceClientBuilder;
[ERROR]         @@ -10,7 +10,6 @@
[ERROR]          import·org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
[ERROR]          import·org.springframework.context.annotation.Bean;
[ERROR]          import·org.springframework.context.annotation.Configuration;
[ERROR]         -import·com.azure.identity.DefaultAzureCredentialBuilder;
[ERROR]          
[ERROR]          @Configuration
[ERROR]          @ConditionalOnProperty(name·=·"backup.storage.use-edge-storage",·havingValue·=·"true")
[ERROR] Run 'mvn spotless:apply' to fix these violations.
[ERROR] -> [Help 1]
[ERROR] 
[ERROR] To see the full stack trace of the errors, re-run Maven with the -e switch.
[ERROR] Re-run Maven using the -X switch to enable full debug logging.
[ERROR] 
[ERROR] For more information about the errors and possible solutions, please read the following articles:
[ERROR] [Help 1] http://cwiki.apache.org/confluence/display/MAVEN/MojoExecutionException
[ERROR] 
[ERROR] After correcting the problems, you can resume the build with the command
[ERROR]   mvn <args> -rf :app
