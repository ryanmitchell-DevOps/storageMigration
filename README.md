@Configuration
@ConditionalOnProperty(name = "backup.storage.use-edge-storage", havingValue = "true")
@Slf4j
public class BackupBlobServiceClient {

    @Value("${backup.storage.host}")
    private String storageHost;
    @Value("${backup.storage.account-name}")
    private String storageAccountName;
    @Value("${backup.storage.account-key}")
    private String storageAccountKey;
    @Value("${backup.storage.container}")
    private String storageContainer;

    @Bean
    public BlobContainerClient backupBlobContainerClient() {
        BlobServiceClient blobServiceClient = new BlobServiceClientBuilder().serviceVersion(
                BlobServiceVersion.V2020_04_08)
                .credential(new AzureNamedKeyCredential(storageAccountName, storageAccountKey))
                .endpoint(storageHost + "/" + storageAccountName)
                .buildClient();
        BlobContainerClient blobContainerClient = blobServiceClient.getBlobContainerClient(storageContainer);
        blobContainerClient.createIfNotExists();
        log.info("Container client initialized. Storage account name: {}, container: {}.",
                storageAccountName,
                storageContainer);
        return blobContainerClient;
    }

}

package com.gea.ft.service.dnbconnector.service;

import com.gea.ft.service.dnbconnector.dto.BackupUploadDto;
import com.gea.ft.service.dnbconnector.dto.MessageDto;
import com.gea.ft.service.dnbconnector.service.directmethod.DirectMethodCallbackService;
import com.gea.ft.service.dnbconnector.service.mqtt.MqttGatewayService;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.models.BlobProperties;
import com.azure.storage.blob.sas.BlobSasPermission;
import com.azure.storage.blob.sas.BlobServiceSasSignatureValues;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.azure.sdk.iot.device.twin.DirectMethodPayload;
import lombok.SneakyThrows;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.integration.config.EnableIntegration;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testcontainers.shaded.org.awaitility.Awaitility.await;

@SpringBootTest(properties = {"backup.compress-backups=true", "backup.enabled=true", "backup.scan-delay=100", "backup.prefix-with-timestamp=false", "backup.storage.use-edge-storage=false", "mqtt.enabled=false"})
@Testcontainers
@ActiveProfiles("test")
@EnableIntegration
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class DairyNetBoxBackupServiceRequestFlowIT {
    private static final String CONTAINER_NAME = "upload-test";
    private static final String CONNECTION_FORMAT_STRING =
            "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://%s:%d/devstoreaccount1;"; // NOSONAR Azurite default key

    private static String tempPath;

    @MockitoBean
    private MessageService messageService;
    @MockitoBean
    private DairyNetVersionService dairyNetVersionService;
    @MockitoBean
    private MqttGatewayService mqttGatewayService;
    @Autowired
    private ObjectMapper objectMapper;
    @Autowired
    private DirectMethodCallbackService directMethodCallbackService;

    @Container
    private static final GenericContainer<?> azurite = new GenericContainer<>(
            "mcr.microsoft.com/azure-storage/azurite:3.34.0").withExposedPorts(10000);

    @DynamicPropertySource
    static void registerProperties(DynamicPropertyRegistry registry) throws IOException {
        tempPath = Files.createTempDirectory(UUID.randomUUID().toString()).toFile().getAbsolutePath();
        registry.add("backup.directory", () -> tempPath);
    }

    @SneakyThrows
    @Test
    void given_tar_file_in_folder_then_create_request_and_upload_on_response() {
        when(dairyNetVersionService.getDairyNetMajorVersionLessThan44()).thenReturn(true);
        String fileName = UUID.randomUUID() + ".tar";
        Path filePath = Files.writeString(Path.of(tempPath, fileName), "teststr");
        //File needs to be at least a minute old to be considered "ready to process"
        Files.setLastModifiedTime(filePath, FileTime.from(ZonedDateTime.now().minusMinutes(5).toInstant()));

        Request request = verifyRequestMessage(fileName);
        testRequestUpload(request, fileName);
    }

    private @NotNull Request verifyRequestMessage(String fileName) throws JsonProcessingException {
        ArgumentCaptor<MessageDto> messageCaptor = ArgumentCaptor.forClass(MessageDto.class);
        verify(messageService, timeout(10000)).addPendingMessage(messageCaptor.capture());
        assertThat(messageCaptor.getValue().getComponent()).isEqualTo("backup-upload");
        assertThat(messageCaptor.getValue().getJsonString()).isNotBlank();
        Request request = objectMapper.readValue(messageCaptor.getValue().getJsonString(), Request.class);
        assertThat(request.requestId()).isNotNull();
        assertThat(request.fileName()).isEqualTo(fileName + ".gz");
        return request;
    }

    private void testRequestUpload(Request request, String fileName) {
        BlobClient blobClient = createAzuriteBlobClient(fileName);
        String blobUploadUrl = createUploadUrlWithSas(blobClient);
        DirectMethodPayload methodPayload = mock(DirectMethodPayload.class);
        when(methodPayload.getPayload(any())).thenReturn(new BackupUploadDto(request.requestId(), blobUploadUrl));

        directMethodCallbackService.onMethodInvoked("backup-upload", methodPayload, null);

        await().atMost(30, TimeUnit.SECONDS).until(blobClient::exists);
        BlobProperties properties = blobClient.getProperties();
        assertThat(properties.getBlobSize()).isPositive();
        assertThat(properties.getContentType()).isEqualTo("application/gzip");
    }

    private String createUploadUrlWithSas(BlobClient blobClient) {
        String blobUrl = blobClient.getBlobUrl();
        String sasToken = blobClient.generateSas(prepareSignatureValues());
        return "%s?%s".formatted(blobUrl, sasToken);
    }

    private static @NotNull BlobClient createAzuriteBlobClient(String fileName) {
        BlobServiceClient testClient = new BlobServiceClientBuilder().connectionString(CONNECTION_FORMAT_STRING
                .formatted(azurite.getHost(), azurite.getFirstMappedPort())).buildClient();
        BlobContainerClient containerClient = testClient.createBlobContainer(CONTAINER_NAME);
        return containerClient.getBlobClient(fileName);
    }

    private BlobServiceSasSignatureValues prepareSignatureValues() {
        OffsetDateTime now = OffsetDateTime.now();
        OffsetDateTime startTime = now.minusMinutes(15);
        OffsetDateTime linkExpiryTime = now.plusMinutes(15);
        BlobSasPermission permission = new BlobSasPermission().setAddPermission(true).setCreatePermission(true);
        return new BlobServiceSasSignatureValues(linkExpiryTime, permission).setStartTime(startTime);
    }

    record Request(UUID requestId, String fileName) {}
}

package com.gea.ft.service.dnbconnector.service;

import com.gea.ft.service.dnbconnector.dto.BackupUploadDto;
import com.gea.ft.service.dnbconnector.dto.MessageDto;
import com.gea.ft.service.dnbconnector.service.directmethod.DirectMethodCallbackService;
import com.gea.ft.service.dnbconnector.service.mqtt.MqttGatewayService;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.models.BlobProperties;
import com.azure.storage.blob.sas.BlobSasPermission;
import com.azure.storage.blob.sas.BlobServiceSasSignatureValues;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.azure.sdk.iot.device.twin.DirectMethodPayload;
import lombok.SneakyThrows;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockserver.client.MockServerClient;
import org.mockserver.model.MediaType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.integration.config.EnableIntegration;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.MockServerContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;
import static org.mockserver.model.JsonBody.json;
import static org.testcontainers.shaded.org.awaitility.Awaitility.await;

@SpringBootTest(properties = {"backup.compress-backups=false", "backup.enabled=true", "backup.scan-delay=100", "backup.prefix-with-timestamp=false", "backup.storage.use-edge-storage=false", "mqtt.enabled=false"})
@Testcontainers
@ActiveProfiles("test")
@EnableIntegration
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class DairyNetBoxBackupServiceRequestFlowWithDNVersion44IT {
    private static final String CONTAINER_NAME = "upload-test";
    private static final String CONNECTION_FORMAT_STRING =
            "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://%s:%d/devstoreaccount1;"; // NOSONAR Azurite default key

    private static String tempPath;

    @MockitoBean
    private MessageService messageService;
    @MockitoBean
    private MqttGatewayService mqttGatewayService;
    @Autowired
    private ObjectMapper objectMapper;
    @Autowired
    private DirectMethodCallbackService directMethodCallbackService;

    @Container
    private static final GenericContainer<?> azurite = new GenericContainer<>(
            "mcr.microsoft.com/azure-storage/azurite:3.34.0").withExposedPorts(10000);

    @Container
    static MockServerContainer mockServerContainer = new MockServerContainer(DockerImageName.parse(
            "mockserver/mockserver:5.15.0"));

    static MockServerClient mockServerClient;

    @DynamicPropertySource
    static void registerProperties(DynamicPropertyRegistry registry) throws IOException {
        tempPath = Files.createTempDirectory(UUID.randomUUID().toString()).toFile().getAbsolutePath();
        registry.add("backup.directory", () -> tempPath);
        mockServerClient = new MockServerClient(mockServerContainer.getHost(), mockServerContainer.getServerPort());
        registry.add("dairynet.host", mockServerContainer::getEndpoint);
    }

    @SneakyThrows
    @Test
    void given_zip_file_in_folder_then_create_request_and_upload_on_response_with() {
        mockServerClient.when(request().withMethod("GET").withPath("/dairynetbackend/version"))
                .respond(response().withStatusCode(200).withContentType(MediaType.APPLICATION_JSON).withBody(json("""
                        {
                            "dairynetbox":"44.0.0-SNAPSHOT"
                        }
                        """)));

        String fileName = UUID.randomUUID() + ".zip";
        Path filePath = Files.writeString(Path.of(tempPath, fileName), "teststr");
        //File needs to be at least a minute old to be considered "ready to process"
        Files.setLastModifiedTime(filePath, FileTime.from(ZonedDateTime.now().minusMinutes(5).toInstant()));

        Request request = verifyRequestMessage(fileName, false);
        testRequestUpload(request, fileName);
    }

    private @NotNull Request verifyRequestMessage(String fileName, boolean compressed) throws JsonProcessingException {
        ArgumentCaptor<MessageDto> messageCaptor = ArgumentCaptor.forClass(MessageDto.class);
        verify(messageService, timeout(10000)).addPendingMessage(messageCaptor.capture());
        assertThat(messageCaptor.getValue().getComponent()).isEqualTo("backup-upload");
        assertThat(messageCaptor.getValue().getJsonString()).isNotBlank();
        Request request = objectMapper.readValue(messageCaptor.getValue().getJsonString(), Request.class);
        assertThat(request.requestId()).isNotNull();
        assertThat(request.fileName()).isEqualTo(fileName);
        return request;
    }

    private void testRequestUpload(Request request, String fileName) {
        BlobClient blobClient = createAzuriteBlobClient(fileName);
        String blobUploadUrl = createUploadUrlWithSas(blobClient);
        DirectMethodPayload methodPayload = mock(DirectMethodPayload.class);
        when(methodPayload.getPayload(any())).thenReturn(new BackupUploadDto(request.requestId(), blobUploadUrl));

        directMethodCallbackService.onMethodInvoked("backup-upload", methodPayload, null);

        await().atMost(30, TimeUnit.SECONDS).until(blobClient::exists);
        BlobProperties properties = blobClient.getProperties();
        assertThat(properties.getBlobSize()).isPositive();
        assertThat(properties.getContentType()).isEqualTo("application/octet-stream");
    }

    private String createUploadUrlWithSas(BlobClient blobClient) {
        String blobUrl = blobClient.getBlobUrl();
        String sasToken = blobClient.generateSas(prepareSignatureValues());
        return "%s?%s".formatted(blobUrl, sasToken);
    }

    private static @NotNull BlobClient createAzuriteBlobClient(String fileName) {
        BlobServiceClient testClient = new BlobServiceClientBuilder().connectionString(CONNECTION_FORMAT_STRING
                .formatted(azurite.getHost(), azurite.getFirstMappedPort())).buildClient();
        BlobContainerClient containerClient = testClient.createBlobContainer(CONTAINER_NAME);
        return containerClient.getBlobClient(fileName);
    }

    private BlobServiceSasSignatureValues prepareSignatureValues() {
        OffsetDateTime now = OffsetDateTime.now();
        OffsetDateTime startTime = now.minusMinutes(15);
        OffsetDateTime linkExpiryTime = now.plusMinutes(15);
        BlobSasPermission permission = new BlobSasPermission().setAddPermission(true).setCreatePermission(true);
        return new BlobServiceSasSignatureValues(linkExpiryTime, permission).setStartTime(startTime);
    }

    record Request(UUID requestId, String fileName) {}
}
