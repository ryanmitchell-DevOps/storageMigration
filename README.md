package com.gea.ft.service.farmsiteconnector.config;

import com.azure.core.credential.AzureNamedKeyCredential;
import com.azure.identity.DefaultAzureCredential;
import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.Profile;

@Configuration
public class BlobServiceClientConfiguration {

    @Profile("!local & !openapi & !test")
    @Bean
    public AzureCredential defaultCredential() {
        return new AzureCredential(new DefaultAzureCredentialBuilder().build(), null);
    }

    @Bean(name = "publicBlobClient")
    public BlobServiceClient publicBlobServiceClient(AzureCredential credential,
                                                     @Value("${public.assets.storage.account.endpoint}") String publicStorageAccountEndpoint) {
        BlobServiceClientBuilder builder = new BlobServiceClientBuilder().endpoint(publicStorageAccountEndpoint);
        if (credential.defaultCredential != null) {
            builder = builder.credential(credential.defaultCredential);
        } else {
            builder = builder.credential(credential.keyCredential);
        }
        return builder.buildClient();
    }

    @Primary
    @Bean
    public BlobServiceClient blobServiceClient(AzureCredential credential,
                                               @Value("${storage.account.endpoint}") String storageAccountEndpoint) {
        BlobServiceClientBuilder builder = new BlobServiceClientBuilder().endpoint(storageAccountEndpoint);
        if (credential.defaultCredential != null) {
            builder = builder.credential(credential.defaultCredential);
        } else {
            builder = builder.credential(credential.keyCredential);
        }
        return builder.buildClient();
    }

    @lombok.Value
    public static class AzureCredential {
        DefaultAzureCredential defaultCredential;
        AzureNamedKeyCredential keyCredential;
    }

}
package com.gea.ft.service.farmsiteconnector.config;

import com.azure.core.credential.AzureNamedKeyCredential;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

@Configuration
@Profile("local | openapi | test")
public class NoOpBlobServiceClientConfiguration {

    @Bean
    public BlobServiceClientConfiguration.AzureCredential defaultCredential(@Value("${spring.cloud.azure.storage.blob.account-name}") String accountName,
                                                                            @Value("${spring.cloud.azure.storage.blob.account-key}") String accountKey) {
        return new BlobServiceClientConfiguration.AzureCredential(null, new AzureNamedKeyCredential(accountName,
                accountKey));
    }
}
package com.gea.ft.service.farmsiteconnector.controller;

import com.gea.ft.service.exception.ApiError;
import com.gea.ft.service.exception.utils.LogApiError;
import com.gea.ft.service.farmsiteconnector.dto.CountryDto;
import com.gea.ft.service.farmsiteconnector.dto.LanguageDto;
import com.gea.ft.service.farmsiteconnector.enums.Language;
import com.gea.ft.service.farmsiteconnector.service.BlobService;
import com.gea.ft.service.farmsiteconnector.service.DocumentService;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.enums.ParameterIn;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import lombok.RequiredArgsConstructor;
import org.slf4j.event.Level;
import org.springframework.core.io.InputStreamResource;
import org.springframework.core.io.Resource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;

import jakarta.validation.ConstraintViolationException;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Pattern;
import java.io.InputStream;
import java.util.List;
import java.util.UUID;

import static jakarta.validation.constraints.Pattern.Flag.CASE_INSENSITIVE;

@RestController
@RequestMapping(value = "${api.base-path}/documents/")
@Validated
@RequiredArgsConstructor
public class DocumentController {

    private static final String FV_TERMS_OF_USE = "fv-terms-of-use";
    private static final String REMOTE_ACCESS_POLICY = "remote-access-policy";
    private static final String TERMS_OF_SALES = "terms-of-sales";
    private static final String SAM_MANUAL = "sam-manual";
    private static final String SAM_TROUBLESHOOTING = "sam-troubleshooting";
    private static final String DOC_REGEX = FV_TERMS_OF_USE + "|" + REMOTE_ACCESS_POLICY + "|" + TERMS_OF_SALES + "|" +
            SAM_MANUAL + "|" + SAM_TROUBLESHOOTING;

    private final BlobService blobService;
    private final DocumentService documentService;

    @GetMapping(path = "{documentType}/{language}", produces = MediaType.APPLICATION_OCTET_STREAM_VALUE)
    @Operation(summary = "Get document")
    @ApiResponse(responseCode = "200", description = "Returns the desired document")
    @Parameter(in = ParameterIn.PATH, name = "documentType", schema = @Schema(type = "string", allowableValues = {FV_TERMS_OF_USE, REMOTE_ACCESS_POLICY, TERMS_OF_SALES, SAM_MANUAL, SAM_TROUBLESHOOTING}))
    @Parameter(in = ParameterIn.PATH, name = "language", schema = @Schema(type = "string"))
    public ResponseEntity<Resource> getDocument(@PathVariable("documentType") @Pattern(regexp = DOC_REGEX, flags = {CASE_INSENSITIVE}) String documentType,
                                                @PathVariable("language") Language language) {

        String path = "%s/%s.pdf".formatted(documentType.toLowerCase(), language.toString().toLowerCase());
        InputStream file = blobService.download(path);
        HttpHeaders headers = new HttpHeaders();
        headers.add("Content-Disposition", "attachment; filename=%s.pdf".formatted(documentType.toLowerCase()));
        return ResponseEntity.ok()
                .headers(headers)
                .contentType(MediaType.APPLICATION_OCTET_STREAM)
                .body(new InputStreamResource(file));
    }

    @GetMapping(path = "{documentType}/{language}/pdf", produces = MediaType.APPLICATION_PDF_VALUE)
    @Operation(summary = "Get document as a PDF")
    @ApiResponse(responseCode = "200", description = "Returns the desired document")
    @Parameter(in = ParameterIn.PATH, name = "documentType", schema = @Schema(type = "string", allowableValues = {FV_TERMS_OF_USE, REMOTE_ACCESS_POLICY, TERMS_OF_SALES, SAM_MANUAL, SAM_TROUBLESHOOTING}))
    public ResponseEntity<Resource> getDocumentAsPdf(@PathVariable("documentType") @Pattern(regexp = DOC_REGEX, flags = {CASE_INSENSITIVE}) String documentType,
                                                     @Valid @Parameter(name = "country") CountryDto country,
                                                     @PathVariable("language") Language language) {
        InputStream file = documentService.getDocument(documentType.toLowerCase(),
                country.getCountry(),
                language.toString().toLowerCase());
        HttpHeaders headers = new HttpHeaders();
        headers.add("Content-Disposition", "attachment; filename=%s.pdf".formatted(documentType.toLowerCase()));
        return ResponseEntity.ok()
                .headers(headers)
                .contentType(MediaType.APPLICATION_PDF)
                .body(new InputStreamResource(file));
    }

    @GetMapping(value = "{documentType}/languages", produces = MediaType.APPLICATION_JSON_VALUE)
    @Operation(summary = "Get file list under the folder")
    @ApiResponse(responseCode = "200", description = "Returns the file name list of the specified folder")
    @Parameter(in = ParameterIn.PATH, name = "documentType", schema = @Schema(type = "string", allowableValues = {FV_TERMS_OF_USE, REMOTE_ACCESS_POLICY, TERMS_OF_SALES, SAM_MANUAL, SAM_TROUBLESHOOTING}))
    public List<LanguageDto> getAvailableLanguagesForDocument(@PathVariable("documentType") @Pattern(regexp = DOC_REGEX, flags = {CASE_INSENSITIVE}) String documentType,
                                                              @Valid @Parameter(name = "country") CountryDto country) {
        return documentService.getAvailableLanguagesForDocument(documentType, country.getCountry())
                .stream()
                .map(LanguageDto::new)
                .toList();
    }

    @ExceptionHandler(ConstraintViolationException.class)
    @ResponseStatus(HttpStatus.NOT_FOUND)
    @LogApiError(Level.WARN)
    public ApiError handleConstraintViolationException(ConstraintViolationException e) {
        if (exceptionMessageContains(e, "documentType")) {
            // This error is thrown by the annotation when the document type is not valid
            return new ApiError(UUID.randomUUID(), "Invalid document type");
        } else {
            throw e;
        }
    }

    @ExceptionHandler(MethodArgumentTypeMismatchException.class)
    @ResponseStatus(HttpStatus.NOT_FOUND)
    @LogApiError(Level.WARN)
    public ApiError handleMethodArgumentNotValidException(MethodArgumentTypeMismatchException e) {
        if (exceptionMessageContains(e, "Language")) {
            // This error is thrown by the annotation when the language is not valid
            return new ApiError(UUID.randomUUID(), "Invalid language");
        } else {
            throw e;
        }
    }

    private static boolean exceptionMessageContains(Exception e, String containText) {
        String m = e.getMessage();
        if (m == null) {
            return false;
        }
        return m.contains(containText);
    }
}
connector/last-connection-date/b51b82a4-3986-4748-b3c1-7169de458aed;client=10.74.59.11;user=32846f05-78f7-4df7-b934-cefc084f21c1","exceptionClass":"ItemNotFoundException","imageVersion":"d8da4a176e2707652f80c63bc2948005a549c75d"}
{"timestamp":"2026-07-21T13:17:59.922503308Z","logger":"com.gea.ft.service.farmsiteconnector.service.mobileproxy.MobileProxyWhitelistService","level":"DEBUG","thread":"http-nio-8080-exec-6","mdc":{"trace_id":"9af07abba194ed6cc598a77a2d0e391d","trace_flags":"01","span_id":"b6426dddef675527"},"message":"Mobile proxy request matched whitelist entry with alias get.mobile: GET /mobile/basicdata","imageVersion":"d8da4a176e2707652f80c63bc2948005a549c75d"}
{"timestamp":"2026-07-21T13:17:59.922615414Z","logger":"com.gea.ft.service.farmsiteconnector.service.mobileproxy.MobileProxyService","level":"DEBUG","thread":"http-nio-8080-exec-6","mdc":{"trace_id":"9af07abba194ed6cc598a77a2d0e391d","trace_flags":"01","span_id":"b6426dddef675527"},"message":"Proxying request: GET /mobile/basicdata","imageVersion":"d8da4a176e2707652f80c63bc2948005a549c75d"}
{"timestamp":"2026-07-21T13:17:59.977654392Z","logger":"com.azure.identity.ChainedTokenCredential","level":"INFO","thread":"http-nio-8080-exec-6","mdc":{"trace_id":"9af07abba194ed6cc598a77a2d0e391d","trace_flags":"00","span_id":"663e4a4a296d2db3"},"message":"Azure Identity => Attempted credential EnvironmentCredential is unavailable.","imageVersion":"d8da4a176e2707652f80c63bc2948005a549c75d"}
{"timestamp":"2026-07-21T13:18:00.131812875Z","logger":"com.azure.identity.ChainedTokenCredential","level":"INFO","thread":"http-nio-8080-exec-6","mdc":{"trace_id":"9af07abba194ed6cc598a77a2d0e391d","trace_flags":"00","span_id":"663e4a4a296d2db3"},"message":"Azure Identity => Attempted credential WorkloadIdentityCredential returns a token","imageVersion":"d8da4a176e2707652f80c63bc2948005a549c75d"}
{"timestamp":"2026-07-21T13:18:00.236480959Z","logger":"com.gea.ft.service.exception.utils.LogApiErrorHandler","level":"WARN","thread":"http-nio-8080-exec-6","mdc":{"trace_id":"9af07abba194ed6cc598a77a2d0e391d","trace_flags":"01","span_id":"b6426dddef675527"},"stackTrace":"com.azure.storage.blob.models.BlobStorageException: If you are using a StorageSharedKeyCredential, and the server returned an error message that says 'Signature did not match', you can compare the string to sign with the one generated by the SDK. To log the string to sign, pass in the context key value pair 'Azure-Storage-Log-String-To-Sign': true to the appropriate method call.\nIf you are using a SAS token, and the server returned an error message that says 'Signature did not match', you can compare the string to sign with the one generated by the SDK. To log the string to sign, pass in the context key value pair 'Azure-Storage-Log-String-To-Sign': true to the appropriate generateSas method call.\nPlease remember to disable 'Azure-Storage-Log-String-To-Sign' before going to production as this string can potentially contain PII.\nIf you are using a StorageSharedKeyCredential, and the server returned an error message that says 'Signature did not match', you can compare the string to sign with the one generated by the SDK. To log the string to sign, pass in the context key value pair 'Azure-Storage-Log-String-To-Sign': true to the appropriate method call.\nIf you are using a SAS token, and the server returned an error message that says 'Signature did not match', you can compare the string to sign with the one generated by the SDK. To log the string to sign, pass in the context key value pair 'Azure-Storage-Log-String-To-Sign': true to the appropriate generateSas method call.\nPlease remember to disable 'Azure-Storage-Log-String-To-Sign' before going to production as this string can potentially contain PII.\nStatus code 403, \"﻿<?xml version=\"1.0\" encoding=\"utf-8\"?><Error><Code>AuthorizationPermissionMismatch</Code><Message>This request is not authorized to perform this operation using this permission.\nRequestId:db271b73-e01e-0029-5d13-193963000000\nTime:2026-07-21T13:18:00.1823473Z</Message></Error>\"\n\tat com.azure.storage.blob.implementation.util.ModelHelper.mapToBlobStorageException(ModelHelper.java:660)\n\tat com.azure.storage.blob.implementation.ServicesImpl.getUserDelegationKeyWithResponse(ServicesImpl.java:1969)\n\tat com.azure.storage.blob.BlobServiceClient.lambda$getUserDelegationKeyWithResponse$8(BlobServiceClient.java:798)\n\tat com.azure.storage.common.implementation.StorageImplUtils.sendRequest(StorageImplUtils.java:494)\n\tat com.azure.storage.blob.BlobServiceClient.getUserDelegationKeyWithResponse(BlobServiceClient.java:803)\n\tat com.azure.storage.blob.BlobServiceClient.getUserDelegationKey(BlobServiceClient.java:764)\n\tat com.gea.ft.lib.cloud.directmethod.service.sas.IdentityBasedSasGenerator.generateSas(IdentityBasedSasGenerator.java:23)\n\tat com.gea.ft.lib.cloud.directmethod.service.CloudDirectMethodBlobService.generateSas(CloudDirectMethodBlobService.java:76)\n\tat com.gea.ft.lib.cloud.directmethod.service.CloudDirectMethodBlobService.generateSasForBlobUpload(CloudDirectMethodBlobService.java:69)\n\tat com.gea.ft.lib.cloud.directmethod.service.CloudDirectMethodMessageHelper.buildMessageRequestEnvelope(CloudDirectMethodMessageHelper.java:43)\n\tat com.gea.ft.lib.cloud.directmethod.service.DirectMethodService.invokeCustomModule(DirectMethodService.java:61)\n\tat com.gea.ft.service.farmsiteconnector.service.mobileproxy.MobileProxyDirectMethodService.sendRequestToDevice(MobileProxyDirectMethodService.java:34)\n\t... 165 common frames omitted\nWrapped by: com.gea.ft.service.farmsiteconnector.exception.IoTHubCommunicationException: com.azure.storage.blob.models.BlobStorageException: If you are using a StorageSharedKeyCredential, and the server returned an error message that says 'Signature did not match', you can compare the string to sign with the one generated by the SDK. To log the string to sign, pass in the context key value pair 'Azure-Storage-Log-String-To-Sign': true to the appropriate method call.\nIf you are using a SAS token, and the server returned an error message that says 'Signature did not match', you can compare the string to sign with the one generated by the SDK. To log the string to sign, pass in the context key value pair 'Azure-Storage-Log-String-To-Sign': true to the appropriate generateSas method call.\nPlease remember to disable 'Azure-Storage-Log-String-To-Sign' before going to production as this string can potentially contain PII.\nIf you are using a StorageSharedKeyCredential, and the server returned an error message that says 'Signature did not match', you can compare the string to sign with the one generated by the SDK. To log the string to sign, pass in the context key value pair 'Azure-Storage-Log-String-To-Sign': true to the appropriate method call.\nIf you are using a SAS token, and the server returned an error message that says 'Signature did not match', you can compare the string to sign with the one generated by the SDK. To log the string to sign, pass in the context key value pair 'Azure-Storage-Log-String-To-Sign': true to the appropriate generateSas method call.\nPlease remember to disable 'Azure-Storage-Log-String-To-Sign' before going to production as this string can potentially contain PII.\nStatus code 403, \"﻿<?xml version=\"1.0\" encoding=\"utf-8\"?><Error><Code>AuthorizationPermissionMismatch</Code><Message>This request is not authorized to perform this operation using this permission.\nRequestId:db271b73-e01e-0029-5d13-193963000000\nTime:2026-07-21T13:18:00.1823473Z</Message></Error>\"\n\tat com.gea.ft.service.farmsiteconnector.service.mobileproxy.MobileProxyDirectMethodService.sendRequestToDevice(MobileProxyDirectMethodService.java:43)\n\tat com.gea.ft.service.farmsiteconnector.service.mobileproxy.MobileProxyService.proxyCall(MobileProxyService.java:65)\n\tat com.gea.ft.service.farmsiteconnector.controller.v1.MobileProxyV1Controller.proxy(MobileProxyV1Controller.java:53)\n\tat jdk.internal.reflect.DirectMethodHandleAccessor.invoke(Unknown Source)\n\tat java.lang.reflect.Method.invoke(Unknown Source)\n\tat org.springframework.aop.support.AopUtils.invokeJoinpointUsingReflection(AopUtils.java:360)\n\tat org.springframework.aop.framework.ReflectiveMethodInvocation.invokeJoinpoint(ReflectiveMethodInvocation.java:196)\n\tat org.springframework.aop.framework.ReflectiveMethodInvocation.proceed(ReflectiveMethodInvocation.java:163)\n\tat org.springframework.aop.aspectj.MethodInvocationProceedingJoinPoint.proceed(MethodInvocationProceedingJoinPoint.java:89)\n\tat com.gea.ft.service.authorization.evaluation.utils.AuthorizationCheck.checkAuthorization(AuthorizationCheck.java:45)\n\tat jdk.internal.reflect.DirectMethodHandleAccessor.invoke(Unknown Source)\n\tat java.lang.reflect.Method.invoke(Unknown Source)\n\tat org.springframework.aop.aspectj.AbstractAspectJAdvice.invokeAdviceMethodWithGivenArgs(AbstractAspectJAdvice.java:649)\n\tat org.springframework.aop.aspectj.AbstractAspectJAdvice.invokeAdviceMethod(AbstractAspectJAdvice.java:631)\n\tat org.springframework.aop.aspectj.AspectJAroundAdvice.invoke(AspectJAroundAdvice.java:71)\n\tat org.springframework.aop.framework.ReflectiveMethodInvocation.proceed(ReflectiveMethodInvocation.java:173)\n\tat org.springframework.aop.interceptor.ExposeInvocationInterceptor.invoke(ExposeInvocationInterceptor.java:97)\n\tat org.springframework.aop.framework.ReflectiveMethodInvocation.proceed(ReflectiveMethodInvocation.java:184)\n\tat org.springframework.aop.framework.CglibAopProxy$DynamicAdvisedInterceptor.intercept(CglibAopProxy.java:728)\n\tat 




    a
