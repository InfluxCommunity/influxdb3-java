/*
 * The MIT License
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package com.influxdb.v3.client.internal;

import java.io.FileInputStream;
import java.net.InetSocketAddress;
import java.net.ProxySelector;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.QueryStringEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.influxdb.v3.client.InfluxDBApiException;
import com.influxdb.v3.client.InfluxDBApiHttpException;
import com.influxdb.v3.client.InfluxDBPartialWriteException;
import com.influxdb.v3.client.config.ClientConfig;

final class RestClient implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(RestClient.class);

    private static final TrustManager[] TRUST_ALL_CERTS = new TrustManager[]{
            new X509TrustManager() {
                public X509Certificate[] getAcceptedIssuers() {
                    return null;
                }

                public void checkClientTrusted(
                        final X509Certificate[] certs, final String authType) {
                }

                public void checkServerTrusted(
                        final X509Certificate[] certs, final String authType) {
                }
            }
    };

    final String baseUrl;
    final String userAgent;
    final HttpClient client;

    private final ClientConfig config;
    private final Map<String, String> defaultHeaders;
    private final ObjectMapper objectMapper = new ObjectMapper();

    RestClient(@Nonnull final ClientConfig config) {
        Arguments.checkNotNull(config, "config");

        this.config = config;

        // user agent version
        this.userAgent = Identity.getUserAgent();

        // URL
        String host = config.getHost();
        this.baseUrl = host.endsWith("/") ? host : String.format("%s/", host);

        // timeout and redirects
        HttpClient.Builder builder = HttpClient.newBuilder()
                .connectTimeout(config.getWriteTimeout())
                .followRedirects(config.getAllowHttpRedirects()
                        ? HttpClient.Redirect.NORMAL : HttpClient.Redirect.NEVER);

        // default headers
        this.defaultHeaders = config.getHeaders() != null ? Map.copyOf(config.getHeaders()) : null;

        if (config.getProxyUrl() != null) {
            URI proxyUri = URI.create(config.getProxyUrl());
            ProxySelector proxy = ProxySelector.of(new InetSocketAddress(proxyUri.getHost(), proxyUri.getPort()));
            builder.proxy(proxy);
            if (config.getAuthenticator() != null) {
                builder.authenticator(config.getAuthenticator());
            }
        } else if (config.getProxy() != null) {
            builder.proxy(config.getProxy());
            if (config.getAuthenticator() != null) {
                builder.authenticator(config.getAuthenticator());
            }
        }

        if (baseUrl.startsWith("https")) {
            try {
                SSLContext sslContext = SSLContext.getInstance("TLS");
                if (config.getDisableServerCertificateValidation()) {
                    sslContext.init(null, TRUST_ALL_CERTS, new SecureRandom());
                } else if (config.sslRootsFilePath() != null) {
                    X509TrustManager x509TrustManager = getX509TrustManagerFromFile(config.sslRootsFilePath());
                    sslContext.init(null, new X509TrustManager[]{x509TrustManager}, new SecureRandom());
                } else {
                    sslContext.init(null, null, new SecureRandom());
                }
                builder.sslContext(sslContext);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        this.client = builder.build();
    }

    public String getServerVersion() {
        String influxdbVersion;
        HttpResponse<String> response = request("ping", HttpMethod.GET, null, null, null);
        try {
            influxdbVersion = response.headers().firstValue("X-Influxdb-Version").orElse(null);
            if (influxdbVersion == null) {
                JsonNode jsonNode = objectMapper.readTree(response.body());
                influxdbVersion = Optional.ofNullable(jsonNode.get("version")).map(JsonNode::asText).orElse(null);
            }
        } catch (JsonProcessingException e) {
            return null;
        }

        return influxdbVersion;
    }

    HttpResponse<String> request(@Nonnull final String path,
                                 @Nonnull final HttpMethod method,
                                 @Nullable final byte[] data,
                                 @Nullable final Map<String, String> queryParams,
                                 @Nullable final Map<String, String> headers
    ) {
        return request(path, method, data, queryParams, headers, false, false);
    }

    HttpResponse<String> request(@Nonnull final String path,
                                 @Nonnull final HttpMethod method,
                                 @Nullable final byte[] data,
                                 @Nullable final Map<String, String> queryParams,
                                 @Nullable final Map<String, String> headers,
                                 final boolean acceptPartial,
                                 final boolean useV2Api
    ) {

        QueryStringEncoder uriEncoder = new QueryStringEncoder(String.format("%s%s", baseUrl, path));
        if (queryParams != null) {
            queryParams.forEach((name, value) -> {
                if (value != null && !value.isEmpty()) {
                    uriEncoder.addParam(name, value);
                }
            });
        }

        HttpRequest.Builder request = HttpRequest.newBuilder();

        // uri
        try {
            request.uri(uriEncoder.toUri());
        } catch (URISyntaxException e) {
            throw new InfluxDBApiException(e);
        }

        // method and body
        request.method(method.name(), data == null
                ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofByteArray(data));

        // headers
        if (headers != null) {
            for (Map.Entry<String, String> entry : headers.entrySet()) {
                request.header(entry.getKey(), entry.getValue());
            }
        }
        if (defaultHeaders != null) {
            for (Map.Entry<String, String> entry : defaultHeaders.entrySet()) {
                if (headers == null || !headers.containsKey(entry.getKey())) {
                    request.header(entry.getKey(), entry.getValue());
                }
            }
        }
        request.header("User-Agent", userAgent);
        if (config.getToken() != null && config.getToken().length > 0) {
            String authScheme = config.getAuthScheme();
            if (authScheme == null) {
                authScheme = "Token";
            }
            request.header("Authorization", String.format("%s %s", authScheme, new String(config.getToken())));
        }

        HttpResponse<String> response;
        try {
            response = client.send(request.build(), HttpResponse.BodyHandlers.ofString());
        } catch (Exception e) {
            throw new InfluxDBApiException(e);
        }

        int statusCode = response.statusCode();
        if (statusCode < 200 || statusCode >= 300) {
            handleErrorResponse(response, path, acceptPartial, useV2Api);
        }

        return response;
    }

    private void handleErrorResponse(@Nonnull final HttpResponse<String> response,
                                     @Nonnull final String path,
                                     final boolean acceptPartial,
                                     final boolean useV2Api
    ) {
        int statusCode = response.statusCode();
        String contentType = response.headers().firstValue("Content-Type").orElse(null);

        if (!errIsJsonLikeContentType(contentType)) {
            throw createHttpException(statusCode, extractFallbackReason(response), response);
        }

        JsonNode root = parseJsonBody(response.body());
        if (root == null) {
            throw createHttpException(statusCode, extractFallbackReason(response), response);
        }

        String rootMessage = errNonEmptyField(root, "message");
        if (rootMessage != null) {
            throw createHttpException(statusCode, rootMessage, response);
        }

        String reason = Optional.ofNullable(errNonEmptyField(root, "error")).orElse("");
        if (isV3PartialWriteError(statusCode, path, acceptPartial, useV2Api, root) && root.isObject()) {
            // InfluxDB 3 Core/Enterprise partial write error format:
            // {"error":"...","data":[{"error_message":"...","line_number":2,"original_line": "..."}]}
            handlePartialWriteError(statusCode, response, (ObjectNode) root, reason);
        }

        // Core/Enterprise object format:
        // {"error":"...","data":{"error_message":"..."}}
        JsonNode dataNode = root.get("data");
        if (dataNode != null && dataNode.isObject()) {
            reason = formatObjectDataError(dataNode, reason);
        }

        if (reason.isEmpty()) {
            reason = extractFallbackReason(response);
        }

        throw createHttpException(statusCode, reason, response);
    }

    @Nonnull
    private String extractFallbackReason(@Nonnull final HttpResponse<String> response) {
        var reason = extractErrorMsgInHeader(response);
        if (reason.isEmpty()) {
            reason = response.body();
        }
        if (reason.isEmpty()) {
            reason = HttpResponseStatus.valueOf(response.statusCode()).reasonPhrase();
        }
        return reason;
    }

    private boolean errIsJsonLikeContentType(@Nullable final String contentType) {
        return contentType == null
                || contentType.isEmpty()
                || contentType.regionMatches(true, 0, "application/json", 0, "application/json".length());
    }

    @Nullable
    private JsonNode parseJsonBody(@Nullable final String body) {
        try {
            if (body == null) {
                return null;
            }
            return objectMapper.readTree(body);
        } catch (JsonProcessingException e) {
            LOG.debug("Can't parse msg from response body {}", body, e);
            return null;
        }
    }

    @Nonnull
    private InfluxDBApiHttpException createHttpException(final int statusCode,
                                                         @Nullable final String reason,
                                                         @Nonnull final HttpResponse<String> response
    ) {
        String message = String.format("HTTP status code: %d; Message: %s", statusCode, reason);
        return new InfluxDBApiHttpException(message, response.headers(), response.statusCode());
    }

    private void handlePartialWriteError(final int statusCode,
                                         @Nonnull final HttpResponse<String> response,
                                         @Nonnull final ObjectNode root,
                                         @Nonnull final String baseReason
    ) {
        ParseLineErrorResult result = parsePartialWriteLineErrors(root);
        List<String> errorMsgDetails = createErrorMsgDetails(result, root);
        String reason = baseReason;

        if (!errorMsgDetails.isEmpty()) {
            StringBuilder sb = new StringBuilder(baseReason).append(":");
            for (String detailError : errorMsgDetails) {
                sb.append("\n\t").append(detailError);
            }
            reason = sb.toString();
        }

        String message = String.format("HTTP status code: %d; Message: %s", statusCode, reason);
        throw new InfluxDBPartialWriteException(
                message,
                response.headers(),
                response.statusCode(),
                result.lineErrors()
        );
    }

    @Nonnull
    private static String extractErrorMsgInHeader(@Nonnull final HttpResponse<String> response) {
        String reason = "";
        reason = Stream.of("X-Platform-Error-Code", "X-Influx-Error", "X-InfluxDb-Error")
                .map(name -> response.headers().firstValue(name).orElse(null))
                .filter(message -> message != null && !message.isEmpty()).findFirst()
                .orElse("");

        return reason;
    }

    @Nonnull
    private String formatObjectDataError(@Nonnull final JsonNode dataNode, @Nonnull final String error) {
        String lineNumber = Optional.ofNullable(errNonEmptyField(dataNode, "line_number")).orElse("");
        String errorMessage = Optional.ofNullable(errNonEmptyField(dataNode, "error_message")).orElse("");
        String originalLine = Optional.ofNullable(errNonEmptyField(dataNode, "original_line")).orElse("");

        if (!errorMessage.isEmpty() && (lineNumber.isEmpty() || !Utils.isInteger(lineNumber))) {
            return error + ":\n\t" + errorMessage;
        } else if (!errorMessage.isEmpty() && Utils.isInteger(lineNumber) && originalLine.isEmpty()) {
            return String.format("%s:\n\tline %s: %s", error, lineNumber, errorMessage);
        } else if (!errorMessage.isEmpty() && !originalLine.isEmpty()) {
            return String.format("%s:\n\tline %s: %s (%s)", error, lineNumber, errorMessage, originalLine);
        }
        return error;
    }

    @Nonnull
    private List<String> createErrorMsgDetails(
            @Nonnull final ParseLineErrorResult result,
            @Nonnull final ObjectNode root
    ) {
        if (result.allTyped()) {
            return result.lineErrors().stream()
                    .map(this::formatLineError)
                    .collect(Collectors.toList());
        }

        List<String> errorMsgDetails = new ArrayList<>();
        root.path("data").forEach(node -> errorMsgDetails.add(node.toString()));
        return errorMsgDetails;
    }

    @Nullable
    private String formatLineError(@Nonnull final InfluxDBPartialWriteException.LineError lineError) {
        Integer lineNumber = lineError.lineNumber();
        String originalLine = lineError.originalLine();
        String errorMessage = lineError.errorMessage();

        if (lineNumber != null) {
            if (originalLine != null && !originalLine.isEmpty()) {
                return String.format("line %d: %s (%s)", lineNumber, errorMessage, originalLine);
            }
            return String.format("line %d: %s", lineNumber, errorMessage);
        }
        return errorMessage;
    }

    @Nonnull
    private ParseLineErrorResult parsePartialWriteLineErrors(@Nonnull final ObjectNode root) {
        var allTyped = true;
        final List<InfluxDBPartialWriteException.LineError> lineErrors = new ArrayList<>();
        for (JsonNode node : root.withArray("data")) {
            final InfluxDBPartialWriteException.LineError lineError = parseLineError(node);
            if (lineError != null) {
                lineErrors.add(lineError);
            } else {
                allTyped = false;
            }
        }
        return new ParseLineErrorResult(lineErrors, !lineErrors.isEmpty() && allTyped);
    }

    @Nullable
    private InfluxDBPartialWriteException.LineError parseLineError(@Nonnull final JsonNode node) {
        if (!node.isObject()) {
            return null;
        }

        final String errorMessage = errNonEmptyField(node, "error_message");
        if (errorMessage == null) {
            return null;
        }

        final String lineNumberStr = errNonEmptyField(node, "line_number");
        Integer lineNumber = null;
        if (lineNumberStr != null) {
            if (!Utils.isInteger(lineNumberStr)) {
                return null;
            }
            lineNumber = Integer.parseInt(lineNumberStr);
        }

        final String originalLine = errNonEmptyField(node, "original_line");
        return new InfluxDBPartialWriteException.LineError(lineNumber, errorMessage, originalLine);
    }

    private boolean isV3PartialWriteError(@Nonnull final Integer statusCode,
                                          @Nonnull final String path,
                                          final boolean isAcceptPartial,
                                          final boolean isWriteUseV2Api,
                                          @Nullable final JsonNode bodyRoot
    ) {
        final String error = errNonEmptyField(bodyRoot, "error");
        if (error == null || error.isEmpty()) {
            return false;
        }
        return statusCode == 400
                && "api/v3/write_lp".equals(path)
                && isAcceptPartial
                && !isWriteUseV2Api
                && bodyRoot.path("data").isArray();
    }

    @Nullable
    private String errNonEmptyText(@Nullable final JsonNode node) {
        if (node == null || node.isNull()) {
            return null;
        }

        final String value;
        if (node.isTextual()) {
            value = node.asText();
        } else if (node.isNumber() || node.isBoolean()) {
            value = node.asText();
        } else {
            value = node.toString();
        }

        return value.isEmpty() ? null : value;
    }

    @Nullable
    private String errNonEmptyField(@Nullable final JsonNode object, @Nonnull final String fieldName) {
        if (object == null || !object.isObject()) {
            return null;
        }
        return errNonEmptyText(object.get(fieldName));
    }

    private X509TrustManager getX509TrustManagerFromFile(@Nonnull final String filePath) {
        try {
            KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
            trustStore.load(null);

            FileInputStream fis = new FileInputStream(filePath);
            List<? extends Certificate> certificates = new ArrayList<Certificate>(
                    CertificateFactory.getInstance("X.509")
                            .generateCertificates(fis)
            );

            for (int i = 0; i < certificates.size(); i++) {
                Certificate cert = certificates.get(i);
                trustStore.setCertificateEntry("alias" + i, cert);
            }

            TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(
                    TrustManagerFactory.getDefaultAlgorithm()
            );
            trustManagerFactory.init(trustStore);
            X509TrustManager x509TrustManager = null;
            for (TrustManager trustManager : trustManagerFactory.getTrustManagers()) {
                if (trustManager instanceof X509TrustManager) {
                    x509TrustManager = (X509TrustManager) trustManager;
                }
            }
            return x509TrustManager;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
    }

    private record ParseLineErrorResult(List<InfluxDBPartialWriteException.LineError> lineErrors, boolean allTyped) {
    }
}

