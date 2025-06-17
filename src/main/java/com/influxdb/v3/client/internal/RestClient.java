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
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.QueryStringEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.influxdb.v3.client.InfluxDBApiException;
import com.influxdb.v3.client.InfluxDBApiHttpException;
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
                .connectTimeout(config.getTimeout())
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
                                 @Nullable final Map<String, String> headers) {

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
            String reason = "";
            String body = response.body();
            if (!body.isEmpty()) {
                try {
                    final JsonNode root = objectMapper.readTree(body);
                    final List<String> possibilities = List.of("message", "error_message", "error");
                    for (final String field : possibilities) {
                        final JsonNode node = root.findValue(field);
                        if (node != null) {
                            reason = node.asText();
                            break;
                        }
                    }
                } catch (JsonProcessingException e) {
                    LOG.debug("Can't parse msg from response {}", response);
                }
            }

            if (reason.isEmpty()) {
                reason = Stream.of("X-Platform-Error-Code", "X-Influx-Error", "X-InfluxDb-Error")
                        .map(name -> response.headers().firstValue(name).orElse(null))
                        .filter(message -> message != null && !message.isEmpty()).findFirst()
                        .orElse("");
            }

            if (reason.isEmpty()) {
                reason = body;
            }

            if (reason.isEmpty()) {
                reason = HttpResponseStatus.valueOf(statusCode).reasonPhrase();
            }

            String message = String.format("HTTP status code: %d; Message: %s", statusCode, reason);
            throw new InfluxDBApiHttpException(message, response.headers(), response.statusCode());
        }

        return response;
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
}
