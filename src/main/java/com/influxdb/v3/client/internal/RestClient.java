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

import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.net.ssl.SSLException;

import com.google.common.base.Strings;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.QueryStringEncoder;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import reactor.core.publisher.Mono;
import reactor.netty.ByteBufFlux;
import reactor.netty.http.client.HttpClient;
import reactor.netty.http.client.HttpClientResponse;
import reactor.netty.resources.ConnectionProvider;

import com.influxdb.v3.client.config.InfluxDBClientConfigs;

final class RestClient implements AutoCloseable {

    private final HttpClient client;
    private final ConnectionProvider connectionProvider;

    RestClient(@Nonnull final InfluxDBClientConfigs configs) {
        Arguments.checkNotNull(configs, "configs");

        // user agent version
        Package mainPackage = RestClient.class.getPackage();
        String version = mainPackage != null ? mainPackage.getImplementationVersion() : "unknown";
        String userAgent = String.format("influxdb3-java/%s", version != null ? version : "unknown");

        // connection pool
        connectionProvider = ConnectionProvider.builder("influxdb-client").build();

        // SSL verification
        SslContextBuilder sslContextBuilder = SslContextBuilder.forClient();
        if (configs.getDisableServerCertificateValidation()) {
            sslContextBuilder.trustManager(InsecureTrustManagerFactory.INSTANCE);
        }
        SslContext sslContext;
        try {
            sslContext = sslContextBuilder.build();
        } catch (SSLException e) {
            throw new RuntimeException(e);
        }

        String baseUrl = configs.getHostUrl().endsWith("/")
                ? configs.getHostUrl() : String.format("%s/", configs.getHostUrl());
        this.client = HttpClient.create(connectionProvider)
                .baseUrl(baseUrl)
                .responseTimeout(configs.getResponseTimeout())
                .followRedirect(configs.getAllowHttpRedirects())
                .headers(headers -> {
                    headers.add("User-Agent", userAgent);
                    if (!Strings.isNullOrEmpty(configs.getAuthToken())) {
                        headers.add("Authorization", String.format("Token %s", configs.getAuthToken()));
                    }
                })
                .secure(t -> t.sslContext(sslContext));
    }

    void request(@Nonnull final String path,
                 @Nonnull final HttpMethod method,
                 @Nullable final String data,
                 @Nullable final String contentType,
                 @Nullable final Map<String, String> queryParams) {

        String uri = String.format("%s%s", client.configuration().baseUrl(), path);
        QueryStringEncoder uriEncoder = new QueryStringEncoder(uri);
        if (queryParams != null) {
            queryParams.forEach((name, value) -> {
                if (value != null && !value.isEmpty()) {
                    uriEncoder.addParam(name, value);
                }
            });
        }

        Mono<HttpClientResponse> response;
        HttpClient.RequestSender requestSender = client
                .headers(headers -> {
                    if (contentType != null) {
                        headers.add("Content-Type", contentType);
                    }
                })
                .request(method)
                .uri(uriEncoder.toString());

        if (data != null && !data.isEmpty()) {
            response = requestSender.send(ByteBufFlux.fromString(Mono.just(data))).response();
        } else {
            response = requestSender.response();
        }

        response.block();
    }

    @Override
    public void close() {
        connectionProvider.dispose();
    }
}
