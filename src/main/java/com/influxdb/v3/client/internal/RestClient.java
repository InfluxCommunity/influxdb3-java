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
import java.util.function.BiFunction;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.net.ssl.SSLException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.QueryStringEncoder;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.netty.ByteBufFlux;
import reactor.netty.ByteBufMono;
import reactor.netty.http.client.HttpClient;
import reactor.netty.http.client.HttpClientResponse;
import reactor.netty.resources.ConnectionProvider;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import com.influxdb.v3.client.InfluxDBApiException;
import com.influxdb.v3.client.config.InfluxDBClientConfigs;

final class RestClient implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(RestClient.class);

    private final HttpClient client;
    private final ConnectionProvider connectionProvider;
    private final ObjectMapper objectMapper = new ObjectMapper();

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
                    if (configs.getAuthToken() != null && configs.getAuthToken().length > 0) {
                        headers.add("Authorization", String.format("Token %s", new String(configs.getAuthToken())));
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

        Mono<Tuple2<HttpClientResponse, String>> responseMono;
        HttpClient.RequestSender requestSender = client
                .headers(headers -> {
                    if (contentType != null) {
                        headers.add("Content-Type", contentType);
                    }
                })
                .request(method)
                .uri(uriEncoder.toString());

        if (data != null && !data.isEmpty()) {
            responseMono = requestSender.send(ByteBufFlux.fromString(Mono.just(data))).responseSingle(toBody());
        } else {
            responseMono = requestSender.responseSingle(toBody());
        }

        Tuple2<HttpClientResponse, String> tuple = responseMono.block();
        HttpClientResponse response = tuple != null ? tuple.getT1() : null;
        if (response != null) {
            if (response.status().code() < 200 || response.status().code() >= 300) {
                String reason = "";
                String body = tuple.getT2();
                if (!body.isEmpty()) {
                    try {
                        reason = objectMapper.readTree(body).get("message").asText();
                    } catch (JsonProcessingException e) {
                        LOG.debug("Can't parse msg from response {}", response);
                    }
                }

                if (reason.isEmpty()) {
                    reason = Stream.of("X-Platform-Error-Code", "X-Influx-Error", "X-InfluxDb-Error")
                            .map(name -> response.responseHeaders().get(name))
                            .filter(message -> message != null && !message.isEmpty()).findFirst()
                            .orElse("");
                }

                if (reason.isEmpty()) {
                    reason = body;
                }

                if (reason.isEmpty()) {
                    reason = response.status().reasonPhrase();
                }

                String message = String.format("HTTP status code: %d; Message: %s", response.status().code(), reason);
                throw new InfluxDBApiException(message);
            }
        }
    }

    @Override
    public void close() {
        connectionProvider.dispose();
    }

    @Nonnull
    private static BiFunction<HttpClientResponse, ByteBufMono, Mono<Tuple2<HttpClientResponse, String>>> toBody() {
        return (response, bodyMono) -> bodyMono
                .asString()
                .switchIfEmpty(Mono.just(""))
                .map(body -> Tuples.of(response, body));
    }

}
