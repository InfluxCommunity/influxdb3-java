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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.influxdb.v3.client.InfluxDBApiException;
import com.influxdb.v3.client.InfluxDBApiNettyException;
import com.influxdb.v3.client.config.ClientConfig;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.oio.OioEventLoopGroup;
import io.netty.channel.socket.oio.OioSocketChannel;
import io.netty.handler.codec.http.*;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.proxy.HttpProxyHandler;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.util.CharsetUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.net.ssl.SSLException;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;
import java.io.FileInputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

final class RestClient implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(RestClient.class);

//    private static final TrustManager[] TRUST_ALL_CERTS = new TrustManager[]{
//            new X509TrustManager() {
//                public X509Certificate[] getAcceptedIssuers() {
//                    return null;
//                }
//
//                public void checkClientTrusted(
//                        final X509Certificate[] certs, final String authType) {
//                }
//
//                public void checkServerTrusted(
//                        final X509Certificate[] certs, final String authType) {
//                }
//            }
//    };

    final String baseUrl;
    final String userAgent;
    private final Integer port;
    private final String host;
    private SslContext sslContext;
    final Duration timeout;
    Channel channel;

    private final EventLoopGroup eventLoopGroup;

    private final ClientHandler clientHandler;
    private HttpProxyHandler proxyHandler;


    private final ClientConfig config;
    private final Map<String, String> defaultHeaders;
    private final ObjectMapper objectMapper = new ObjectMapper();

    RestClient(@Nonnull final ClientConfig config) throws SSLException, URISyntaxException {
        Arguments.checkNotNull(config, "config");

        this.config = config;

        // user agent version
        this.userAgent = Identity.getUserAgent();


        String host = config.getHost();
        this.baseUrl = host.endsWith("/") ? host : String.format("%s/", host);

        URI uri = new URI(config.getHost());
        String scheme = uri.getScheme() == null ? "http" : uri.getScheme();
        int port = uri.getPort();
        if (port == -1) {
            if ("http".equalsIgnoreCase(scheme)) {
                port = 80;
            } else if ("https".equalsIgnoreCase(scheme)) {
                port = 443;
            }
        }
        this.port = port;
        this.host = uri.getHost();

        this.timeout = config.getWriteTimeout();

        if ("https".equalsIgnoreCase(scheme)) {
            if (config.getNettyHttpClientConfig() != null && config.getNettyHttpClientConfig().getSslContext() != null) {
                this.sslContext = config.getNettyHttpClientConfig().getSslContext();
            } else {
                this.sslContext = SslContextBuilder.forClient().build();
            }
        }

        this.eventLoopGroup = new OioEventLoopGroup();

//        this.promise = this.eventLoopGroup.next().newPromise();

//        ------- Config proxy
//        if (config.getProxyUrl() != null) {
//            URI proxyUri = URI.create(config.getProxyUrl());
//            InetSocketAddress proxyAddress = new InetSocketAddress(proxyUri.getHost(), proxyUri.getPort());
//            this.proxyHandler = new HttpProxyHandler(proxyAddress);
//        } else if (config.getNettyHttpClientConfig() != null && config.getNettyHttpClientConfig().getHttpProxyHandler() != null) {
//            this.proxyHandler = config.getNettyHttpClientConfig().getHttpProxyHandler();
//        }

        //fixme timeout and redirects
//        HttpClient.Builder builder = HttpClient.newBuilder()
//                .connectTimeout(config.getWriteTimeout())
//                .followRedirects(config.getAllowHttpRedirects()
//                        ? HttpClient.Redirect.NORMAL : HttpClient.Redirect.NEVER);
//        -------

        // default headers
        this.defaultHeaders = config.getHeaders() != null ? Map.copyOf(config.getHeaders()) : null;

        //fixme `clientHandler`should store in a list and pass to `ClientChannelInitializer()`
        this.clientHandler = new ClientHandler();

//        if (config.getProxyUrl() != null) {
//            URI proxyUri = URI.create(config.getProxyUrl());
//            ProxySelector proxy = ProxySelector.of(new InetSocketAddress(proxyUri.getHost(), proxyUri.getPort()));
//            builder.proxy(proxy);
//            if (config.getAuthenticator() != null) {
//                builder.authenticator(config.getAuthenticator());
//            }
//        } else if (config.getProxy() != null) {
//            builder.proxy(config.getProxy());
//            if (config.getAuthenticator() != null) {
//                builder.authenticator(config.getAuthenticator());
//            }
//        }
    }

    public Bootstrap getBootstrap() {
        Bootstrap b = new Bootstrap();
        b.group(this.eventLoopGroup)
                .channel(OioSocketChannel.class)
                .handler(new LoggingHandler(LogLevel.INFO))
                .handler(new ClientChannelInitializer(this.host, this.port, this.sslContext, this.proxyHandler, this.clientHandler))
                .remoteAddress(this.host, this.port);
        return b;
    }

    public String getServerVersion() throws ExecutionException, InterruptedException {
        String influxdbVersion;
        FullHttpResponse response = this.request(HttpMethod.GET, "/ping");
        try {
            influxdbVersion = response.headers().get("x-influxdb-version");
            if (influxdbVersion == null) {
                var str = response.content().toString(CharsetUtil.UTF_8);
                JsonNode jsonNode = objectMapper.readTree(str);
                influxdbVersion = Optional.ofNullable(jsonNode.get("version")).map(JsonNode::asText).orElse(null);
            }
        } catch (JsonProcessingException e) {
            return null;
        }

        return influxdbVersion;
    }

    public FullHttpResponse request(@Nonnull HttpMethod method, @Nonnull String path, Map<String, String> headers) throws RuntimeException, InterruptedException, ExecutionException {
        return request(method, path, headers, null, null);
    }

    public FullHttpResponse request(@Nonnull HttpMethod method, @Nonnull String path) throws RuntimeException, InterruptedException, ExecutionException {
        return request(method, path, null, null, null);
    }

    public FullHttpResponse request(@Nonnull HttpMethod method,
                                    @Nonnull String path,
                                    @Nullable Map<String, String> headers,
                                    @Nullable byte[] body,
                                    @Nullable Map<String, String> queryParams)
            throws RuntimeException, InterruptedException, ExecutionException {
        var content = Unpooled.EMPTY_BUFFER;
        if (body != null) {
            content = Unpooled.copiedBuffer(body);
        }

        String uri = path.startsWith("/") ? path : "/" + path;
        if (queryParams != null) {
            QueryStringEncoder queryStringEncoder = new QueryStringEncoder("/" + path);
            queryParams.forEach((key, value) -> {
                if (value != null) {
                    queryStringEncoder.addParam(key, value);
                }
            });
            uri = queryStringEncoder.toString();
        }


        HttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, method, uri, content);

        if (this.defaultHeaders != null) {
            this.defaultHeaders.forEach((s, s2) -> request.headers().set(s, s2));
        }

        request.headers().add("user-agent", this.userAgent);
        request.headers().add("host", String.format("%s:%d", this.host, this.port));
        request.headers().add("content-length", body == null ? "0" : body.length + "");
        if (this.config.getToken() != null && this.config.getToken().length > 0) {
            String authScheme = config.getAuthScheme();
            if (authScheme == null) {
                authScheme = "Token";
            }
            request.headers().add("authorization", String.format("%s %s", authScheme, new String(this.config.getToken())));
        }

        request.headers().add("accept", "*/*");

        if (headers != null) {
            headers.forEach(request.headers()::set);
        }


        if (this.channel == null || !this.channel.isOpen()) {
            ChannelFuture channelFuture = getBootstrap().connect().sync();
            if (!channelFuture.await(this.timeout.toMillis(), TimeUnit.MILLISECONDS)) {
                throw new InfluxDBApiException(new ConnectTimeoutException());
            } else {
                this.channel = channelFuture.channel();
            }
        }

        //fixme remove syncUninterruptibly
        this.channel.writeAndFlush(request).sync();

        FullHttpResponse fullHttpResponse = this.clientHandler.getResponseFuture().get();

        // Extract headers into io.netty.handler.codec.http.HttpHeaders;
        HttpHeaders responseHeaders = new DefaultHttpHeaders();
        fullHttpResponse.headers().forEach(entry -> responseHeaders.add(entry.getKey(), entry.getValue()));
        int statusCode = fullHttpResponse.status().code();
        if (statusCode < 200 || statusCode >= 300) {
            String reason = "";
            var strContent = fullHttpResponse.content().toString(CharsetUtil.UTF_8);
            if (!strContent.isEmpty()) {
                try {
                    final JsonNode root = objectMapper.readTree(strContent);
                    final List<String> possibilities = List.of("message", "error_message", "error");
                    for (final String field : possibilities) {
                        final JsonNode node = root.findValue(field);
                        if (node != null) {
                            reason = node.asText();
                            break;
                        }
                    }
                } catch (JsonProcessingException e) {
                    LOG.debug("Can't parse msg from response {}", fullHttpResponse);
                }
            }

            if (reason.isEmpty()) {
                for (String s : List.of("X-Platform-Error-Code", "X-Influx-Error", "X-InfluxDb-Error")) {
                    if (responseHeaders.contains(s.toLowerCase())) {
                        reason = responseHeaders.get(s.toLowerCase());
                        break;
                    }
                }
            }

            if (reason.isEmpty()) {
                reason = strContent;
            }

            if (reason.isEmpty()) {
                reason = HttpResponseStatus.valueOf(statusCode).reasonPhrase();
            }


            String message = String.format("HTTP status code: %d; Message: %s", statusCode, reason);

            throw new InfluxDBApiNettyException(message, responseHeaders, statusCode);
        }

        return fullHttpResponse;
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
        this.eventLoopGroup.shutdownGracefully();
        if (this.channel != null && this.channel.isOpen()) {
            this.channel.close();
        }
    }
}
