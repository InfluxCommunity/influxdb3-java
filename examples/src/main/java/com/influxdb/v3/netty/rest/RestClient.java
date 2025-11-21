package com.influxdb.v3.netty.rest;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.influxdb.v3.client.InfluxDBApiHttpException;
import com.influxdb.v3.client.config.ClientConfig;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.*;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.util.AsciiString;
import io.netty.util.CharsetUtil;
import io.netty.util.concurrent.Promise;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.net.ssl.SSLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public final class RestClient implements AutoCloseable {

    private Channel channel;

    private final EventLoopGroup eventLoopGroup;

    private final Promise<FullHttpResponse> promise;

    private final Map<AsciiString, String> defaultHeader = new HashMap<>();

    private final Integer port;

    private final String host;

    private SslContext sslCtx;

    private final ObjectMapper objectMapper = new ObjectMapper();

    private static final Logger LOG = LoggerFactory.getLogger(com.influxdb.v3.netty.rest.RestClient.class);

    public RestClient(ClientConfig config) throws URISyntaxException, SSLException {
        URI uri = new URI(config.getHost());
        String scheme = uri.getScheme() == null ? "http" : uri.getScheme();
        int port = uri.getPort();
        if (port == -1) {
            if ("http" .equalsIgnoreCase(scheme)) {
                port = 80;
            } else if ("https" .equalsIgnoreCase(scheme)) {
                port = 443;
            }
        }
        this.port = port;
        this.host = uri.getHost();

        if ("https" .equalsIgnoreCase(scheme)) {
            this.sslCtx = SslContextBuilder.forClient().build();
        }

        this.eventLoopGroup = new MultiThreadIoEventLoopGroup(NioIoHandler.newFactory());

        this.promise = this.eventLoopGroup.next().newPromise();

        Map<AsciiString, String> header = this.defaultHeader;
        header.put(HttpHeaderNames.HOST, "us-east-1-1.aws.cloud2.influxdata.com");
        header.put(HttpHeaderNames.AUTHORIZATION, String.format("Token %s", new String(config.getToken())));
        header.put(HttpHeaderNames.ACCEPT, "*/*");
        header.put(HttpHeaderNames.USER_AGENT, "influxdb3-java/unknown");
        header.put(HttpHeaderNames.CONNECTION, "close");

        this.channel = getChannel();
    }

    public Channel getChannel() {
        Bootstrap b = new Bootstrap();
        return b.group(this.eventLoopGroup)
                .channel(NioSocketChannel.class)
                .handler(new ClientChannelInitializer(this.host, this.port, this.promise, this.sslCtx))
                .remoteAddress(this.host, this.port)
                .connect()
                .syncUninterruptibly()
                .channel();
    }

    public String getServerVersion() throws InterruptedException, ExecutionException, JsonProcessingException {
        FullHttpResponse response = this.request(HttpMethod.GET, "/ping", null, null);

        String version = response.headers().get("x-influxdb-version");
        if (version == null) {
            return "unknown";
        }
        //fixme get version from the body
        return version;
    }

    public void write(String lineProtocol) throws InterruptedException, ExecutionException, JsonProcessingException {
        var header = new HashMap<AsciiString, String>();
        header.put(HttpHeaderNames.CONTENT_TYPE, "text/plain; charset=utf-8");

        header.put(HttpHeaderNames.CONTENT_LENGTH, String.valueOf(lineProtocol.getBytes(StandardCharsets.UTF_8).length));
        header.putAll(this.defaultHeader);

        QueryStringEncoder encoder = new QueryStringEncoder("/api/v2/write");
        encoder.addParam("bucket", "bucket0");
        encoder.addParam("precision", "ns");

        this.request(HttpMethod.POST, encoder.toString(), header, lineProtocol);
    }

    public FullHttpResponse request(@Nonnull HttpMethod method, @Nonnull String path, @Nullable Map<AsciiString, String> header, @Nullable String body) throws InterruptedException, ExecutionException, JsonProcessingException {
        var content = Unpooled.EMPTY_BUFFER;
        if (body != null) {
            content = Unpooled.copiedBuffer(body, CharsetUtil.UTF_8);
        }
        HttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, method, path, content);
        this.defaultHeader.forEach(request.headers()::set);
        if (header != null) {
            header.forEach(request.headers()::set);
        }

        if (!this.channel.isOpen()) {
            this.channel = getChannel();
        }

        this.channel.writeAndFlush(request).channel().closeFuture().sync();
        FullHttpResponse fullHttpResponse = this.promise.get();

        int statusCode = fullHttpResponse.status().code();
        if (statusCode < 200 || statusCode >= 300) {
            String reason = "";
            var jsonString = fullHttpResponse.content().toString(CharsetUtil.UTF_8);
            if (!jsonString.isEmpty()) {
                try {
                    final JsonNode root = objectMapper.readTree(jsonString);
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
                    if (fullHttpResponse.headers().contains(s.toLowerCase())) {
                        reason = fullHttpResponse.headers().get(s);
                        break;
                    }
                }
            }

//            if (reason.isEmpty()) {
//                reason = body;
//            }

            if (reason.isEmpty()) {
                reason = HttpResponseStatus.valueOf(statusCode).reasonPhrase();
            }
            String message = String.format("HTTP status code: %d; Message: %s", statusCode, reason);
            throw new InfluxDBApiHttpException(message, null, statusCode);
        }

        return fullHttpResponse;
    }

    @Override
    public void close() {
        channel.close();
        eventLoopGroup.shutdownGracefully();
    }
}


