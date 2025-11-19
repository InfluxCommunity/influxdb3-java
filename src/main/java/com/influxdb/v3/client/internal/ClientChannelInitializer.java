package com.influxdb.v3.client.internal;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.proxy.HttpProxyHandler;
import io.netty.handler.proxy.ProxyHandler;
import io.netty.handler.ssl.SslContext;
import io.netty.util.concurrent.Promise;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class ClientChannelInitializer extends ChannelInitializer<SocketChannel> {

    private final SslContext sslCtx;

    private final Promise<FullHttpResponse> promise;

    private final String host;

    private final Integer port;

    private final ProxyHandler proxyHandler;

    public ClientChannelInitializer(@Nonnull String host,
                                    @Nonnull Integer port,
                                    @Nonnull Promise<FullHttpResponse> promise,
                                    @Nullable SslContext sslCtx,
                                    @Nullable HttpProxyHandler proxyHandler
    ) {
        this.sslCtx = sslCtx;
        this.promise = promise;
        this.host = host;
        this.port = port;
        this.proxyHandler = proxyHandler;
    }

    @Override
    public void initChannel(SocketChannel ch) {
        ChannelPipeline p = ch.pipeline();
        p.addLast(new LoggingHandler(LogLevel.INFO));
        if (proxyHandler != null) {
            p.addLast(proxyHandler);
        }
        if (sslCtx != null) {
            p.addLast("ssl", sslCtx.newHandler(ch.alloc(), host, port));
        }
        p.addLast(new HttpClientCodec());
        p.addLast(new HttpObjectAggregator(1048576));
        p.addLast(new ClientHandler(this.promise));
    }
}
