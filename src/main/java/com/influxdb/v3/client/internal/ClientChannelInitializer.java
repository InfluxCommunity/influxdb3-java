package com.influxdb.v3.client.internal;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.proxy.HttpProxyHandler;
import io.netty.handler.proxy.ProxyHandler;
import io.netty.handler.ssl.SslContext;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class ClientChannelInitializer extends ChannelInitializer<SocketChannel> {

    private final SslContext sslCtx;

//    private final Promise<FullHttpResponse> promise;

    private final String host;

    private final Integer port;

    private final ProxyHandler proxyHandler;

    private ChannelHandler[] h;

    public ClientChannelInitializer(@Nonnull String host,
                                    @Nonnull Integer port,
                                    @Nullable SslContext sslCtx,
                                    @Nullable HttpProxyHandler proxyHandler,
                                    ChannelHandler... handlers

    ) {
        this.sslCtx = sslCtx;
        this.host = host;
        this.port = port;
        this.proxyHandler = proxyHandler;
        this.h = handlers;
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
        if (h != null) {
            for (ChannelHandler handler : h) {
                p.addLast(handler);
            }
        }
    }
}
