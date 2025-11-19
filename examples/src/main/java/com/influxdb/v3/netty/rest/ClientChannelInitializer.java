package com.influxdb.v3.netty.rest;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.ssl.SslContext;
import io.netty.util.concurrent.Promise;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class ClientChannelInitializer extends ChannelInitializer<SocketChannel> {

    private final SslContext sslCtx;

    private final Promise<FullHttpResponse> promise;

    private final String host;

    private final Integer port;

    public ClientChannelInitializer(@Nonnull String host, @Nonnull Integer port, @Nonnull Promise<FullHttpResponse> promise, @Nullable SslContext sslCtx) {
        this.sslCtx = sslCtx;
        this.promise = promise;
        this.host = host;
        this.port = port;
    }

    @Override
    public void initChannel(SocketChannel ch) {
        ChannelPipeline p = ch.pipeline();
        if (sslCtx != null) {
            p.addLast("ssl", sslCtx.newHandler(ch.alloc(), host, port));
        }
        p.addLast(new HttpClientCodec());
        p.addLast(new HttpObjectAggregator(1048576));
        p.addLast(new ClientHandler(this.promise));
    }
}
