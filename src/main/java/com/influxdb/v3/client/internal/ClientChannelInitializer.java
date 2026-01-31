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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.oio.OioSocketChannel;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.proxy.HttpProxyHandler;
import io.netty.handler.proxy.ProxyHandler;
import io.netty.handler.ssl.SslContext;

public class ClientChannelInitializer extends ChannelInitializer<OioSocketChannel> {

    private final SslContext sslCtx;

    private final String host;

    private final Integer port;

    private final ProxyHandler proxyHandler;

    private ChannelHandler[] h;

    public ClientChannelInitializer(@Nonnull final String host,
                                    @Nonnull final Integer port,
                                    @Nullable final SslContext sslCtx,
                                    @Nullable final HttpProxyHandler proxyHandler,
                                    final ChannelHandler... handlers

    ) {
        this.sslCtx = sslCtx;
        this.host = host;
        this.port = port;
        this.proxyHandler = proxyHandler;
        this.h = handlers;
    }

    @Override
    public void initChannel(final OioSocketChannel ch) {
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
