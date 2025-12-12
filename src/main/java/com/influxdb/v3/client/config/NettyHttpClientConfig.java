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
package com.influxdb.v3.client.config;

import java.util.function.Supplier;
import javax.annotation.Nonnull;

import io.grpc.ProxyDetector;
import io.netty.handler.proxy.HttpProxyHandler;
import io.netty.handler.ssl.SslContext;

// fixme refactor
public class NettyHttpClientConfig {

    private SslContext sslContext;

    private HttpProxyHandler httpProxyHandler;

    private ProxyDetector proxyDetector;

    public NettyHttpClientConfig() {
    }

    public void configureSsl(@Nonnull final Supplier<SslContext> configureSsl) {
        this.sslContext = configureSsl.get();
    }

    public void configureChannelProxy(@Nonnull final Supplier<HttpProxyHandler> configureHttpProxyHandler) {
        this.httpProxyHandler = configureHttpProxyHandler.get();
    }

    public void configureManagedChannelProxy(@Nonnull final ProxyDetector configureManagedChannelProxy) {
        this.proxyDetector = configureManagedChannelProxy;
    }

    public SslContext getSslContext() {
        return sslContext;
    }

    public ProxyDetector getProxyDetector() {
        return proxyDetector;
    }

    public HttpProxyHandler getHttpProxyHandler() {
        return httpProxyHandler;
    }

}
