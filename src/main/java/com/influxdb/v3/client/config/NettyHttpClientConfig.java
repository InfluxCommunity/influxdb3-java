package com.influxdb.v3.client.config;

import io.grpc.ProxyDetector;
import io.netty.handler.proxy.HttpProxyHandler;
import io.netty.handler.ssl.SslContext;

import java.util.function.Supplier;

// fixme refactor
public class NettyHttpClientConfig {

    private SslContext sslContext;

    private HttpProxyHandler httpProxyHandler;

    private ProxyDetector proxyDetector;

    public NettyHttpClientConfig() {
    }

    public void configureSsl(Supplier<SslContext> configureSsl) {
        this.sslContext = configureSsl.get();
    }

    public void configureChannelProxy(Supplier<HttpProxyHandler> configureHttpProxyHandler) {
        this.httpProxyHandler = configureHttpProxyHandler.get();
    }

    public void configureManagedChannelProxy(Supplier<ProxyDetector> configureManagedChannelProxy) {
        this.proxyDetector = configureManagedChannelProxy.get();
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
