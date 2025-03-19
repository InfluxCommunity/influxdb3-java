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

import java.io.FileInputStream;
import java.lang.reflect.InvocationTargetException;
import java.net.InetSocketAddress;
import java.net.ProxySelector;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.grpc.HttpConnectProxiedSocketAddress;
import io.grpc.Metadata;
import io.grpc.ProxyDetector;
import io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.NettyChannelBuilder;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightGrpcUtils;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.HeaderCallOption;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.LocationSchemes;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.flight.grpc.MetadataAdapter;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.influxdb.v3.client.config.ClientConfig;
import com.influxdb.v3.client.query.QueryType;

final class FlightSqlClient implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(FlightSqlClient.class);

    private final FlightClient client;

    private final Map<String, String> defaultHeaders = new HashMap<>();
    private final ObjectMapper objectMapper = new ObjectMapper();

    FlightSqlClient(@Nonnull final ClientConfig config) {
        this(config, null);
    }

    /**
     * Constructor for testing purposes.
     *
     * @param config the client configuration
     * @param client the flight client, if null a new client will be created
     */
    FlightSqlClient(@Nonnull final ClientConfig config, @Nullable final FlightClient client) {
        Arguments.checkNotNull(config, "config");

        if (config.getToken() != null && config.getToken().length > 0) {
            defaultHeaders.put("Authorization", "Bearer " + new String(config.getToken()));
        }

        defaultHeaders.put("User-Agent", Identity.getUserAgent());

        if (config.getHeaders() != null) {
            defaultHeaders.putAll(config.getHeaders());
        }

        this.client = (client != null) ? client : createFlightClient(config);
    }

    @Nonnull
    Stream<VectorSchemaRoot> execute(@Nonnull final String query,
                                     @Nonnull final String database,
                                     @Nonnull final QueryType queryType,
                                     @Nonnull final Map<String, Object> queryParameters,
                                     @Nonnull final Map<String, String> headers) {

        Map<String, Object> ticketData = new HashMap<>() {{
            put("database", database);
            put("sql_query", query);
            put("query_type", queryType.name().toLowerCase());
        }};

        if (!queryParameters.isEmpty()) {
            ticketData.put("params", queryParameters);
        }

        String json;
        try {
            json = objectMapper.writeValueAsString(ticketData);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }

        HeaderCallOption headerCallOption = metadataHeader(headers);
        Ticket ticket = new Ticket(json.getBytes(StandardCharsets.UTF_8));
        FlightStream stream = client.getStream(ticket, headerCallOption);
        FlightSqlIterator iterator = new FlightSqlIterator(stream);

        Spliterator<VectorSchemaRoot> spliterator = Spliterators.spliteratorUnknownSize(iterator, Spliterator.NONNULL);
        return StreamSupport.stream(spliterator, false).onClose(iterator::close);
    }

    @Override
    public void close() throws Exception {
        client.close();
    }

    @Nonnull
    FlightClient createFlightClient(@Nonnull final ClientConfig config) {
        Location location = createLocation(config);

        final NettyChannelBuilder nettyChannelBuilder = NettyChannelBuilder.forTarget(location.getUri().getHost());
        var validSchemas = List.of(
                LocationSchemes.GRPC,
                LocationSchemes.GRPC_INSECURE,
                LocationSchemes.GRPC_TLS,
                LocationSchemes.GRPC_DOMAIN_SOCKET
        );
        if (!validSchemas.contains(location.getUri().getScheme())) {
            throw new IllegalArgumentException(
                    "Scheme is not supported: " + location.getUri().getScheme());
        }

        if (location.getUri().getScheme().equals(LocationSchemes.GRPC_DOMAIN_SOCKET)) {
            setChannelTypeAndEventLoop(nettyChannelBuilder);
        }

        if (LocationSchemes.GRPC_TLS.equals(location.getUri().getScheme())) {
            nettyChannelBuilder.useTransportSecurity();

            SslContext nettySslContext = createNettySslContext(config);
            nettyChannelBuilder.sslContext(nettySslContext);
        } else {
            nettyChannelBuilder.usePlaintext();
        }

        if (config.getProxyUrl() != null) {
            ProxyDetector proxyDetector = createProxyDetector(config.getHost(), config.getProxyUrl());
            nettyChannelBuilder.proxyDetector(proxyDetector);
        }

        if (config.getProxy() != null) {
            LOG.warn("proxy property will not work in query api, use proxyUrl property instead");
        }

        nettyChannelBuilder.maxTraceEvents(0)
                .maxInboundMessageSize(Integer.MAX_VALUE)
                .maxInboundMetadataSize(Integer.MAX_VALUE);

        return FlightGrpcUtils.createFlightClient(new RootAllocator(Long.MAX_VALUE), nettyChannelBuilder.build());
    }

    @Nonnull
    SslContext createNettySslContext(@Nonnull final ClientConfig config) {
        try {
            SslContextBuilder sslContextBuilder;
            sslContextBuilder = GrpcSslContexts.forClient();
            if (!config.getDisableServerCertificateValidation()) {
                if (config.certificateFilePath() != null) {
                    try (FileInputStream fileInputStream = new FileInputStream(config.certificateFilePath())) {
                        sslContextBuilder.trustManager(fileInputStream);
                    }
                }
            } else {
                sslContextBuilder.trustManager(InsecureTrustManagerFactory.INSTANCE);
            }
            return sslContextBuilder.build();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Nonnull
    private Location createLocation(@Nonnull final ClientConfig config) {
        try {
            URI uri = new URI(config.getHost());
            if ("https".equals(uri.getScheme())) {
                return Location.forGrpcTls(uri.getHost(), uri.getPort() != -1 ? uri.getPort() : 443);
            } else {
                return Location.forGrpcInsecure(uri.getHost(), uri.getPort() != -1 ? uri.getPort() : 80);
            }
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    @Nonnull
    private HeaderCallOption metadataHeader(@Nonnull final Map<String, String> requestHeaders) {
        MetadataAdapter metadata = new MetadataAdapter(new Metadata());
        for (Map.Entry<String, String> entry : requestHeaders.entrySet()) {
            metadata.insert(entry.getKey(), entry.getValue());
        }

        for (Map.Entry<String, String> entry : defaultHeaders.entrySet()) {
            if (!metadata.containsKey(entry.getKey())) {
                metadata.insert(entry.getKey(), entry.getValue());
            }
        }
        return new HeaderCallOption(metadata);
    }

    private void setChannelTypeAndEventLoop(@Nonnull final NettyChannelBuilder nettyChannelBuilder) {
        // The implementation is platform-specific, so we have to find the classes at runtime
        try {
            try {
                // Linux
                nettyChannelBuilder.channelType(
                        Class.forName("io.netty.channel.epoll.EpollDomainSocketChannel")
                                .asSubclass(ServerChannel.class));
                final EventLoopGroup elg =
                        Class.forName("io.netty.channel.epoll.EpollEventLoopGroup")
                                .asSubclass(EventLoopGroup.class)
                                .getDeclaredConstructor()
                                .newInstance();
                nettyChannelBuilder.eventLoopGroup(elg);
            } catch (ClassNotFoundException e) {
                // BSD
                nettyChannelBuilder.channelType(
                        Class.forName("io.netty.channel.kqueue.KQueueDomainSocketChannel")
                                .asSubclass(ServerChannel.class));
                final EventLoopGroup elg =
                        Class.forName("io.netty.channel.kqueue.KQueueEventLoopGroup")
                                .asSubclass(EventLoopGroup.class)
                                .getDeclaredConstructor()
                                .newInstance();
                nettyChannelBuilder.eventLoopGroup(elg);
            }
        } catch (ClassNotFoundException
                 | InstantiationException
                 | IllegalAccessException
                 | NoSuchMethodException
                 | InvocationTargetException e) {
            throw new UnsupportedOperationException(
                    "Could not find suitable Netty native transport implementation for domain socket address.");
        }
    }

    private ProxyDetector createProxyDetector(@Nonnull final String hostUrl, @Nonnull final String proxyUrl) {
        URI proxyUri = URI.create(proxyUrl);
        URI hostUri = URI.create(hostUrl);
        return (targetServerAddress) -> {
            InetSocketAddress targetAddress = (InetSocketAddress) targetServerAddress;
            if (hostUri.getHost().equals(targetAddress.getHostString())) {
                return HttpConnectProxiedSocketAddress.newBuilder()
                        .setProxyAddress(new InetSocketAddress(proxyUri.getHost(), proxyUri.getPort()))
                        .setTargetAddress(targetAddress)
                        .build();
            }
            return null;
        };
    }

    private static final class FlightSqlIterator implements Iterator<VectorSchemaRoot>, AutoCloseable {

        private final List<AutoCloseable> autoCloseable = new ArrayList<>();

        private final FlightStream flightStream;

        private FlightSqlIterator(@Nonnull final FlightStream flightStream) {
            this.flightStream = flightStream;
        }

        @Override
        public boolean hasNext() {
            return flightStream.next();
        }

        @Override
        public VectorSchemaRoot next() {
            if (flightStream.getRoot() == null) {
                throw new NoSuchElementException();
            }

            autoCloseable.add(flightStream.getRoot());

            return flightStream.getRoot();
        }

        @Override
        public void close() {
            try {
                AutoCloseables.close(autoCloseable);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
}
