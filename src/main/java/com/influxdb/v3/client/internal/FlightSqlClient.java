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
import java.net.InetSocketAddress;
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
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import org.apache.arrow.flight.CallOption;
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
                                     @Nonnull final Map<String, String> headers,
                                     final CallOption... callOptions) {

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
        CallOption[] callOptionArray = GrpcCallOptions.mergeCallOptions(callOptions, headerCallOption);

        Ticket ticket = new Ticket(json.getBytes(StandardCharsets.UTF_8));
        FlightStream stream = client.getStream(ticket, callOptionArray);
        FlightSqlIterator iterator = new FlightSqlIterator(stream);

        Spliterator<VectorSchemaRoot> spliterator = Spliterators.spliteratorUnknownSize(iterator, Spliterator.NONNULL);
        return StreamSupport.stream(spliterator, false).onClose(iterator::close);
    }

    @Override
    public void close() throws Exception {
        client.close();
    }

    @Nonnull
    private FlightClient createFlightClient(@Nonnull final ClientConfig config) {
        URI uri = createLocation(config).getUri();
        final NettyChannelBuilder nettyChannelBuilder = NettyChannelBuilder.forAddress(uri.getHost(), uri.getPort());

        if (LocationSchemes.GRPC_TLS.equals(uri.getScheme())) {
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
            LOG.warn("proxy property in ClientConfig will not work in query api, use proxyUrl property instead");
        }

        nettyChannelBuilder.maxTraceEvents(0)
                .maxInboundMetadataSize(Integer.MAX_VALUE);

        return FlightGrpcUtils.createFlightClient(new RootAllocator(Long.MAX_VALUE), nettyChannelBuilder.build());
    }

    @Nonnull
    SslContext createNettySslContext(@Nonnull final ClientConfig config) {
        try {
            SslContextBuilder sslContextBuilder;
            sslContextBuilder = GrpcSslContexts.forClient();
            if (config.getDisableServerCertificateValidation()) {
                sslContextBuilder.trustManager(InsecureTrustManagerFactory.INSTANCE);
            } else if (config.sslRootsFilePath() != null) {
                try (FileInputStream fileInputStream = new FileInputStream(config.sslRootsFilePath())) {
                    sslContextBuilder.trustManager(fileInputStream);
                }
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

    ProxyDetector createProxyDetector(@Nonnull final String targetUrl, @Nonnull final String proxyUrl) {
        URI targetUri = URI.create(targetUrl);
        URI proxyUri = URI.create(proxyUrl);
        return (targetServerAddress) -> {
            InetSocketAddress targetAddress = (InetSocketAddress) targetServerAddress;
            if (targetUri.getHost().equals(targetAddress.getHostString())
                    && targetUri.getPort() == targetAddress.getPort()) {
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
