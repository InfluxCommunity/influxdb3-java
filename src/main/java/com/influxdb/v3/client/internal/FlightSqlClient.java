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
import io.grpc.Metadata;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.HeaderCallOption;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.flight.grpc.MetadataAdapter;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.VectorSchemaRoot;

import com.influxdb.v3.client.config.ClientConfig;
import com.influxdb.v3.client.query.QueryType;

final class FlightSqlClient implements AutoCloseable {

    private final HeaderCallOption headers;
    private final FlightClient client;

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

        MetadataAdapter metadata = new MetadataAdapter(new Metadata());
        if (config.getToken() != null && config.getToken().length > 0) {
            metadata.insert("Authorization", "Bearer " + new String(config.getToken()));
        }
        if (config.getHeaders() != null) {
            for (Map.Entry<String, String> entry : config.getHeaders().entrySet()) {
                metadata.insert(entry.getKey(), entry.getValue());
            }
        }

        this.headers = new HeaderCallOption(metadata);
        this.client = (client != null) ? client : createFlightClient(config);
    }

    @Nonnull
    Stream<VectorSchemaRoot> execute(@Nonnull final String query,
                                     @Nonnull final String database,
                                     @Nonnull final QueryType queryType,
                                     @Nonnull final Map<String, Object> queryParameters) {

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

        Ticket ticket = new Ticket(json.getBytes(StandardCharsets.UTF_8));
        FlightStream stream = client.getStream(ticket, headers);
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
        Location location = createLocation(config);

        return FlightClient.builder()
                .location(location)
                .allocator(new RootAllocator(Long.MAX_VALUE))
                .verifyServer(!config.getDisableServerCertificateValidation())
                .build();
    }

    private static Location createLocation(@Nonnull final ClientConfig config) {
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
