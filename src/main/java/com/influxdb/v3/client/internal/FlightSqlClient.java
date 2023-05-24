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

import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import javax.annotation.Nonnull;

import io.grpc.Metadata;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.HeaderCallOption;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.LocationSchemes;
import org.apache.arrow.flight.grpc.MetadataAdapter;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.VectorSchemaRoot;

import com.influxdb.v3.client.config.InfluxDBClientConfigs;

final class FlightSqlClient implements AutoCloseable {

    private final InfluxDBClientConfigs configs;
    private final Map<String, String> headers;

    private final org.apache.arrow.flight.sql.FlightSqlClient client;

    FlightSqlClient(@Nonnull final InfluxDBClientConfigs configs, @Nonnull final Map<String, String> headers) {
        Arguments.checkNotNull(configs, "configs");
        Arguments.checkNotNull(headers, "headers");

        this.configs = configs;
        this.headers = headers;

        Location location;
        try {
            location = new Location(configs.getHostUrl()
                    .replace("https", LocationSchemes.GRPC_TLS)
                    .replace("http", LocationSchemes.GRPC_INSECURE));
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }

        FlightClient flightClient = FlightClient.builder()
                .location(location)
                .allocator(new RootAllocator(Long.MAX_VALUE))
                .verifyServer(!configs.getDisableServerCertificateValidation())
                .build();

        this.client = new org.apache.arrow.flight.sql.FlightSqlClient(flightClient);
    }

    @Nonnull
    Stream<VectorSchemaRoot> execute(@Nonnull final String query, @Nonnull final String database) {

        MetadataAdapter metadata = new MetadataAdapter(new Metadata());
        if (configs.getAuthToken() != null && !configs.getAuthToken().isEmpty()) {
            metadata.insert("Authorization", "Bearer " + configs.getAuthToken());
        }

        metadata.insert("database", database);
        metadata.insert("bucket-name", database);
        headers.forEach(metadata::insert);

        HeaderCallOption headers = new HeaderCallOption(metadata);

        FlightInfo info = client.execute(query, headers);
        FlightSqlIterator iterator = new FlightSqlIterator(info.getEndpoints(), headers);

        Spliterator<VectorSchemaRoot> spliterator = Spliterators.spliteratorUnknownSize(iterator, Spliterator.NONNULL);
        return StreamSupport.stream(spliterator, false).onClose(iterator::close);
    }

    @Override
    public void close() throws Exception {
        client.close();
    }

    private final class FlightSqlIterator implements Iterator<VectorSchemaRoot>, AutoCloseable {

        private final HeaderCallOption headers;
        private final List<AutoCloseable> autoCloseable = new ArrayList<>();

        private final Iterator<FlightEndpoint> flightEndpoints;

        private FlightStream currentStream = null;
        private VectorSchemaRoot currentVectorSchemaRoot = null;

        private FlightSqlIterator(@Nonnull final List<FlightEndpoint> flightEndpoints,
                                  @Nonnull final HeaderCallOption headers) {
            this.flightEndpoints = flightEndpoints.iterator();
            this.headers = headers;
            loadNext();
        }

        @Override
        public boolean hasNext() {
            return currentVectorSchemaRoot != null;
        }

        @Override
        public VectorSchemaRoot next() {
            if (currentVectorSchemaRoot == null) {
                throw new NoSuchElementException();
            }

            VectorSchemaRoot oldVectorSchemaRoot = currentVectorSchemaRoot;
            loadNext();

            return oldVectorSchemaRoot;
        }

        @Override
        public void close() {
            try {
                AutoCloseables.close(autoCloseable);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        private void loadNext() {

            if (currentStream != null && currentStream.next()) {
                currentVectorSchemaRoot = currentStream.getRoot();
                autoCloseable.add(currentVectorSchemaRoot);

            } else if (flightEndpoints.hasNext()) {
                currentStream = client.getStream(flightEndpoints.next().getTicket(), headers);
                autoCloseable.add(currentStream);

                loadNext();
            } else {
                currentVectorSchemaRoot = null;
            }
        }
    }
}
