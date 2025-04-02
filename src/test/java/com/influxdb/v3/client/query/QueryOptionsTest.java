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
package com.influxdb.v3.client.query;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import javax.annotation.Nonnull;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.apache.arrow.flight.CallOption;
import org.apache.arrow.flight.CallOptions;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.NoOpFlightProducer;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.flight.impl.FlightServiceGrpc;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.influxdb.v3.client.InfluxDBClient;
import com.influxdb.v3.client.PointValues;
import com.influxdb.v3.client.config.ClientConfig;
import com.influxdb.v3.client.internal.GrpcCallOption;

class QueryOptionsTest {

    private ClientConfig.Builder configBuilder;

    @BeforeEach
    void before() {
        configBuilder = new ClientConfig.Builder()
                .host("http://localhost:8086")
                .token("my-token".toCharArray());
    }

    @Test
    void optionsOverrideAll() {
        ClientConfig config = configBuilder
                .database("my-database")
                .build();

        QueryOptions options = new QueryOptions("your-database", QueryType.InfluxQL);

        Assertions.assertThat(options.databaseSafe(config)).isEqualTo("your-database");
        Assertions.assertThat(options.queryTypeSafe()).isEqualTo(QueryType.InfluxQL);
    }

    @Test
    void optionsOverrideDatabase() {
        ClientConfig config = configBuilder
                .database("my-database")
                .build();

        QueryOptions options = new QueryOptions("your-database");

        Assertions.assertThat(options.databaseSafe(config)).isEqualTo("your-database");
        Assertions.assertThat(options.queryTypeSafe()).isEqualTo(QueryType.SQL);
    }

    @Test
    void optionsOverrideQueryType() {
        ClientConfig config = configBuilder
                .database("my-database")
                .build();

        QueryOptions options = new QueryOptions(QueryType.InfluxQL);

        Assertions.assertThat(options.databaseSafe(config)).isEqualTo("my-database");
        Assertions.assertThat(options.queryTypeSafe()).isEqualTo(QueryType.InfluxQL);
    }

    @Test
    void setInboundMessageSizeSmall() throws Exception {
        URI uri = URI.create("http://127.0.0.1:33333");
        int rowCount = 100;
        try (VectorSchemaRoot vectorSchemaRoot = generateVectorSchemaRoot(10, rowCount);
             BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
             FlightServer flightServer = simpleFlightServer(uri, allocator, simpleProducer(vectorSchemaRoot))
        ) {
            flightServer.start();

            String host = String.format("http://%s:%d", uri.getHost(), uri.getPort());
            ClientConfig.Builder builder = new ClientConfig.Builder()
                    .host(host)
                    .database("test");

            // Set very small message size for testing
            GrpcCallOption grpcCallOption = new GrpcCallOption.Builder()
                    .withMaxInboundMessageSize(200)
                    .build();

            QueryOptions queryOptions = new QueryOptions("test");
            queryOptions.setGrpcCallOption(grpcCallOption);

            try (InfluxDBClient influxDBClient = InfluxDBClient.getInstance(builder.build())) {
                try (Stream<PointValues> stream = influxDBClient.queryPoints(
                        "Select * from \"nothing\"",
                        queryOptions
                )) {
                    try {
                        Assertions.assertThatThrownBy(stream::count);
                    } catch (FlightRuntimeException e) {
                        Assertions.assertThat(e.status().code()).isEqualTo(CallStatus.RESOURCE_EXHAUSTED.code());
                    }
                }
            }
        }
    }

    @Test
    void setInboundMessageSizeLarge() throws Exception {
        URI uri = URI.create("http://127.0.0.1:33333");
        int rowCount = 100;
        try (VectorSchemaRoot vectorSchemaRoot = generateVectorSchemaRoot(10, rowCount);
             BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
             FlightServer flightServer = simpleFlightServer(uri, allocator, simpleProducer(vectorSchemaRoot))
        ) {
            flightServer.start();

            String host = String.format("http://%s:%d", uri.getHost(), uri.getPort());
            ClientConfig clientConfig = new ClientConfig.Builder()
                    .host(host)
                    .database("test")
                    .build();

            GrpcCallOption grpcCallOption = new GrpcCallOption.Builder()
                    .withMaxInboundMessageSize(1024 * 1024 * 1024)
                    .build();

            QueryOptions queryOptions = new QueryOptions("test");
            queryOptions.setGrpcCallOption(grpcCallOption);

            try (InfluxDBClient influxDBClient = InfluxDBClient.getInstance(clientConfig)) {
                Assertions.assertThatNoException().isThrownBy(() -> {
                    Stream<PointValues> stream = influxDBClient.queryPoints(
                            "Select * from \"nothing\"",
                            queryOptions);
                    Assertions.assertThat(stream.count()).isEqualTo(rowCount);
                    stream.close();
                });
            }
        }
    }

    @Test
    void grpcCallOption() {
        GrpcCallOption grpcCallOption = new GrpcCallOption.Builder()
                .withMaxInboundMessageSize(1024)
                .withMaxOutboundMessageSize(1024)
                .withCompressorName("my-compressor")
                .withWaitForReady()
                .build();
        ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", 3333)
                .usePlaintext()
                .build();
        FlightServiceGrpc.FlightServiceStub stub = FlightServiceGrpc.newStub(channel);
        for (CallOption option : grpcCallOption.getCallOptionCallback()) {
            stub = ((CallOptions.GrpcCallOption) option).wrapStub(stub);
        }

        io.grpc.CallOptions stubCallOptions = stub.getCallOptions();
        Assertions.assertThat(stubCallOptions.getMaxInboundMessageSize()).isEqualTo(grpcCallOption.getMaxInboundMessageSize());
        Assertions.assertThat(stubCallOptions.getMaxOutboundMessageSize()).isEqualTo(grpcCallOption.getMaxOutboundMessageSize());
        Assertions.assertThat(stubCallOptions.getCompressor()).isEqualTo(grpcCallOption.getCompressorName());
        Assertions.assertThat(stubCallOptions.isWaitForReady()).isEqualTo(grpcCallOption.getWaitForReady());
    }

    private FlightServer simpleFlightServer(@Nonnull final URI uri,
                                            @Nonnull final BufferAllocator allocator,
                                            @Nonnull final NoOpFlightProducer producer) throws Exception {
        Location location = Location.forGrpcInsecure(uri.getHost(), uri.getPort());
        return FlightServer.builder(allocator, location, producer).build();
    }

    private NoOpFlightProducer simpleProducer(@Nonnull final VectorSchemaRoot vectorSchemaRoot) {
        return new NoOpFlightProducer() {
            @Override
            public void getStream(final CallContext context,
                                  final Ticket ticket,
                                  final ServerStreamListener listener) {
                listener.start(vectorSchemaRoot);
                if (listener.isReady()) {
                    listener.putNext();
                }
                listener.completed();
            }
        };
    }

    private VectorSchemaRoot generateVectorSchemaRoot(final int fieldCount, final int rowCount) {
        List<Field> fields = new ArrayList<>();
        for (int i = 0; i < fieldCount; i++) {
            Field field = new Field("field" + i, FieldType.nullable(new ArrowType.Utf8()), null);
            fields.add(field);
        }

        Schema schema = new Schema(fields);
        VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.create(schema, new RootAllocator(Long.MAX_VALUE));
        for (Field field : fields) {
            VarCharVector vector = (VarCharVector) vectorSchemaRoot.getVector(field);
            vector.allocateNew(rowCount);
            for (int i = 0; i < rowCount; i++) {
                vector.set(i, "Value".getBytes(StandardCharsets.UTF_8));
            }
        }
        vectorSchemaRoot.setRowCount(rowCount);

        return vectorSchemaRoot;
    }
}
