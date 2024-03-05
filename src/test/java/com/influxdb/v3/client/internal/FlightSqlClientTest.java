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
import java.util.Map;

import io.grpc.internal.GrpcUtil;
import org.apache.arrow.flight.CallHeaders;
import org.apache.arrow.flight.CallInfo;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightClientMiddleware;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.NoOpFlightProducer;
import org.apache.arrow.memory.RootAllocator;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.influxdb.v3.client.config.ClientConfig;
import com.influxdb.v3.client.query.QueryType;

public class FlightSqlClientTest {

    private static final String LOCALHOST = "localhost";
    private final Location grpcLocation = Location.forGrpcInsecure(LOCALHOST, 0);
    private final String serverLocation = String.format("http://%s:%d", LOCALHOST, grpcLocation.getUri().getPort());

    private final CallHeadersMiddleware callHeadersMiddleware = new CallHeadersMiddleware();

    private RootAllocator allocator;
    private FlightServer server;
    private FlightClient client;

    @BeforeEach
    void reset() {
        callHeadersMiddleware.headers = null;
    }

    @BeforeEach
    void setUp() throws Exception {
        allocator = new RootAllocator(Long.MAX_VALUE);
        server = FlightServer.builder(allocator, grpcLocation, new NoOpFlightProducer()).build().start();
        client = FlightClient.builder(allocator, server.getLocation()).intercept(callHeadersMiddleware).build();
        callHeadersMiddleware.headers = null;
    }

    @AfterEach
    void tearDown() throws Exception {
        if (client != null) {
            client.close();
        }
        if (server != null) {
            server.shutdown();
            server.awaitTermination();
        }
        if (allocator != null) {
            allocator.close();
        }
    }

    @Test
    public void invalidHost() {
        ClientConfig clientConfig = new ClientConfig.Builder()
                .host("xyz://a bc")
                .token("my-token".toCharArray())
                .build();

        Assertions.assertThatThrownBy(() -> {
                    try (FlightSqlClient ignored = new FlightSqlClient(clientConfig)) {
                        Assertions.fail("Should not be here");
                    }
                })
                .isInstanceOf(RuntimeException.class)
                .hasCauseInstanceOf(URISyntaxException.class)
                .hasMessageContaining("xyz://a bc");
    }

    @Test
    public void callHeaders() throws Exception {
        ClientConfig clientConfig = new ClientConfig.Builder()
                .host(serverLocation)
                .token("my-token".toCharArray())
                .build();

        try (FlightSqlClient flightSqlClient = new FlightSqlClient(clientConfig, client)) {

            flightSqlClient.execute("select * from cpu", "mydb", QueryType.SQL, Map.of());

            final CallHeaders incomingHeaders = callHeadersMiddleware.headers;

            Assertions.assertThat(incomingHeaders.keys()).containsOnly(
                    "authorization",
                    GrpcUtil.MESSAGE_ACCEPT_ENCODING
            );
            Assertions.assertThat(incomingHeaders.get("authorization")).isEqualTo("Bearer my-token");
        }
    }

    @Test
    public void callHeadersWithoutToken() throws Exception {
        ClientConfig clientConfig = new ClientConfig.Builder()
                .host(serverLocation)
                .build();

        try (FlightSqlClient flightSqlClient = new FlightSqlClient(clientConfig, client)) {

            flightSqlClient.execute("select * from cpu", "mydb", QueryType.SQL, Map.of());

            final CallHeaders incomingHeaders = callHeadersMiddleware.headers;

            Assertions.assertThat(incomingHeaders.keys()).containsOnly(GrpcUtil.MESSAGE_ACCEPT_ENCODING);
            Assertions.assertThat(incomingHeaders.get("authorization")).isNull();
        }
    }

    @Test
    public void callHeadersEmptyToken() throws Exception {
        ClientConfig clientConfig = new ClientConfig.Builder()
                .host(serverLocation)
                .token("".toCharArray())
                .build();

        try (FlightSqlClient flightSqlClient = new FlightSqlClient(clientConfig, client)) {

            flightSqlClient.execute("select * from cpu", "mydb", QueryType.SQL, Map.of());

            final CallHeaders incomingHeaders = callHeadersMiddleware.headers;

            Assertions.assertThat(incomingHeaders.keys()).containsOnly(GrpcUtil.MESSAGE_ACCEPT_ENCODING);
            Assertions.assertThat(incomingHeaders.get("authorization")).isNull();
        }
    }

    @Test
    public void callHeadersCustomHeader() throws Exception {
        ClientConfig clientConfig = new ClientConfig.Builder()
                .host(serverLocation)
                .token("my-token".toCharArray())
                .headers(Map.of("X-Tracing-Id", "123"))
                .build();

        try (FlightSqlClient flightSqlClient = new FlightSqlClient(clientConfig, client)) {

            flightSqlClient.execute("select * from cpu", "mydb", QueryType.SQL, Map.of());

            final CallHeaders incomingHeaders = callHeadersMiddleware.headers;

            Assertions.assertThat(incomingHeaders.keys()).containsOnly(
                    "authorization",
                    "x-tracing-id",
                    GrpcUtil.MESSAGE_ACCEPT_ENCODING
            );
            Assertions.assertThat(incomingHeaders.get("X-Tracing-Id")).isEqualTo("123");
        }
    }

    static class CallHeadersMiddleware implements FlightClientMiddleware.Factory {
        CallHeaders headers;

        @Override
        public FlightClientMiddleware onCallStarted(final CallInfo info) {
            return new FlightClientMiddleware() {
                @Override
                public void onBeforeSendingHeaders(final CallHeaders outgoingHeaders) {
                    headers = outgoingHeaders;
                }

                @Override
                public void onHeadersReceived(final CallHeaders incomingHeaders) {
                }

                @Override
                public void onCallCompleted(final CallStatus status) {
                }
            };
        }
    }
}
