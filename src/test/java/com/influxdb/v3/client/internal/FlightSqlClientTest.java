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

import java.net.InetSocketAddress;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import io.grpc.HttpConnectProxiedSocketAddress;
import io.grpc.ProxyDetector;
import io.grpc.internal.GrpcUtil;
import org.apache.arrow.flight.CallHeaders;
import org.apache.arrow.flight.CallInfo;
import org.apache.arrow.flight.CallOption;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.FlightServerMiddleware;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.RequestContext;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.influxdb.v3.client.InfluxDBClient;
import com.influxdb.v3.client.TestUtils;
import com.influxdb.v3.client.config.ClientConfig;
import com.influxdb.v3.client.query.QueryOptions;
import com.influxdb.v3.client.query.QueryType;

public class FlightSqlClientTest {

    private static final String LOCALHOST = "localhost";
    private static final FlightServerMiddleware.Key<HeaderCaptureMiddleware> HEADER_CAPTURE_KEY =
        FlightServerMiddleware.Key.of("header-capture");
    private final Location grpcLocation = Location.forGrpcInsecure(LOCALHOST, 0);
    private final HeaderCaptureMiddlewareFactory headerFactory = new HeaderCaptureMiddlewareFactory();
    private final int rowCount = 10;
    private final VectorSchemaRoot vectorSchemaRoot = TestUtils.generateVectorSchemaRoot(10, rowCount);


    private RootAllocator allocator;
    private FlightServer server;

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
            .host(server.getLocation().getUri().toString())
            .token("my-token".toCharArray())
            .build();

        try (FlightSqlClient flightSqlClient = new FlightSqlClient(clientConfig);
            var data = executeQuery(flightSqlClient, Map.of(), Map.of())) {

            Assertions.assertThat(data.count()).isEqualTo(rowCount);

            final Map<String, String> receivedHeaders = headerFactory.getLastInstance().getHeaders();

            Assertions.assertThat(receivedHeaders.keySet()).contains(
                "authorization",
                "user-agent",
                GrpcUtil.MESSAGE_ACCEPT_ENCODING
            );
            Assertions.assertThat(receivedHeaders.get("authorization")).isEqualTo("Bearer my-token");
            Assertions.assertThat(receivedHeaders.get("user-agent")).startsWith(Identity.getUserAgent());
        }
    }

    @Test
    public void callHeadersWithoutToken() throws Exception {
        ClientConfig clientConfig = new ClientConfig.Builder()
            .host(server.getLocation().getUri().toString())
            .build();

        try (FlightSqlClient flightSqlClient = new FlightSqlClient(clientConfig);
            var data = executeQuery(flightSqlClient, Map.of(), Map.of())) {

            Assertions.assertThat(data.count()).isEqualTo(rowCount);

            final Map<String, String> receivedHeaders = headerFactory.getLastInstance().getHeaders();

            Assertions.assertThat(receivedHeaders.keySet()).containsOnly(
                "user-agent",
                GrpcUtil.MESSAGE_ACCEPT_ENCODING,
                "content-type"
            );
            Assertions.assertThat(receivedHeaders.get("authorization")).isNull();
        }
    }

    @Test
    public void callHeadersEmptyToken() throws Exception {
        ClientConfig clientConfig = new ClientConfig.Builder()
            .host(server.getLocation().getUri().toString())
            .token("".toCharArray())
            .build();

        try (FlightSqlClient flightSqlClient = new FlightSqlClient(clientConfig);
            var data = executeQuery(flightSqlClient, Map.of(), Map.of())) {

            Assertions.assertThat(data.count()).isEqualTo(rowCount);

            final Map<String, String> receivedHeaders = headerFactory.getLastInstance().getHeaders();
            Assertions.assertThat(receivedHeaders.keySet()).containsOnly(
                "user-agent",
                GrpcUtil.MESSAGE_ACCEPT_ENCODING,
                "content-type"
            );
            Assertions.assertThat(receivedHeaders.get("authorization")).isNull();
        }
    }

    @Test
    public void callHeadersCustomHeader() throws Exception {
        ClientConfig clientConfig = new ClientConfig.Builder()
            .host(server.getLocation().getUri().toString())
            .token("my-token".toCharArray())
            .headers(Map.of("X-Tracing-Id", "123"))
            .build();

        try (FlightSqlClient flightSqlClient = new FlightSqlClient(clientConfig);
            var data = executeQuery(flightSqlClient, Map.of(), Map.of())) {

            Assertions.assertThat(data.count()).isEqualTo(rowCount);

            final Map<String, String> receivedHeaders = headerFactory.getLastInstance().getHeaders();

            Assertions.assertThat(receivedHeaders.keySet()).contains(
                "authorization",
                "user-agent",
                "x-tracing-id",
                GrpcUtil.MESSAGE_ACCEPT_ENCODING
            );
            Assertions.assertThat(receivedHeaders.get("x-tracing-id")).isEqualTo("123");
        }
    }

    @Test
    public void customHeaderForRequest() throws Exception {
        ClientConfig clientConfig = new ClientConfig.Builder()
            .host(server.getLocation().getUri().toString())
            .token("my-token".toCharArray())
            .headers(Map.of("X-Tracing-Id", "123"))
            .build();

        try (FlightSqlClient flightSqlClient = new FlightSqlClient(clientConfig);
            var data = executeQuery(flightSqlClient, Map.of(), Map.of("X-Invoice-Id", "456"))) {

            Assertions.assertThat(data.count()).isEqualTo(rowCount);

            final Map<String, String> receivedHeaders = headerFactory.getLastInstance().getHeaders();

            Assertions.assertThat(receivedHeaders.keySet()).contains(
                "authorization",
                "user-agent",
                "x-tracing-id",
                "x-invoice-id",
                GrpcUtil.MESSAGE_ACCEPT_ENCODING
            );
            Assertions.assertThat(receivedHeaders.get("x-invoice-id")).isEqualTo("456");
        }
    }

    @Test
    public void customHeaderForRequestOverrideConfig() throws Exception {
        ClientConfig clientConfig = new ClientConfig.Builder()
            .host(server.getLocation().getUri().toString())
            .token("my-token".toCharArray())
            .headers(Map.of("X-Tracing-Id", "123"))
            .build();

        try (FlightSqlClient flightSqlClient = new FlightSqlClient(clientConfig);
            var data = executeQuery(flightSqlClient, Map.of(), Map.of("X-Tracing-Id", "456"))) {

            Assertions.assertThat(data.count()).isEqualTo(rowCount);

            final Map<String, String> receivedHeaders = headerFactory.getLastInstance().getHeaders();

            Assertions.assertThat(receivedHeaders.keySet()).contains(
                "authorization",
                "user-agent",
                "x-tracing-id",
                GrpcUtil.MESSAGE_ACCEPT_ENCODING
            );
            Assertions.assertThat(receivedHeaders.get("x-tracing-id")).isEqualTo("456");
        }
    }

    @Test
    public void useParamsFromQueryConfig() throws Exception {
        ClientConfig clientConfig = new ClientConfig.Builder()
            .host(server.getLocation().getUri().toString())
            .token("my-token".toCharArray())
            .database("mydb")
            .build();

        try (FlightSqlClient flightSqlClient = new FlightSqlClient(clientConfig);
            InfluxDBClient influxDBClient = new InfluxDBClientImpl(clientConfig, null, flightSqlClient);
            Stream<Object[]> data = influxDBClient.query(
                "select * from cpu",
                new QueryOptions(Map.of("X-Tracing-Id", "987")))) {

            Assertions.assertThat(data.count()).isEqualTo(rowCount);

            final Map<String, String> receivedHeaders = headerFactory.getLastInstance().getHeaders();

            Assertions.assertThat(receivedHeaders.keySet()).contains(
                "authorization",
                "x-tracing-id",
                "user-agent",
                GrpcUtil.MESSAGE_ACCEPT_ENCODING
            );
            Assertions.assertThat(receivedHeaders.get("x-tracing-id")).isEqualTo("987");
        }
    }

    @Test
    public void disableGRPCCompression() throws Exception {
        ClientConfig clientConfig = new ClientConfig.Builder()
            .host(server.getLocation().getUri().toString())
            .token("my-token".toCharArray())
            .disableGRPCCompression(true)
            .build();

        var qopts = new GrpcCallOptions.Builder().withCompressorName("identity").build();
        try (FlightSqlClient flightSqlClient = new FlightSqlClient(clientConfig);
            var data = executeQuery(flightSqlClient, Map.of(), Map.of(), qopts.getCallOptions())) {

            Assertions.assertThat(data.count()).isEqualTo(rowCount);

            final Map<String, String> receivedHeaders = headerFactory.getLastInstance().getHeaders();

            Assertions.assertThat(receivedHeaders.keySet()).containsOnly(
                "authorization",
                "user-agent",
                "content-type"
            );
            Assertions.assertThat(receivedHeaders.get(GrpcUtil.MESSAGE_ACCEPT_ENCODING)).isNull();
        }
    }

    private Stream<Map<String, Object>> executeQuery(final FlightSqlClient flightSqlClient,
                                                     final Map<String, Object> queryParameters,
                                                     final Map<String, String> headers,
                                                     final CallOption... callOptions) {
        return flightSqlClient.execute(
                "select * from cpu",
                "mydb",
                QueryType.SQL,
                queryParameters,
                headers,
                callOptions)
            .flatMap(vector -> IntStream.range(0, vector.getRowCount())
                .mapToObj(rowNumber ->
                    VectorSchemaRootConverter.INSTANCE
                        .getMapFromVectorSchemaRoot(
                            vector,
                            rowNumber
                        )));
    }

    @BeforeEach
    void setUp() throws Exception {
        allocator = new RootAllocator(Long.MAX_VALUE);
        server = FlightServer.builder(allocator, grpcLocation, TestUtils.simpleProducer(vectorSchemaRoot)).middleware(
            HEADER_CAPTURE_KEY,
            headerFactory).build().start();
    }

    @AfterEach
    void tearDown() throws Exception {
        if (server != null) {
            server.shutdown();
            server.awaitTermination();
        }
        if (allocator != null) {
            allocator.close();
        }
    }

    @Test
    void flightSqlClient() throws Exception {
        String correctHost = "grpc+unix://tmp/dummy.sock";
        ClientConfig clientConfig = new ClientConfig.Builder()
            .host(correctHost)
            .token("Token".toCharArray())
            .build();
        try (FlightSqlClient flightSqlClient = new FlightSqlClient(clientConfig)) {
            Assertions.assertThat(flightSqlClient).isNotNull();
        }

        FlightClient.Builder builder = FlightClient.builder(allocator, server.getLocation());
        try (FlightClient flightClient = builder.build()) {
            FlightSqlClient flightSqlClient = new FlightSqlClient(clientConfig, flightClient);
            Assertions.assertThat(flightSqlClient).isNotNull();
        }

        var inCorrectHost = "grpc+unix://///tmp/dummy.sock";
        ClientConfig clientConfig1 = new ClientConfig.Builder()
            .host(inCorrectHost)
            .token("Token".toCharArray())
            .build();
        Assertions.assertThatThrownBy(() -> new FlightSqlClient(clientConfig1));
    }

    @Test
    void createProxyDetector() {
        String targetUrl = "https://localhost:80";
        ClientConfig clientConfig = new ClientConfig.Builder()
            .host(targetUrl)
            .build();
        try (FlightSqlClient flightSqlClient = new FlightSqlClient(clientConfig)) {
            String proxyUrl = "http://localhost:10000";
            ProxyDetector proxyDetector = flightSqlClient.createProxyDetector(targetUrl, proxyUrl);
            Assertions.assertThat(proxyDetector.proxyFor(
                new InetSocketAddress("localhost", 80)
            )).isEqualTo(HttpConnectProxiedSocketAddress.newBuilder()
                .setProxyAddress(new InetSocketAddress("localhost", 10000))
                .setTargetAddress(new InetSocketAddress("localhost", 80))
                .build());

            // Return null case
            Assertions.assertThat(proxyDetector.proxyFor(
                new InetSocketAddress("123.2.3.1", 80)
            )).isNull();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    static class HeaderCaptureMiddleware implements FlightServerMiddleware {

        private final Map<String, String> headers = new HashMap<>();

        public HeaderCaptureMiddleware(final CallHeaders callHeaders) {
            for (String key : callHeaders.keys()) {
                headers.put(key, callHeaders.get(key));
            }
        }

        public Map<String, String> getHeaders() {
            return headers;
        }


        @Override
        public void onBeforeSendingHeaders(final CallHeaders callHeaders) {

        }

        @Override
        public void onCallCompleted(final CallStatus callStatus) {

        }

        @Override
        public void onCallErrored(final Throwable throwable) {

        }
    }

    static class HeaderCaptureMiddlewareFactory implements FlightServerMiddleware.Factory<HeaderCaptureMiddleware> {

        private HeaderCaptureMiddleware lastInstance;


        public HeaderCaptureMiddleware getLastInstance() {
            return lastInstance;
        }

        @Override
        public HeaderCaptureMiddleware onCallStarted(final CallInfo callInfo,
                                                     final CallHeaders callHeaders,
                                                     final RequestContext requestContext) {
            lastInstance = new HeaderCaptureMiddleware(callHeaders);
            return lastInstance;
        }
    }

}
