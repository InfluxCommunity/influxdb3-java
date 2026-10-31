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
package com.influxdb.v3.client;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

import mockwebserver3.MockResponse;
import mockwebserver3.MockWebServer;
import okhttp3.Headers;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import com.influxdb.v3.client.config.ClientConfig;
import com.influxdb.v3.client.internal.Utils;

import static org.assertj.core.api.AssertionsForClassTypes.catchThrowable;

public class InfluxDBClientTest {

    @Test
    void withProxyUrl() {
        String proxyUrl = "http://localhost:10000";
        ClientConfig.Builder builder = new ClientConfig.Builder();
        builder.proxyUrl(proxyUrl);
        ClientConfig clientConfig = builder.build();
        Assertions.assertThat(clientConfig.getProxyUrl()).isEqualTo(proxyUrl);
    }

    @Test
    void parseIpv6() throws UnknownHostException, URISyntaxException {
        record Test(String url, boolean isCorrect) {
        }
        var tests = List.of(
                new Test("http://[2001:db8::1]/", true),
                new Test("http://[2001:db8:a0b:12f0::1]/index.html", true),
                new Test("http://[2001:db8:a0b:12f0::1]:80/index.html", true),
                new Test("https://[2001:db8:a0b:12f0::1%25eth0]:15000/", true),
                new Test("http://[2607:f8b0:4005:802::1007]/", true),
                new Test("http://2001:db8::1/", false),
                new Test("http://2001:db8::1:8080/", false)
        );
        for (Test test : tests) {
            if (!test.isCorrect()) {
                Assertions.assertThatThrownBy(() -> {
                    try (var client = InfluxDBClient.getInstance(test.url(), "my-token".toCharArray(), "bucket0")) {
                        client.getServerVersion();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }).hasMessageContaining("Invalid URL.");
            } else {
                Assertions.assertThatNoException().isThrownBy(() -> {
                    try (var ignored = InfluxDBClient.getInstance(test.url(), "my-token".toCharArray(), "bucket0")) {
                        Assertions.assertThat(true);
                    }
                });
            }
        }
    }

    @Test
    void withSslRootsFilePath() {
        String path = "/path/to/cert";
        ClientConfig.Builder builder = new ClientConfig.Builder();
        builder.sslRootsFilePath(path);
        ClientConfig clientConfig = builder.build();
        Assertions.assertThat(clientConfig.sslRootsFilePath()).isEqualTo(path);
    }

    @Test
    void requiredHost() {
        Assertions.assertThatThrownBy(() -> InfluxDBClient.getInstance(null, "my-token".toCharArray(), "my-database"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Invalid URL.");

        Assertions.assertThatThrownBy(() -> InfluxDBClient.getInstance(" ", "my-token".toCharArray(), "my-database"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Invalid URL.");
    }

    @Test
    void requiredHostConnectionString() {

        Assertions.assertThatThrownBy(() -> InfluxDBClient.getInstance("?token=my-token&database=my-database"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("no protocol");
    }

    @Test
    void fromParameters() throws Exception {

        try (InfluxDBClient client = InfluxDBClient.getInstance("http://localhost:8086",
                "my-token".toCharArray(), "my-database")) {
            Assertions.assertThat(client).isNotNull();
        }
    }

    @Test
    void fromConnectionString() throws Exception {

        try (InfluxDBClient client = InfluxDBClient.getInstance("http://localhost:8086"
                + "?token=my-token&database=my-db")) {
            Assertions.assertThat(client).isNotNull();
        }
    }

    @Test
    void fromEnvOrProperties() throws Exception {

        final Properties old = System.getProperties();
        final Properties p = new Properties();
        p.put("influx.host", "http://localhost:8086");
        p.put("influx.token", "my-token");
        p.put("influx.database", "my-db");
        System.setProperties(p);

        try (InfluxDBClient client = InfluxDBClient.getInstance()) {
            Assertions.assertThat(client).isNotNull();
        } finally {
            System.setProperties(old);
        }
    }

    @Test
    void withDefaultTags() throws Exception {

        Map<String, String> defaultTags = Map.of("unit", "U2", "model", "M1");

        try (InfluxDBClient client = InfluxDBClient.getInstance(
                "http://localhost:8086",
                "MY-TOKEN".toCharArray(),
                "MY-DATABASE",
                defaultTags)) {
            Assertions.assertThat(client).isNotNull();
        }
    }

    @Test
    public void unsupportedQueryParams() throws Exception {
        try (InfluxDBClient client = InfluxDBClient.getInstance("http://localhost:8086",
                "my-token".toCharArray(), "my-database")) {

            String query = "select * from cpu where client=$client";
            Map<String, Object> parameters = Map.of("client", client);

            Assertions.assertThatThrownBy(() -> client.queryPoints(query, parameters))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("The parameter client value has unsupported type: "
                            + "class com.influxdb.v3.client.internal.InfluxDBClientImpl");
        }
    }

    String tlsDir = "src/test/java/com/influxdb/v3/client/testdata/certificates/";

    String influxdbCertPath = tlsDir + "influxdb.crt";
    String influxdbKeyPath = tlsDir + "influxdb.key";
    String influxdbP12 = tlsDir + "influxdb.p12";

    String otherCertPath = tlsDir + "other-server.crt";
    String otherKeyCertPath = tlsDir + "other-server.key";
    String otherP12 = tlsDir + "other-server.p12";

    String clientCertPath = tlsDir + "client.crt";
    String clientKeyPath = tlsDir + "client.key";
    String clientP12 = tlsDir + "client.p12";

    char[] defaultPassword = "changeit".toCharArray();

    record TlsTest(
            @Nonnull String name,
            @Nullable String clientCertFilePath,
            @Nullable String clientKeyFilePath,
            @Nonnull String sslRootFilePath,
            boolean expectSuccess
    ) {
    }

    private List<TlsTest> tlsCases() {
        return List.of(
                new TlsTest(
                        "Tls success with correct server certificate",
                        null,
                        null,
                        influxdbCertPath,
                        true
                ),
                new TlsTest(
                        "Tls success with correct .p12 server certificate",
                        null,
                        null,
                        influxdbP12,
                        true
                ),
                new TlsTest(
                        "Tls fail with incorrect server certificate",
                        null,
                        null,
                        otherCertPath,
                        false
                ),
                new TlsTest(
                        "mtls success with correct client certificate .key and .crt file",
                        clientCertPath,
                        clientKeyPath,
                        influxdbCertPath,
                        true
                ),
                new TlsTest(
                        "mtls success with correct client certificate .p12 file",
                        clientP12,
                        null,
                        influxdbCertPath,
                        true
                ),
                new TlsTest(
                        "mtls fail with incorrect client certificate .crt and .key file",
                        otherCertPath,
                        otherKeyCertPath,
                        influxdbCertPath,
                        false
                ),
                new TlsTest(
                        "mtls fail with incorrect client certificate .p12 file",
                        otherP12,
                        null,
                        influxdbCertPath,
                        false
                )
        );
    }

    @Test
    public void testWriteSecureServer() {
        for (TlsTest tlsCase : tlsCases()) {
            Throwable thrown = catchThrowable(() ->
                    testWriteSecureMockServer(
                            tlsCase.clientCertFilePath,
                            tlsCase.clientKeyFilePath,
                            defaultPassword,
                            tlsCase.sslRootFilePath,
                            defaultPassword
                    ));
            if (!tlsCase.expectSuccess) {
                Assertions.assertThat(thrown).isNotNull();
                Assertions.assertThat(thrown).isInstanceOf(InfluxDBApiException.class);
                Assertions.assertThat(thrown).hasMessageContaining("ssl");
            } else {
                Assertions.assertThat(thrown).isNull();
            }
        }
    }

    @Test
    public void testQuerySecureServer() throws Exception {
        for (TlsTest tlsCase : tlsCases()) {
            try (var flightServer = createQuerySecureServer(
                    influxdbCertPath,
                    influxdbKeyPath,
                    tlsCase.clientCertFilePath != null ? clientCertPath : null)) {
                flightServer.start();
                URI uri = flightServer.getLocation().getUri();

                ClientConfig clientConfig = getSecureClientConfigBuilder(
                        String.format("https://%s:%d", uri.getHost(), uri.getPort()),
                        tlsCase.sslRootFilePath,
                        defaultPassword,
                        tlsCase.clientCertFilePath,
                        tlsCase.clientKeyFilePath,
                        defaultPassword
                ).build();
                try (InfluxDBClient influxDBClient = InfluxDBClient.getInstance(clientConfig)) {
                    if (!tlsCase.expectSuccess) {
                        Throwable thrown = catchThrowable(() -> {
                            Stream<Object[]> stream = influxDBClient.query("Select * from \"nothing\"");
                            stream.forEach(objects -> {
                                Assertions.assertThat(objects[0]).isEqualTo("Value");
                            });
                        });
                        System.out.println(thrown);
                    } else {
                        Stream<Object[]> stream = influxDBClient.query("Select * from \"nothing\"");
                        stream.forEach(objects -> {
                            Assertions.assertThat(objects[0]).isEqualTo("Value");
                        });
                    }
                }
            }
        }
    }

    @Nonnull
    public FlightServer createQuerySecureServer(@Nonnull final String serverCertFilePath,
                                                @Nonnull final String serverKeyFilePath,
                                                @Nullable final String clientCertPath)
            throws Exception {
        URI uri = URI.create("https://127.0.0.1:" + TestUtils.findFreePort());
        VectorSchemaRoot vectorSchemaRoot = TestUtils.generateVectorSchemaRootWithNull();
        return TestUtils.secureFlightServer(
                uri,
                new RootAllocator(Long.MAX_VALUE),
                TestUtils.simpleProducer(vectorSchemaRoot),
                serverCertFilePath,
                serverKeyFilePath,
                clientCertPath
        );
    }

    public ClientConfig.Builder getSecureClientConfigBuilder(
            @Nonnull final String host,
            @Nonnull final String trustFilePath,
            @Nullable final char[] trustFilePathPassword,
            @Nullable final String clientCertFilePath,
            @Nullable final String clientKeyFilePath,
            @Nullable final char[] clientCertFilePassword
    ) {
        ClientConfig.Builder configBuilder = new ClientConfig.Builder()
                .token("my-token".toCharArray())
                .database("mydb")
                .sslRootsFilePath(trustFilePath);

        if (trustFilePathPassword != null) {
            configBuilder.sslRootFilePassword(trustFilePathPassword);
        }

        if (clientCertFilePath != null) {
            if (clientCertFilePath.endsWith(".p12")) {
                configBuilder.certificateP12FilePath(clientCertFilePath, clientCertFilePassword);
            } else {
                configBuilder.certificateFilePath(clientCertFilePath, clientKeyFilePath);
            }
        }

        configBuilder.host(host);
        return configBuilder;
    }

    public void testWriteSecureMockServer(@Nullable final String clientCertFilePath,
                                          @Nullable final String clientKeyFilePath,
                                          @Nullable final char[] clientCertFilePassword,
                                          @Nonnull final String trustFilePath,
                                          @Nullable final char[] trustFilePathPassword
    )
            throws Exception {
        MockWebServer mockServer = null;
        InfluxDBClient client = null;
        try {
            mockServer = createWriteSecureServer(
                    influxdbCertPath,
                    influxdbKeyPath,
                    clientCertFilePath != null ? clientCertPath : null);
            final ClientConfig clientConfig = getSecureClientConfigBuilder(
                    mockServer.url("/").url().toString(),
                    trustFilePath,
                    trustFilePathPassword,
                    clientCertFilePath,
                    clientKeyFilePath,
                    clientCertFilePassword
            ).build();
            client = InfluxDBClient.getInstance(clientConfig);
            client.writeRecord("mem,tag=one value=1.0");
        } catch (Exception e) {
            throw e;
        } finally {
            if (client != null) {
                client.close();
            }

            mockServer.close();
        }
    }

    private MockWebServer createWriteSecureServer(
            @Nonnull final String serverCertFilePath,
            @Nonnull final String serverKeyFilePath,
            @Nullable final String clientCertFilePath
    ) throws Exception {
        KeyManagerFactory kmf = Utils.createKmf(serverCertFilePath, serverKeyFilePath);
        var mockServer = new MockWebServer();
        TrustManagerFactory tmf = null;
        if (clientCertFilePath != null) {
            tmf = Utils.createTmf(clientCertFilePath, null);
            mockServer.requireClientAuth();
        }
        SSLContext sslContext = Utils.buildSslContext(kmf, tmf);
        mockServer.useHttps(sslContext.getSocketFactory());
        mockServer.start();
        mockServer.enqueue(createResponse());

        return mockServer;
    }

    @Nonnull
    protected MockResponse createResponse() {
        return new MockResponse(200,
                Headers.of("something", "something"),
                "ok");
    }

}
