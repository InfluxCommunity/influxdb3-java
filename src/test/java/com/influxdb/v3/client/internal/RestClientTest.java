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

import com.influxdb.v3.client.*;
import com.influxdb.v3.client.config.ClientConfig;
import com.influxdb.v3.client.config.NettyHttpClientConfig;
import com.influxdb.v3.client.write.WriteOptions;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.ssl.JdkSslContext;
import io.netty.handler.ssl.SslContext;
import mockwebserver3.Dispatcher;
import mockwebserver3.MockResponse;
import mockwebserver3.MockWebServer;
import mockwebserver3.RecordedRequest;
import okhttp3.Headers;
import org.assertj.core.api.Assertions;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;
import javax.net.ssl.SSLException;
import java.io.IOException;
import java.net.InetAddress;
import java.net.URISyntaxException;
import java.net.http.HttpHeaders;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.AssertionsForClassTypes.catchThrowable;

public class RestClientTest extends AbstractMockServerTest {

    private RestClient restClient;

    @AfterEach
    void tearDown() {
        if (restClient != null) {
            restClient.close();
        }
    }

    @Test
    public void baseUrl() throws URISyntaxException, SSLException {
        restClient = new RestClient(new ClientConfig.Builder().host("http://localhost:8086").build());
        Assertions
                .assertThat(restClient.baseUrl)
                .isEqualTo("http://localhost:8086/");
    }

    @Test
    public void baseUrlSlashEnd() throws URISyntaxException, SSLException {
        restClient = new RestClient(new ClientConfig.Builder().host("http://localhost:8086/").build());
        Assertions
                .assertThat(restClient.baseUrl)
                .isEqualTo("http://localhost:8086/");
    }

    @Test
    public void responseTimeout() throws URISyntaxException, SSLException {
        restClient = new RestClient(new ClientConfig.Builder()
                .host("http://localhost:8086")
                .timeout(Duration.ofSeconds(13))
                .build());

        Optional<Duration> connectTimeout = Optional.of(restClient.timeout);

        Assertions.assertThat(connectTimeout).isPresent();
        Assertions.assertThat(connectTimeout.get()).isEqualTo(Duration.ofSeconds(13));
    }

    //fixme how to handle redirect
//    @Test
//    public void allowHttpRedirectsDefaults() throws URISyntaxException, SSLException {
//        restClient = new RestClient(new ClientConfig.Builder()
//                .host("http://localhost:8086")
//                .build());
//
//        HttpClient.Redirect redirect = restClient.client.followRedirects();
//        Assertions.assertThat(redirect).isEqualTo(HttpClient.Redirect.NEVER);
//    }

    @Test
    public void authenticationHeader() throws InterruptedException, URISyntaxException, SSLException, ExecutionException {
        mockServer.enqueue(createResponse(200));

        restClient = new RestClient(new ClientConfig.Builder()
                .host(baseURL)
                .token("my-token".toCharArray())
                .build());

        restClient.request(HttpMethod.GET, "/ping");

        RecordedRequest recordedRequest = mockServer.takeRequest();

        String authorization = recordedRequest.getHeaders().get("Authorization");
        Assertions.assertThat(authorization).isEqualTo("Token my-token");
    }

    @Test
    public void authenticationHeaderCustomAuthScheme() throws InterruptedException, URISyntaxException, SSLException, ExecutionException {
        mockServer.enqueue(createResponse(200));

        restClient = new RestClient(new ClientConfig.Builder()
                .host(baseURL)
                .token("my-token".toCharArray())
                .authScheme("my-auth-scheme")
                .build());

        restClient.request(HttpMethod.GET, "ping");

        RecordedRequest recordedRequest = mockServer.takeRequest();

        String authorization = recordedRequest.getHeaders().get("Authorization");
        Assertions.assertThat(authorization).isEqualTo("my-auth-scheme my-token");
    }

    @Test
    public void authenticationHeaderNotDefined() throws InterruptedException, URISyntaxException, SSLException, ExecutionException {
        mockServer.enqueue(createResponse(200));

        restClient = new RestClient(new ClientConfig.Builder()
                .host(baseURL)
                .build());

        restClient.request(HttpMethod.GET, "/ping");

        RecordedRequest recordedRequest = mockServer.takeRequest();

        String authorization = recordedRequest.getHeaders().get("Authorization");
        Assertions.assertThat(authorization).isNull();
    }

    @Test
    public void userAgent() throws InterruptedException, URISyntaxException, SSLException, ExecutionException {
        mockServer.enqueue(createResponse(200));

        restClient = new RestClient(new ClientConfig.Builder()
                .host(baseURL)
                .build());

        restClient.request(HttpMethod.GET, "/ping");

        RecordedRequest recordedRequest = mockServer.takeRequest();

        String userAgent = recordedRequest.getHeaders().get("User-Agent");
        Assertions.assertThat(userAgent).startsWith("influxdb3-java/");
    }

    @Test
    public void customHeader() throws InterruptedException, ExecutionException, URISyntaxException, SSLException {
        mockServer.enqueue(createResponse(200));

        restClient = new RestClient(new ClientConfig.Builder()
                .host(baseURL)
                .token("my-token".toCharArray())
                .headers(Map.of("X-device", "ab-01"))
                .build());

        restClient.request(HttpMethod.GET, "/ping");

        RecordedRequest recordedRequest = mockServer.takeRequest();

        String authorization = recordedRequest.getHeaders().get("X-device");
        Assertions.assertThat(authorization).isEqualTo("ab-01");
    }

    @Test
    public void customHeaderRequest() throws InterruptedException, URISyntaxException, SSLException, ExecutionException {
        mockServer.enqueue(createResponse(200));

        restClient = new RestClient(new ClientConfig.Builder()
                .host(baseURL)
                .token("my-token".toCharArray())
                .headers(Map.of("X-device", "ab-01"))
                .build());

        restClient.request(HttpMethod.GET, "ping", Map.of("X-Request-Trace-Id", "123"));

        RecordedRequest recordedRequest = mockServer.takeRequest();

        String header = recordedRequest.getHeaders().get("X-device");
        Assertions.assertThat(header).isEqualTo("ab-01");
        header = recordedRequest.getHeaders().get("X-Request-Trace-Id");
        Assertions.assertThat(header).isEqualTo("123");
    }

    @Test
    public void useCustomHeaderFromRequest() throws InterruptedException, URISyntaxException, SSLException, ExecutionException {
        mockServer.enqueue(createResponse(200));

        restClient = new RestClient(new ClientConfig.Builder()
                .host(baseURL)
                .token("my-token".toCharArray())
                .headers(Map.of("X-device", "ab-01"))
                .build());

        restClient.request(HttpMethod.GET, "ping", Map.of("X-device", "ab-02"));

        RecordedRequest recordedRequest = mockServer.takeRequest();

        String header = recordedRequest.getHeaders().get("X-device");
        Assertions.assertThat(header).isEqualTo("ab-02");
    }

    @Test
    public void useParamsFromWriteConfig() throws Exception {

        ClientConfig config = new ClientConfig.Builder()
                .host(baseURL)
                .token("my-token".toCharArray())
                .database("my-database")
                .build();

        mockServer.enqueue(createResponse(200));

        try (RestClient restClient = new RestClient(config);
             InfluxDBClient client = new InfluxDBClientImpl(config, restClient, null)) {

            client.writeRecord("mem,tag=one value=1.0", new WriteOptions(Map.of("X-Tracing-Id", "852")));
        }

        RecordedRequest recordedRequest = mockServer.takeRequest();

        String header = recordedRequest.getHeaders().get("X-Tracing-Id");
        Assertions.assertThat(header).isEqualTo("852");
    }

    @Test
    public void uri() throws InterruptedException, URISyntaxException, SSLException, ExecutionException {
        mockServer.enqueue(createResponse(200));

        restClient = new RestClient(new ClientConfig.Builder()
                .host(baseURL)
                .build());

        restClient.request(HttpMethod.GET, "/ping");

        RecordedRequest recordedRequest = mockServer.takeRequest();

        Assertions.assertThat(recordedRequest.getUrl()).isNotNull();
        Assertions.assertThat(recordedRequest.getUrl().toString()).isEqualTo(baseURL + "ping");
    }

    //fixme how to handle redirect???
//    @Test
//    public void allowHttpRedirects() throws URISyntaxException, SSLException {
//        restClient = new RestClient(new ClientConfig.Builder()
//                .host("http://localhost:8086")
//                .allowHttpRedirects(true)
//                .build());
//
//        HttpClient.Redirect redirect = restClient.client.followRedirects();
//        Assertions.assertThat(redirect).isEqualTo(HttpClient.Redirect.NORMAL);
//    }

    //fixme test no longer valid, no longer support ClientConfig.Builder().proxy()
//    @Test
//    public void proxy() throws InterruptedException, ExecutionException, URISyntaxException, SSLException {
//        mockServer.enqueue(createResponse(200));
//
//        restClient = new RestClient(new ClientConfig.Builder()
//                .host("http://foo.com:8086")
//                .proxy(ProxySelector.of((InetSocketAddress) mockServer.getProxyAddress().address()))
//                .build());
//
//        restClient.request(HttpMethod.GET, "ping");
//
//        RecordedRequest recordedRequest = mockServer.takeRequest();
//
//        Assertions.assertThat(recordedRequest.getUrl()).isNotNull();
//        // with mockwebserver3 getUrl() returns target URL not proxy URL
//        // successful return implies proxy was used correctly.
//        Assertions.assertThat(recordedRequest.getUrl().toString())
//                .isEqualTo("http://foo.com:8086/ping"); // server is used as proxy
//        Assertions.assertThat(recordedRequest.getRequestLine())
//                .isEqualTo("GET https://foo.com:8086/ping HTTP/1.1");
//    }

    //fixme Must implement HttpProxyHandler first
//    @Test
//    public void proxyUrl() throws InterruptedException, URISyntaxException, IOException, ExecutionException {
//        try (MockWebServer proxyServer = overrideDispatchServer("localhost", 10000, null, null, false);) {
//            restClient = new RestClient(new ClientConfig.Builder()
//                    .host(String.format("http://%s:%d", mockServer.getHostName(), mockServer.getPort()))
//                    .proxyUrl("http://localhost:10000")
//                    .build());
//
//            restClient.request(HttpMethod.GET, "ping");
//
//            RecordedRequest recordedRequest = proxyServer.takeRequest();
//
//            Assertions.assertThat(recordedRequest.getUrl()).isNotNull();
//            Assertions.assertThat(recordedRequest.getUrl().host()).isEqualTo(mockServer.getHostName());
//            Assertions.assertThat(recordedRequest.getUrl().port()).isEqualTo(mockServer.getPort());
//            Assertions.assertThat(recordedRequest.getMethod()).isEqualTo("CONNECT");
//        }
//    }

    //fixme test no longer valid, no longer support ClientConfig.Builder().authenticator()
//    @Test
//    public void proxyWithAuthentication() throws InterruptedException, URISyntaxException, SSLException, ExecutionException {
//        mockServer.enqueue(createResponse(407, Map.of("Proxy-Authenticate", "Basic"), null));
//        mockServer.enqueue(createResponse(200));
//
//        restClient = new RestClient(new ClientConfig.Builder()
//                .host("http://foo.com:8086")
//                .proxyUrl(String.format("http://%s:%d", mockServer.getHostName(), mockServer.getPort()))
//                .authenticator(new Authenticator() {
//                    @Override
//                    protected PasswordAuthentication getPasswordAuthentication() {
//                        return new PasswordAuthentication("john", "secret".toCharArray());
//                    }
//                })
//                .build());
//
//        restClient.request(HttpMethod.GET, "ping");
//
//        RecordedRequest recordedRequest = mockServer.takeRequest();
//        RecordedRequest proxyAuthRequest = mockServer.takeRequest();
//
//        Assertions.assertThat(recordedRequest.getUrl()).isNotNull();
//        // with mockwebserver3 getUrl() returns target URL not proxy URL
//        // successful return implies proxy was used correctly.
//        Assertions.assertThat(recordedRequest.getUrl().toString()).isEqualTo("http://foo.com:8086/ping");
//        Assertions.assertThat(recordedRequest.getRequestLine()).isEqualTo("GET http://foo.com:8086/ping HTTP/1.1");
//
//        Assertions.assertThat(mockServer.getRequestCount()).isEqualTo(2);
//        String proxyAuthorization = proxyAuthRequest.getHeaders().get("Proxy-Authorization");
//        Assertions.assertThat(proxyAuthorization)
//                .isEqualTo("Basic " + Base64.getEncoder().encodeToString("john:secret".getBytes()));
//    }

    @Test
    public void error() throws URISyntaxException, SSLException {
        mockServer.enqueue(createResponse(404));

        restClient = new RestClient(new ClientConfig.Builder()
                .host(baseURL)
                .build());

        Assertions.assertThatThrownBy(
                        () -> restClient.request(HttpMethod.GET, "ping"))
                .isInstanceOf(InfluxDBApiException.class)
                .hasMessage("HTTP status code: 404; Message: Not Found");
    }

    @Test
    public void errorFromHeader() throws URISyntaxException, SSLException {

        mockServer.enqueue(createResponse(500, Map.of("X-Influx-Error", "not used"), null));

        restClient = new RestClient(new ClientConfig.Builder()
                .host(baseURL)
                .build());

        Assertions.assertThatThrownBy(
                        () -> restClient.request(HttpMethod.GET, "ping"))
                .isInstanceOf(InfluxDBApiException.class)
                .hasMessage("HTTP status code: 500; Message: not used");
    }

    @Test
    public void errorFromBody() throws URISyntaxException, SSLException {

        mockServer.enqueue(createResponse(401,
                Map.of("X-Influx-Errpr", "not used"),
                "{\"message\":\"token does not have sufficient permissions\"}"));

        restClient = new RestClient(new ClientConfig.Builder()
                .host(baseURL)
                .build());

        Assertions.assertThatThrownBy(
                        () -> restClient.request(HttpMethod.GET, "ping")
                )
                .isInstanceOf(InfluxDBApiException.class)
                .hasMessage("HTTP status code: 401; Message: token does not have sufficient permissions");
    }

    @Test
    public void errorFromBodyEdgeWithoutMessage() throws URISyntaxException, SSLException { // OSS/Edge error message

        mockServer.enqueue(createResponse(400,
                null,
                "{\"error\":\"parsing failed\"}"));

        restClient = new RestClient(new ClientConfig.Builder()
                .host(baseURL)
                .build());

        Assertions.assertThatThrownBy(
                        () -> restClient.request(HttpMethod.GET, "ping")
                )
                .isInstanceOf(InfluxDBApiException.class)
                .hasMessage("HTTP status code: 400; Message: parsing failed");
    }

    @Test
    public void errorFromBodyEdgeWithMessage() throws URISyntaxException, SSLException { // OSS/Edge specific error message

        mockServer.enqueue(createResponse(400,
                null,
                "{\"error\":\"parsing failed\",\"data\":{\"error_message\":\"invalid field value\"}}"));

        restClient = new RestClient(new ClientConfig.Builder()
                .host(baseURL)
                .build());

        Assertions.assertThatThrownBy(
                        () -> restClient.request(HttpMethod.GET, "ping")
                )
                .isInstanceOf(InfluxDBApiException.class)
                .hasMessage("HTTP status code: 400; Message: invalid field value");
    }

    @Test
    public void errorFromBodyText() throws URISyntaxException, IOException {
        mockServer.enqueue(createResponse(402, null, "token is over the limit"));

        restClient = new RestClient(new ClientConfig.Builder()
                .host(baseURL)
                .build());

        Assertions.assertThatThrownBy(
                        () -> restClient.request(HttpMethod.GET, "ping")
                )
                .isInstanceOf(InfluxDBApiException.class)
                .hasMessage("HTTP status code: 402; Message: token is over the limit");
    }

    @Test
    public void generateHttpException() {
        HttpHeaders headers = HttpHeaders.of(Map.of(
                        "content-type", List.of("application/json"),
                        "retry-after", List.of("300")),
                (key, value) -> true);

        InfluxDBApiHttpException exception = new InfluxDBApiHttpException(
                new InfluxDBApiException("test exception"), headers, 418);

        Assertions.assertThat(exception.headers()).isEqualTo(headers);
        Assertions.assertThat(exception.statusCode()).isEqualTo(418);
        Assertions.assertThat(exception.getCause()).isInstanceOf(InfluxDBApiException.class);
        Assertions.assertThat(exception.getCause().getMessage()).isEqualTo("test exception");
    }

    @Test
    public void errorHttpExceptionThrown() throws URISyntaxException, SSLException {
        String retryDate = Instant.now().plus(300, ChronoUnit.SECONDS).toString();

        mockServer.enqueue(createResponse(503,
                Map.of("retry-after", retryDate, "content-type", "application/json"),
                "{\"message\":\"temporarily offline\"}"));

        restClient = new RestClient(new ClientConfig.Builder()
                .host(baseURL)
                .build());

        Throwable thrown = catchThrowable(() -> restClient.request(HttpMethod.POST, "/api/v2/write")
        );

        Assertions.assertThat(thrown).isNotNull();
        Assertions.assertThat(thrown).isInstanceOf(InfluxDBApiNettyException.class);
        InfluxDBApiNettyException he = (InfluxDBApiNettyException) thrown;
        Assertions.assertThat(he.headers()).isNotNull();
        Assertions.assertThat(he.getHeader("retry-after").get(0))
                .isNotNull().isEqualTo(retryDate);
        Assertions.assertThat(he.getHeader("content-type").get(0))
                .isNotNull().isEqualTo("application/json");
        Assertions.assertThat(he.getHeader("wumpus").size()).isEqualTo(0);
        Assertions.assertThat(he.statusCode()).isEqualTo(503);
        Assertions.assertThat(he.getMessage())
                .isEqualTo("HTTP status code: 503; Message: temporarily offline");
    }

    @Test
    public void getServerVersionV2Successful() throws Exception {
        String influxDBVersion = "v2.1.0";
        mockServer.enqueue(createResponse(200,
                Map.of("x-influxdb-version", influxDBVersion),
                null));

        restClient = new RestClient(new ClientConfig.Builder()
                .host(baseURL)
                .build());
        String version = restClient.getServerVersion();

        Assertions.assertThat(version).isEqualTo(influxDBVersion);
    }

    @Test
    public void getServerVersionV3Successful() throws Exception {
        String influxDBVersion = "3.0.0";
        mockServer.enqueue(createResponse(200,
                null,
                "{\"version\":\"" + influxDBVersion + "\"}"));

        restClient = new RestClient(new ClientConfig.Builder()
                .host(baseURL)
                .build());
        String version = restClient.getServerVersion();

        Assertions.assertThat(version).isEqualTo(influxDBVersion);
    }

    @Test
    public void getServerVersionError() throws URISyntaxException, SSLException, ExecutionException, InterruptedException {
        MockResponse mockResponse = new MockResponse(200,
                Headers.of("something", "something"),
                "not json");
        mockServer.enqueue(mockResponse);

        restClient = new RestClient(new ClientConfig.Builder()
                .host(baseURL)
                .build());
        String version = restClient.getServerVersion();
        Assertions.assertThat(version).isEqualTo(null);
    }

    @Test
    public void getServerVersionErrorNoBody() throws ExecutionException, InterruptedException, URISyntaxException, SSLException {
        mockServer.enqueue(new MockResponse(200, Headers.of(), "Test-Version"));
        restClient = new RestClient(new ClientConfig.Builder()
                .host(baseURL)
                .build());
        String version = restClient.getServerVersion();
        Assertions.assertThat(version).isEqualTo(null);
    }

    @Test
    public void nettyRestMutualSslContext() throws ExecutionException, InterruptedException, URISyntaxException, IOException, UnrecoverableKeyException, CertificateException, KeyStoreException, NoSuchAlgorithmException {
        var password = "123456";
        var format = "PKCS12";

        var keyFilePath = "src/test/java/com/influxdb/v3/client/testdata/server/pkcs12/keystore.p12";
        var trustFilePath = "src/test/java/com/influxdb/v3/client/testdata/server/pkcs12/truststore.p12";
        JdkSslContext serverSslContext = (JdkSslContext) TestUtils.createNettySslContext(true, format, password, keyFilePath, trustFilePath, false, true);

        keyFilePath = "src/test/java/com/influxdb/v3/client/testdata/client/pkcs12/keystore.p12";
        trustFilePath = "src/test/java/com/influxdb/v3/client/testdata/client/pkcs12/truststore.p12";
        SslContext clientSslContext = TestUtils.createNettySslContext(false, format, password, keyFilePath, trustFilePath, false, false);

        NettyHttpClientConfig nettyHttpClientConfig = new NettyHttpClientConfig();
        nettyHttpClientConfig.configureSsl(() -> clientSslContext);
        ClientConfig config = new ClientConfig.Builder().host("https://localhost:8080")
                .nettyHttpClientConfig(nettyHttpClientConfig)
                .build();

        var influxDBVersion = "4.0.0";
        try (
                MockWebServer ignored = overrideDispatchServer("localhost", 8080, new MockResponse(200, Headers.of(), "{\"version\":\"" + influxDBVersion + "\"}"), serverSslContext, true);
                RestClient restClient = new RestClient(config);
        ) {
            Assertions.assertThat(restClient.getServerVersion()).isEqualTo(influxDBVersion);
        }
    }

    // Make the call fails because this is mTLS but the client does not send its key, note that: isDisableKeyStore = true
    @Test
    public void nettyRestMutualSslContextFail() throws IOException, UnrecoverableKeyException, CertificateException, KeyStoreException, NoSuchAlgorithmException {
        var password = "123456";
        var format = "PKCS12";

        var keyFilePath = "src/test/java/com/influxdb/v3/client/testdata/server/pkcs12/keystore.p12";
        var trustFilePath = "src/test/java/com/influxdb/v3/client/testdata/server/pkcs12/truststore.p12";
        JdkSslContext serverSslContext = (JdkSslContext) TestUtils.createNettySslContext(true, format, password, keyFilePath, trustFilePath, false, true);

        keyFilePath = "src/test/java/com/influxdb/v3/client/testdata/client/pkcs12/keystore.p12";
        trustFilePath = "src/test/java/com/influxdb/v3/client/testdata/client/pkcs12/truststore.p12";
        // The call failed because isDisableKeyStore = true
        SslContext clientSslContext = TestUtils.createNettySslContext(false, format, password, keyFilePath, trustFilePath, true, false);

        NettyHttpClientConfig nettyHttpClientConfig = new NettyHttpClientConfig();
        nettyHttpClientConfig.configureSsl(() -> clientSslContext);
        ClientConfig config = new ClientConfig.Builder().host("https://localhost:8080")
                .nettyHttpClientConfig(nettyHttpClientConfig)
                .build();

        try (
                MockWebServer ignored = overrideDispatchServer("localhost", 8080, new MockResponse(400, Headers.of(), ""), serverSslContext, true);
                RestClient restClient = new RestClient(config);
        ) {
            restClient.getServerVersion();
        } catch (Exception e) {
            Assertions.assertThat(e.getMessage()).contains("BAD_CERTIFICATE");
        }
    }

    /*Private functions*/
    //fixme refactor this
    private MockWebServer overrideDispatchServer(@NotNull String host, @NotNull Integer port, @Nullable MockResponse mockResponse, @Nullable SslContext sslContext, boolean requireClientAuth) throws IOException {
        MockWebServer server = new MockWebServer();
        server.setDispatcher(new Dispatcher() {
            @NotNull
            @Override
            public MockResponse dispatch(@NotNull RecordedRequest recordedRequest) throws InterruptedException {
                return mockResponse != null ? mockResponse : new MockResponse(200, Headers.EMPTY, "Success");
            }
        });
        if (sslContext instanceof JdkSslContext) {
            server.useHttps(((JdkSslContext) sslContext).context().getSocketFactory());
            if (requireClientAuth) {
                server.requireClientAuth();
            }
        }
        server.start(InetAddress.getByName(host), port);
        return server;
    }
}
