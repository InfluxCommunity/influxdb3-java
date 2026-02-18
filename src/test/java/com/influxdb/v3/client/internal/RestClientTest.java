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

import java.net.Authenticator;
import java.net.InetSocketAddress;
import java.net.PasswordAuthentication;
import java.net.ProxySelector;
import java.net.http.HttpClient;
import java.net.http.HttpHeaders;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import io.netty.handler.codec.http.HttpMethod;
import mockwebserver3.MockResponse;
import mockwebserver3.RecordedRequest;
import okhttp3.Headers;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import com.influxdb.v3.client.AbstractMockServerTest;
import com.influxdb.v3.client.InfluxDBApiException;
import com.influxdb.v3.client.InfluxDBApiHttpException;
import com.influxdb.v3.client.InfluxDBClient;
import com.influxdb.v3.client.config.ClientConfig;
import com.influxdb.v3.client.write.WriteOptions;

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
    public void baseUrl() {
        restClient = new RestClient(new ClientConfig.Builder().host("http://localhost:8086").build());
        Assertions
                .assertThat(restClient.baseUrl)
                .isEqualTo("http://localhost:8086/");
    }

    @Test
    public void baseUrlSlashEnd() {
        restClient = new RestClient(new ClientConfig.Builder().host("http://localhost:8086/").build());
        Assertions
                .assertThat(restClient.baseUrl)
                .isEqualTo("http://localhost:8086/");
    }

    @Test
    public void responseTimeout() {
        restClient = new RestClient(new ClientConfig.Builder()
                .host("http://localhost:8086")
                .timeout(Duration.ofSeconds(13))
                .build());

        Optional<Duration> connectTimeout = restClient.client.connectTimeout();

        Assertions.assertThat(connectTimeout).isPresent();
        Assertions.assertThat(connectTimeout.get()).isEqualTo(Duration.ofSeconds(13));
    }

    @Test
    public void allowHttpRedirectsDefaults() {
        restClient = new RestClient(new ClientConfig.Builder()
                .host("http://localhost:8086")
                .build());

        HttpClient.Redirect redirect = restClient.client.followRedirects();
        Assertions.assertThat(redirect).isEqualTo(HttpClient.Redirect.NEVER);
    }

    @Test
    public void authenticationHeader() throws InterruptedException {
        mockServer.enqueue(createResponse(200));

        restClient = new RestClient(new ClientConfig.Builder()
                .host(baseURL)
                .token("my-token".toCharArray())
                .build());

        restClient.request("ping", HttpMethod.GET, null, null, null);

        RecordedRequest recordedRequest = mockServer.takeRequest();

        String authorization = recordedRequest.getHeaders().get("Authorization");
        Assertions.assertThat(authorization).isEqualTo("Token my-token");
    }

    @Test
    public void authenticationHeaderCustomAuthScheme() throws InterruptedException {
        mockServer.enqueue(createResponse(200));

        restClient = new RestClient(new ClientConfig.Builder()
                .host(baseURL)
                .token("my-token".toCharArray())
                .authScheme("my-auth-scheme")
                .build());

        restClient.request("ping", HttpMethod.GET, null, null, null);

        RecordedRequest recordedRequest = mockServer.takeRequest();

        String authorization = recordedRequest.getHeaders().get("Authorization");
        Assertions.assertThat(authorization).isEqualTo("my-auth-scheme my-token");
    }

    @Test
    public void authenticationHeaderNotDefined() throws InterruptedException {
        mockServer.enqueue(createResponse(200));

        restClient = new RestClient(new ClientConfig.Builder()
                .host(baseURL)
                .build());

        restClient.request("ping", HttpMethod.GET, null, null, null);

        RecordedRequest recordedRequest = mockServer.takeRequest();

        String authorization = recordedRequest.getHeaders().get("Authorization");
        Assertions.assertThat(authorization).isNull();
    }

    @Test
    public void userAgent() throws InterruptedException {
        mockServer.enqueue(createResponse(200));

        restClient = new RestClient(new ClientConfig.Builder()
                .host(baseURL)
                .build());

        restClient.request("ping", HttpMethod.GET, null, null, null);

        RecordedRequest recordedRequest = mockServer.takeRequest();

        String userAgent = recordedRequest.getHeaders().get("User-Agent");
        Assertions.assertThat(userAgent).startsWith("influxdb3-java/");
    }

    @Test
    public void customHeader() throws InterruptedException {
        mockServer.enqueue(createResponse(200));

        restClient = new RestClient(new ClientConfig.Builder()
                .host(baseURL)
                .token("my-token".toCharArray())
                .headers(Map.of("X-device", "ab-01"))
                .build());

        restClient.request("ping", HttpMethod.GET, null, null, null);

        RecordedRequest recordedRequest = mockServer.takeRequest();

        String authorization = recordedRequest.getHeaders().get("X-device");
        Assertions.assertThat(authorization).isEqualTo("ab-01");
    }

    @Test
    public void customHeaderRequest() throws InterruptedException {
        mockServer.enqueue(createResponse(200));

        restClient = new RestClient(new ClientConfig.Builder()
                .host(baseURL)
                .token("my-token".toCharArray())
                .headers(Map.of("X-device", "ab-01"))
                .build());

        restClient.request("ping", HttpMethod.GET, null, null, Map.of("X-Request-Trace-Id", "123"));

        RecordedRequest recordedRequest = mockServer.takeRequest();

        String header = recordedRequest.getHeaders().get("X-device");
        Assertions.assertThat(header).isEqualTo("ab-01");
        header = recordedRequest.getHeaders().get("X-Request-Trace-Id");
        Assertions.assertThat(header).isEqualTo("123");
    }

    @Test
    public void useCustomHeaderFromRequest() throws InterruptedException {
        mockServer.enqueue(createResponse(200));

        restClient = new RestClient(new ClientConfig.Builder()
                .host(baseURL)
                .token("my-token".toCharArray())
                .headers(Map.of("X-device", "ab-01"))
                .build());

        restClient.request("ping", HttpMethod.GET, null, null, Map.of("X-device", "ab-02"));

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
    public void uri() throws InterruptedException {
        mockServer.enqueue(createResponse(200));

        restClient = new RestClient(new ClientConfig.Builder()
                .host(baseURL)
                .build());

        restClient.request("ping", HttpMethod.GET, null, null, null);

        RecordedRequest recordedRequest = mockServer.takeRequest();

        Assertions.assertThat(recordedRequest.getUrl()).isNotNull();
        Assertions.assertThat(recordedRequest.getUrl().toString()).isEqualTo(baseURL + "ping");
    }

    @Test
    public void allowHttpRedirects() {
        restClient = new RestClient(new ClientConfig.Builder()
                .host("http://localhost:8086")
                .allowHttpRedirects(true)
                .build());

        HttpClient.Redirect redirect = restClient.client.followRedirects();
        Assertions.assertThat(redirect).isEqualTo(HttpClient.Redirect.NORMAL);
    }

    @Test
    public void proxy() throws InterruptedException {
        mockServer.enqueue(createResponse(200));

        restClient = new RestClient(new ClientConfig.Builder()
                .host("http://foo.com:8086")
                .proxy(ProxySelector.of((InetSocketAddress) mockServer.getProxyAddress().address()))
                .build());

        restClient.request("ping", HttpMethod.GET, null, null, null);

        RecordedRequest recordedRequest = mockServer.takeRequest();

        Assertions.assertThat(recordedRequest.getUrl()).isNotNull();
        // with mockwebserver3 getUrl() returns target URL not proxy URL
        // successful return implies proxy was used correctly.
        Assertions.assertThat(recordedRequest.getUrl().toString())
          .isEqualTo("http://foo.com:8086/ping"); // server is used as proxy
        Assertions.assertThat(recordedRequest.getRequestLine())
          .isEqualTo("GET http://foo.com:8086/ping HTTP/1.1");
    }


    @Test
    public void proxyUrl() throws InterruptedException {
        mockServer.enqueue(createResponse(200));

        restClient = new RestClient(new ClientConfig.Builder()
                .host("http://foo.com:8086")
                .proxyUrl(String.format("http://%s:%d", mockServer.getHostName(), mockServer.getPort()))
                .build());

        restClient.request("ping", HttpMethod.GET, null, null, null);

        RecordedRequest recordedRequest = mockServer.takeRequest();

        Assertions.assertThat(recordedRequest.getUrl()).isNotNull();
        // with mockwebserver3 getUrl() returns target URL not proxy URL
        // successful return implies proxy was used correctly.
        Assertions.assertThat(recordedRequest.getUrl().toString())
          .isEqualTo("http://foo.com:8086/ping"); // server is used as proxy
        Assertions.assertThat(recordedRequest.getRequestLine())
          .isEqualTo("GET http://foo.com:8086/ping HTTP/1.1");
    }


    @Test
    public void proxyWithAuthentication() throws InterruptedException {
        mockServer.enqueue(createResponse(407, Map.of("Proxy-Authenticate", "Basic"), null));
        mockServer.enqueue(createResponse(200));

        restClient = new RestClient(new ClientConfig.Builder()
                .host("http://foo.com:8086")
                .proxyUrl(String.format("http://%s:%d", mockServer.getHostName(), mockServer.getPort()))
                .authenticator(new Authenticator() {
                    @Override
                    protected PasswordAuthentication getPasswordAuthentication() {
                        return new PasswordAuthentication("john", "secret".toCharArray());
                    }
                })
                .build());

        restClient.request("ping", HttpMethod.GET, null, null, null);

        RecordedRequest recordedRequest = mockServer.takeRequest();
        RecordedRequest proxyAuthRequest = mockServer.takeRequest();

        Assertions.assertThat(recordedRequest.getUrl()).isNotNull();
       // with mockwebserver3 getUrl() returns target URL not proxy URL
       // successful return implies proxy was used correctly.
        Assertions.assertThat(recordedRequest.getUrl().toString()).isEqualTo("http://foo.com:8086/ping");
        Assertions.assertThat(recordedRequest.getRequestLine()).isEqualTo("GET http://foo.com:8086/ping HTTP/1.1");

        Assertions.assertThat(mockServer.getRequestCount()).isEqualTo(2);
        String proxyAuthorization = proxyAuthRequest.getHeaders().get("Proxy-Authorization");
        Assertions.assertThat(proxyAuthorization)
                .isEqualTo("Basic " + Base64.getEncoder().encodeToString("john:secret".getBytes()));
    }

    @Test
    public void error() {
        mockServer.enqueue(createResponse(404));

        restClient = new RestClient(new ClientConfig.Builder()
                .host(baseURL)
                .build());

        Assertions.assertThatThrownBy(
                        () -> restClient.request("ping", HttpMethod.GET, null, null, null))
                .isInstanceOf(InfluxDBApiException.class)
                .hasMessage("HTTP status code: 404; Message: Not Found");
    }

    @Test
    public void errorFromHeader() {

        mockServer.enqueue(createResponse(500, Map.of("X-Influx-Error", "not used"), null));

        restClient = new RestClient(new ClientConfig.Builder()
                .host(baseURL)
                .build());

        Assertions.assertThatThrownBy(
                        () -> restClient.request("ping", HttpMethod.GET, null, null, null))
                .isInstanceOf(InfluxDBApiException.class)
                .hasMessage("HTTP status code: 500; Message: not used");
    }

    @Test
    public void errorFromBody() {

      mockServer.enqueue(createResponse(401,
        "application/json",
        Map.of("X-Influx-Errpr", "not used"),
        "{\"message\":\"token does not have sufficient permissions\"}"));

      restClient = new RestClient(new ClientConfig.Builder()
              .host(baseURL)
              .build());

      Assertions.assertThatThrownBy(
                () -> restClient.request("ping", HttpMethod.GET, null, null, null)
        )
              .isInstanceOf(InfluxDBApiException.class)
              .hasMessage("HTTP status code: 401; Message: token does not have sufficient permissions");
    }

    @Test
    public void errorFromBodyIgnoredForNonJsonContentType() {
      mockServer.enqueue(createResponse(400,
        "text/plain",
        null,
        "{\"message\":\"token does not have sufficient permissions\"}"));

      restClient = new RestClient(new ClientConfig.Builder()
        .host(baseURL)
        .build());

      Assertions.assertThatThrownBy(
          () -> restClient.request("ping", HttpMethod.GET, null, null, null)
        )
        .isInstanceOf(InfluxDBApiException.class)
        .hasMessage("HTTP status code: 400; Message: {\"message\":\"token does not have sufficient permissions\"}");
    }

    @Test
    public void errorFromBodyInvalidJsonFallsBackToBody() {
      mockServer.enqueue(createResponse(400,
        "application/json",
        null,
        "{\"message\":\"token does not have sufficient permissions\""));

      restClient = new RestClient(new ClientConfig.Builder()
        .host(baseURL)
        .build());

      Assertions.assertThatThrownBy(
          () -> restClient.request("ping", HttpMethod.GET, null, null, null)
        )
        .isInstanceOf(InfluxDBApiException.class)
        .hasMessage("HTTP status code: 400; Message: {\"message\":\"token does not have sufficient permissions\"");
    }

    @Test
    public void errorFromBodyNullMessageFallsBackToError() {
      mockServer.enqueue(createResponse(400,
        "application/json",
        null,
        "{\"message\":null,\"error\":\"parsing failed\"}"));

      restClient = new RestClient(new ClientConfig.Builder()
        .host(baseURL)
        .build());

      Assertions.assertThatThrownBy(
          () -> restClient.request("ping", HttpMethod.GET, null, null, null)
        )
        .isInstanceOf(InfluxDBApiException.class)
        .hasMessage("HTTP status code: 400; Message: parsing failed");
    }

    @Test
    public void errorFromBodyEmptyMessageFallsBackToError() {
      mockServer.enqueue(createResponse(400,
        "application/json",
        null,
        "{\"message\":\"\",\"error\":\"parsing failed\"}"));

      restClient = new RestClient(new ClientConfig.Builder()
        .host(baseURL)
        .build());

      Assertions.assertThatThrownBy(
          () -> restClient.request("ping", HttpMethod.GET, null, null, null)
        )
        .isInstanceOf(InfluxDBApiException.class)
        .hasMessage("HTTP status code: 400; Message: parsing failed");
    }

    @Test
    public void errorFromBodyJsonArrayFallsBackToBody() {
      mockServer.enqueue(createResponse(400,
        "application/json",
        null,
        "[]"));

      restClient = new RestClient(new ClientConfig.Builder()
        .host(baseURL)
        .build());

      Assertions.assertThatThrownBy(
          () -> restClient.request("ping", HttpMethod.GET, null, null, null)
        )
        .isInstanceOf(InfluxDBApiException.class)
        .hasMessage("HTTP status code: 400; Message: []");
    }

    @Test
    public void errorFromBodyV3WithoutMessageAndEmptyContentType() {

      mockServer.enqueue(createResponse(400,
        "",
        null,
        "{\"error\":\"parsing failed\"}"));

      restClient = new RestClient(new ClientConfig.Builder()
                .host(baseURL)
                .build());

      Assertions.assertThatThrownBy(
                    () -> restClient.request("ping", HttpMethod.GET, null, null, null)
        )
              .isInstanceOf(InfluxDBApiException.class)
              .hasMessage("HTTP status code: 400; Message: parsing failed");
    }

    @Test
    public void errorFromBodyV3WithoutMessageAndWithoutContentType() {

      mockServer.enqueue(createResponse(400,
        null,
        null,
        "{\"error\":\"parsing failed\"}"));

      restClient = new RestClient(new ClientConfig.Builder()
                .host(baseURL)
                .build());

      Assertions.assertThatThrownBy(
                    () -> restClient.request("ping", HttpMethod.GET, null, null, null)
        )
              .isInstanceOf(InfluxDBApiException.class)
              .hasMessage("HTTP status code: 400; Message: parsing failed");
    }

    @Test
    public void errorFromBodyV3WithDataObject() { // Core/Enterprise object format

      mockServer.enqueue(createResponse(400,
        "application/json",
        null,
        "{\"error\":\"parsing failed\",\"data\":{\"error_message\":\"invalid field value\"}}"));

      restClient = new RestClient(new ClientConfig.Builder()
              .host(baseURL)
              .build());

      Assertions.assertThatThrownBy(
            () -> restClient.request("ping", HttpMethod.GET, null, null, null)
        )
              .isInstanceOf(InfluxDBApiException.class)
              .hasMessage("HTTP status code: 400; Message: invalid field value");
    }

    @Test
    public void errorFromBodyV3WithDataArray() {
      mockServer.enqueue(createResponse(400,
        "application/json",
        null,
        "{\"error\":\"partial write of line protocol occurred\",\"data\":[{\"error_message\":"
          + "\"invalid column type for column 'v', expected iox::column_type::field::integer,"
          + " got iox::column_type::field::float\",\"line_number\":2,"
          + "\"original_line\":\"testa6a3ad v=1 17702\"}]}"));

      restClient = new RestClient(new ClientConfig.Builder()
        .host(baseURL)
        .build());

      Assertions.assertThatThrownBy(
          () -> restClient.request("ping", HttpMethod.GET, null, null, null)
        )
        .isInstanceOf(InfluxDBApiException.class)
        .hasMessage("HTTP status code: 400; Message: partial write of line protocol occurred:\n"
          + "\tline 2: invalid column type for column 'v', expected iox::column_type::field::integer,"
          + " got iox::column_type::field::float (testa6a3ad v=1 17702)");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("errorFromBodyV3WithDataArrayCases")
    public void errorFromBodyV3WithDataArrayCase(final String testName,
                                                  final String body,
                                                  final String expectedMessage) {

      mockServer.enqueue(createResponse(400,
        "application/json",
        null,
        body));

      restClient = new RestClient(new ClientConfig.Builder()
        .host(baseURL)
        .build());

      Assertions.assertThatThrownBy(
          () -> restClient.request("ping", HttpMethod.GET, null, null, null)
        )
        .isInstanceOf(InfluxDBApiException.class)
        .hasMessage(expectedMessage);
    }

    private static Stream<Arguments> errorFromBodyV3WithDataArrayCases() {
      return Stream.of(
        Arguments.of(
          "message-only detail",
          "{\"error\":\"partial write of line protocol occurred\",\"data\":[{\"error_message\":"
            + "\"only error message\"}]}",
          "HTTP status code: 400; Message: partial write of line protocol occurred:\n\tonly error message"
        ),
        Arguments.of(
          "non-object item skipped",
          "{\"error\":\"partial write of line protocol occurred\",\"data\":[null,{\"error_message\":"
            + "\"bad line\",\"line_number\":2,\"original_line\":\"bad lp\"}]}",
          "HTTP status code: 400; Message: partial write of line protocol occurred:\n"
            + "\tline 2: bad line (bad lp)"
        ),
        Arguments.of(
          "no detail fields",
          "{\"error\":\"partial write of line protocol occurred\",\"data\":[{\"line_number\":2}]}",
          "HTTP status code: 400; Message: partial write of line protocol occurred"
        ),
        Arguments.of(
          "empty error_message skipped",
          "{\"error\":\"partial write of line protocol occurred\",\"data\":[{\"error_message\":\"\"},"
            + "{\"error_message\":\"bad line\",\"line_number\":2,\"original_line\":\"bad lp\"}]}",
          "HTTP status code: 400; Message: partial write of line protocol occurred:\n"
            + "\tline 2: bad line (bad lp)"
        ),
        Arguments.of(
          "non-object primitive item skipped",
          "{\"error\":\"partial write of line protocol occurred\",\"data\":[1,{\"error_message\":"
            + "\"bad line\",\"line_number\":2,\"original_line\":\"bad lp\"}]}",
          "HTTP status code: 400; Message: partial write of line protocol occurred:\n"
            + "\tline 2: bad line (bad lp)"
        ),
        Arguments.of(
          "null error_message skipped",
          "{\"error\":\"partial write of line protocol occurred\",\"data\":[{\"error_message\":null},"
            + "{\"error_message\":\"bad line\",\"line_number\":2,\"original_line\":\"bad lp\"}]}",
          "HTTP status code: 400; Message: partial write of line protocol occurred:\n"
            + "\tline 2: bad line (bad lp)"
        ),
        Arguments.of(
          "empty original_line uses message-only detail",
          "{\"error\":\"partial write of line protocol occurred\",\"data\":[{\"error_message\":"
            + "\"only error message\",\"line_number\":2,\"original_line\":\"\"}]}",
          "HTTP status code: 400; Message: partial write of line protocol occurred:\n\tonly error message"
        )
      );
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("errorFromBodyV3FallbackCases")
    public void errorFromBodyV3FallbackCase(final String testName,
                                            final String body,
                                            final String expectedMessage) {

      mockServer.enqueue(createResponse(400,
        "application/json",
        null,
        body));

      restClient = new RestClient(new ClientConfig.Builder()
        .host(baseURL)
        .build());

      Assertions.assertThatThrownBy(
          () -> restClient.request("ping", HttpMethod.GET, null, null, null)
        )
        .isInstanceOf(InfluxDBApiException.class)
        .hasMessage(expectedMessage);
    }

    private static Stream<Arguments> errorFromBodyV3FallbackCases() {
      return Stream.of(
        Arguments.of(
          "missing error with data array falls back to body",
          "{\"data\":[{\"error_message\":\"bad line\",\"line_number\":2,\"original_line\":\"bad lp\"}]}",
          "HTTP status code: 400; Message: "
            + "{\"data\":[{\"error_message\":\"bad line\",\"line_number\":2,\"original_line\":\"bad lp\"}]}"
        ),
        Arguments.of(
          "empty error with data array falls back to body",
          "{\"error\":\"\",\"data\":[{\"error_message\":\"bad line\",\"line_number\":2,\"original_line\":"
            + "\"bad lp\"}]}",
          "HTTP status code: 400; Message: "
            + "{\"error\":\"\",\"data\":[{\"error_message\":\"bad line\",\"line_number\":2,\"original_line\":"
            + "\"bad lp\"}]}"
        ),
        Arguments.of(
          "data object without error_message falls back to error",
          "{\"error\":\"parsing failed\",\"data\":{}}",
          "HTTP status code: 400; Message: parsing failed"
        ),
        Arguments.of(
          "data object with empty error_message falls back to error",
          "{\"error\":\"parsing failed\",\"data\":{\"error_message\":\"\"}}",
          "HTTP status code: 400; Message: parsing failed"
        ),
        Arguments.of(
          "data string falls back to error",
          "{\"error\":\"parsing failed\",\"data\":\"not-an-object\"}",
          "HTTP status code: 400; Message: parsing failed"
        ),
        Arguments.of(
          "data number falls back to error",
          "{\"error\":\"parsing failed\",\"data\":123}",
          "HTTP status code: 400; Message: parsing failed"
        )
      );
    }

    @Test
    public void errorFromBodyText() {

      mockServer.enqueue(createResponse(402, null, "token is over the limit"));

      restClient = new RestClient(new ClientConfig.Builder()
              .host(baseURL)
              .build());

      Assertions.assertThatThrownBy(
         () -> restClient.request("ping", HttpMethod.GET, null, null, null)
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
    public void errorHttpExceptionThrown() {
        String retryDate = Instant.now().plus(300, ChronoUnit.SECONDS).toString();

      mockServer.enqueue(createResponse(503,
        "application/json",
        Map.of("retry-after", retryDate),
        "{\"message\":\"temporarily offline\"}"));

      restClient = new RestClient(new ClientConfig.Builder()
          .host(baseURL)
          .build());

        Throwable thrown = catchThrowable(() -> restClient.request(
          "/api/v2/write", HttpMethod.POST, null, null, null)
        );

        Assertions.assertThat(thrown).isNotNull();
        Assertions.assertThat(thrown).isInstanceOf(InfluxDBApiHttpException.class);
        InfluxDBApiHttpException he = (InfluxDBApiHttpException) thrown;
        Assertions.assertThat(he.headers()).isNotNull();
        Assertions.assertThat(he.getHeader("retry-after").get(0))
          .isNotNull().isEqualTo(retryDate);
        Assertions.assertThat(he.getHeader("content-type").get(0))
          .isNotNull().isEqualTo("application/json");
        Assertions.assertThat(he.getHeader("wumpus")).isNull();
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
    public void getServerVersionError() {
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
    public void getServerVersionErrorNoBody() {
        mockServer.enqueue(new MockResponse(200, Headers.of(), "Test-Version"));
        restClient = new RestClient(new ClientConfig.Builder()
                .host(baseURL)
                .build());
        String version = restClient.getServerVersion();
        Assertions.assertThat(version).isEqualTo(null);
    }
}
