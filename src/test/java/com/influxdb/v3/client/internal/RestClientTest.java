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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import io.netty.handler.codec.http.HttpMethod;
import mockwebserver3.MockResponse;
import mockwebserver3.RecordedRequest;
import okhttp3.Headers;
import org.assertj.core.api.Assertions;
import org.jspecify.annotations.NonNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import com.influxdb.v3.client.AbstractMockServerTest;
import com.influxdb.v3.client.InfluxDBApiException;
import com.influxdb.v3.client.InfluxDBApiHttpException;
import com.influxdb.v3.client.InfluxDBClient;
import com.influxdb.v3.client.InfluxDBPartialWriteException;
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

    @ParameterizedTest(name = "{0}")
    @MethodSource("errorFromBodyScalarMessageCases")
    public void errorFromBodyScalarMessage(final String testName,
                                           final String responseBody,
                                           final String expectedMessage) {

      mockServer.enqueue(createResponse(401,
        "application/json",
        null,
        responseBody));

      restClient = new RestClient(new ClientConfig.Builder()
              .host(baseURL)
              .build());

      Assertions.assertThatThrownBy(
         () -> restClient.request("ping", HttpMethod.GET, null, null, null)
      )
        .isInstanceOf(InfluxDBApiException.class)
        .hasMessage(expectedMessage);
    }

    private static Stream<Arguments> errorFromBodyScalarMessageCases() {
      return Stream.of(
        Arguments.of(
          "numeric message",
          "{\"message\":123}",
          "HTTP status code: 401; Message: 123"),
        Arguments.of(
          "boolean message",
          "{\"message\":true}",
          "HTTP status code: 401; Message: true")
      );
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
        "{\"error\":\"parsing failed for write_lp endpoint\",\"data\":{\"error_message\":\"invalid field value\"}}"));

      restClient = new RestClient(new ClientConfig.Builder()
              .host(baseURL)
              .build());

      Throwable thrown = catchThrowable(() -> restClient.request("api/v3/write_lp", HttpMethod.POST, null, null, null));
      Assertions.assertThat(thrown)
              .isInstanceOf(InfluxDBApiHttpException.class)
              .hasMessage("HTTP status code: 400; Message: parsing failed for write_lp endpoint:\n"
                      + "\tinvalid field value");
    }

    @Test
    public void partialErrorFromBodyV3WithDataArray() {
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

        Throwable thrown = catchThrowable(() -> restClient.request("api/v3/write_lp", HttpMethod.POST, null,
                null, null, true, false));
      Assertions.assertThat(thrown)
              .isInstanceOf(InfluxDBPartialWriteException.class)
              .hasMessage("HTTP status code: 400; Message: partial write of line protocol occurred:\n"
                      + "\tline 2: invalid column type for column 'v', expected iox::column_type::field::integer,"
                      + " got iox::column_type::field::float (testa6a3ad v=1 17702)");

      InfluxDBPartialWriteException partialWriteException = (InfluxDBPartialWriteException) thrown;
      Assertions.assertThat(partialWriteException.lineErrors()).hasSize(1);
      InfluxDBPartialWriteException.LineError lineError = partialWriteException.lineErrors().get(0);
      Assertions.assertThat(lineError.lineNumber()).isEqualTo(2);
      Assertions.assertThat(lineError.errorMessage())
              .isEqualTo("invalid column type for column 'v', expected iox::column_type::field::integer,"
                      + " got iox::column_type::field::float");
      Assertions.assertThat(lineError.originalLine()).isEqualTo("testa6a3ad v=1 17702");
    }

    @Test
    public void partialErrorFromBodyV3WithInvalidDataArray() {
      mockServer.enqueue(createResponse(400,
        "application/json",
        null,
        "{\"error\":\"partial write of line protocol occurred\",\"data\":[{\"error_message\":"
          + "\"bad line\",\"line_number\":2,\"original_line\":\"bad lp\"},"
          + "{\"error_message\":\"bad line 2\",\"line_number\":\"x\",\"original_line\":\"bad lp 2\"}]}"));

      restClient = new RestClient(new ClientConfig.Builder()
        .host(baseURL)
        .build());

     Throwable thrown = catchThrowable(() -> restClient.request("api/v3/write_lp", HttpMethod.POST, null,
              null, null, true, false));
      Assertions.assertThat(thrown)
        .isInstanceOf(InfluxDBPartialWriteException.class)
        .hasMessage("HTTP status code: 400; Message: partial write of line protocol occurred:\n"
          + "\t{\"error_message\":\"bad line\",\"line_number\":2,\"original_line\":\"bad lp\"}\n"
          + "\t{\"error_message\":\"bad line 2\",\"line_number\":\"x\",\"original_line\":\"bad lp 2\"}");
    }

    private static final String REJECTED_LINE = "home,room=Sunroom temp=\"hi\" 1735545610";
    private static final String REJECTED_LINE_JSON = "home,room=Sunroom temp=\\\"hi\\\" 1735545610";
    private static final String LINE_ERROR = "invalid column type for column 'temp', expected "
            + "iox::column_type::field::float, got iox::column_type::field::string";

    private List<PartialWriteTestCase> testCases() {
        return List.of(
                new PartialWriteTestCase(
                        "V3 accept partial with renamed error and non-empty array",
                        400,
                        "application/json",
                        "{\"error\":\"write completed with rejected rows\","
                                + "\"data\":[{\"error_message\":\"" + LINE_ERROR + "\",\"line_number\":2,"
                                + "\"original_line\":\"" + REJECTED_LINE_JSON + "\"}]}",
                        false,
                        true,
                        "HTTP status code: 400; Message: write completed with rejected rows:"
                                + "\n\tline 2: " + LINE_ERROR + " (" + REJECTED_LINE + ")",
                        true,
                        List.of(new InfluxDBPartialWriteException.LineError(2, LINE_ERROR, REJECTED_LINE))
                ),
                new PartialWriteTestCase(
                        "V3 accept partial without content type",
                        400,
                        null,
                        "{\"error\":\"write completed with rejected rows\","
                                + "\"data\":[{\"error_message\":\"" + LINE_ERROR + "\",\"line_number\":2,"
                                + "\"original_line\":\"" + REJECTED_LINE_JSON + "\"}]}",
                        false,
                        true,
                        "HTTP status code: 400; Message: write completed with rejected rows:"
                                + "\n\tline 2: " + LINE_ERROR + " (" + REJECTED_LINE + ")",
                        true,
                        List.of(new InfluxDBPartialWriteException.LineError(2, LINE_ERROR, REJECTED_LINE))
                ),
                new PartialWriteTestCase(
                        "V3 accept partial with malformed non-empty array",
                        400,
                        "application/json",
                        "{\"error\":\"write completed with rejected rows\","
                                + "\"data\":[{\"line_number\":\"invalid\","
                                + "\"original_line\":\"" + REJECTED_LINE_JSON + "\"}]}",
                        false,
                        true,
                        "HTTP status code: 400; Message: write completed with rejected rows:"
                                + "\n\t{\"line_number\":\"invalid\",\"original_line\":\"" + REJECTED_LINE_JSON
                                + "\"}",
                        true,
                        Collections.emptyList()
                ),
                new PartialWriteTestCase(
                        "V3 accept partial with mixed primitive and typed entries",
                        400,
                        "application/json",
                        "{\"error\":\"write completed with rejected rows\","
                                + "\"data\":[1,{\"error_message\":\"" + LINE_ERROR + "\",\"line_number\":2,"
                                + "\"original_line\":\"" + REJECTED_LINE_JSON + "\"}]}",
                        false,
                        true,
                        "HTTP status code: 400; Message: write completed with rejected rows:"
                                + "\n\t1\n\t{\"error_message\":\"" + LINE_ERROR
                                + "\",\"line_number\":2,\"original_line\":\"" + REJECTED_LINE_JSON + "\"}",
                        true,
                        List.of(new InfluxDBPartialWriteException.LineError(2, LINE_ERROR, REJECTED_LINE))
                ),
                new PartialWriteTestCase(
                        "V3 accept partial with string entries",
                        400,
                        "application/json",
                        "{\"error\":\"write completed with rejected rows\","
                                + "\"data\":[\"" + REJECTED_LINE_JSON + "\"]}",
                        false,
                        true,
                        "HTTP status code: 400; Message: write completed with rejected rows:"
                                + "\n\t\"" + REJECTED_LINE_JSON + "\"",
                        true,
                        Collections.emptyList()
                ),
                new PartialWriteTestCase(
                        "V3 accept partial with error message only",
                        400,
                        "application/json",
                        "{\"error\":\"write completed with rejected rows\","
                                + "\"data\":[{\"error_message\":\"" + LINE_ERROR + "\"}]}",
                        false,
                        true,
                        "HTTP status code: 400; Message: write completed with rejected rows:"
                                + "\n\t" + LINE_ERROR,
                        true,
                        List.of(new InfluxDBPartialWriteException.LineError(null, LINE_ERROR, null))
                ),
                new PartialWriteTestCase(
                        "V3 accept partial with line number but no original line",
                        400,
                        "application/json",
                        "{\"error\":\"write completed with rejected rows\","
                                + "\"data\":[{\"error_message\":\"" + LINE_ERROR + "\",\"line_number\":2}]}",
                        false,
                        true,
                        "HTTP status code: 400; Message: write completed with rejected rows:\n\tline 2: "
                                + LINE_ERROR,
                        true,
                        List.of(new InfluxDBPartialWriteException.LineError(2, LINE_ERROR, null))
                ),
                new PartialWriteTestCase(
                        "V3 accept partial with entry missing error message",
                        400,
                        "application/json",
                        "{\"error\":\"write completed with rejected rows\","
                                + "\"data\":[{\"line_number\":2,\"original_line\":\"" + REJECTED_LINE_JSON + "\"}]}",
                        false,
                        true,
                        "HTTP status code: 400; Message: write completed with rejected rows:"
                                + "\n\t{\"line_number\":2,\"original_line\":\"" + REJECTED_LINE_JSON + "\"}",
                        true,
                        Collections.emptyList()
                ),
                new PartialWriteTestCase("V3 accept partial with empty array", 400, "application/json",
                        "{\"error\":\"write failed\",\"data\":[]}",
                        false,
                        true,
                        "HTTP status code: 400; Message: write failed",
                        true
                ),
                new PartialWriteTestCase("V3 accept partial with object details remains generic", 400,
                        "application/json",
                        "{\"error\":\"line protocol parsing error\",\"data\":{\"error_message\":\""
                                + LINE_ERROR + "\",\"line_number\":2,\"original_line\":\""
                                + REJECTED_LINE_JSON + "\"}}",
                        false,
                        true,
                        "HTTP status code: 400; Message: line protocol parsing error:\n\tline 2: "
                                + LINE_ERROR + " (" + REJECTED_LINE + ")",
                        false
                ),
                new PartialWriteTestCase("V3 reject partial with object details", 400, "application/json",
                        "{\"error\":\"line protocol parsing error\",\"data\":{\"error_message\":\""
                                + LINE_ERROR + "\",\"line_number\":2,\"original_line\":\""
                                + REJECTED_LINE_JSON + "\"}}",
                        false,
                        false,
                        "HTTP status code: 400; Message: line protocol parsing error:\n\tline 2: "
                                + LINE_ERROR + " (" + REJECTED_LINE + ")",
                        false
                ),
                new PartialWriteTestCase("V2 never returns partial write error", 400, "application/json",
                        "{\"error\":\"partial write of line protocol occurred\","
                                + "\"data\":[{\"error_message\":\""
                                + LINE_ERROR + "\",\"line_number\":2,\"original_line\":\""
                                + REJECTED_LINE_JSON + "\"}]}",
                        true,
                        true,
                        "HTTP status code: 400; Message: partial write of line protocol occurred",
                        false
                ),
                new PartialWriteTestCase("V3 non-400 never returns partial write error", 500,
                        "application/json",
                        "{\"error\":\"partial write of line protocol occurred\","
                                + "\"data\":[{\"error_message\":\""
                                + LINE_ERROR + "\",\"line_number\":2,\"original_line\":\"" + REJECTED_LINE_JSON
                                + "\"}]}",
                        false,
                        true,
                        "HTTP status code: 500; Message: partial write of line protocol occurred",
                        false
                ),
                new PartialWriteTestCase("V3 scalar data remains generic", 400, "application/json",
                        "{\"error\":\"write failed\",\"data\":\"invalid\"}",
                        false,
                        true,
                        "HTTP status code: 400; Message: write failed",
                        false
                ),
                new PartialWriteTestCase("V3 empty object data remains generic", 400, "application/json",
                        "{\"error\":\"write failed\",\"data\":{}}",
                        false,
                        true,
                        "HTTP status code: 400; Message: write failed",
                        false
                ),
                new PartialWriteTestCase("V3 null data remains generic", 400, "application/json",
                        "{\"error\":\"write failed\",\"data\":null}",
                        false,
                        true,
                        "HTTP status code: 400; Message: write failed",
                        false
                ),
                new PartialWriteTestCase("V3 malformed JSON preserves raw response", 400,
                        "application/json",
                        "{\"error\":\"write failed\"",
                        false,
                        true,
                        "HTTP status code: 400; Message: {\"error\":\"write failed\"",
                        false
                )
        );
    }

    @Test
    public void testPartialWriteException() {
        for (PartialWriteTestCase testCase : testCases()) {
            mockServer.enqueue(createResponse(testCase.statusCode(),
                    testCase.contentType(),
                    null,
                    testCase.responseBody()));
            restClient = new RestClient(new ClientConfig.Builder()
                    .host(baseURL)
                    .build());
            Throwable thrown = catchThrowable(() -> restClient.request("api/v3/write_lp", HttpMethod.POST,
                    null, null, null, testCase.acceptPartial(), testCase.useV2Api()));

            Assertions.assertThat(thrown).as(testCase.name()).isNotNull();
            Assertions.assertThat(thrown.getMessage()).as(testCase.name()).isEqualTo(testCase.expectedMsg());
            if (testCase.expectPartial()) {
                Assertions.assertThat(thrown).as(testCase.name()).isInstanceOf(InfluxDBPartialWriteException.class);
                InfluxDBPartialWriteException partial = (InfluxDBPartialWriteException) thrown;
                Assertions.assertThat(partial.lineErrors())
                        .as(testCase.name())
                        .containsExactlyElementsOf(testCase.expectedLines());
            } else {
                Assertions.assertThat(thrown).as(testCase.name()).isInstanceOf(InfluxDBApiHttpException.class);
            }
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("errorFromBodyV3FallbackCases")
    public void errorFromBodyV3FallbackCase(final String testName,
                                            final String requestPath,
                                            final String contentType,
                                            final String body,
                                            final Class<? extends InfluxDBApiException> expectedClass,
                                            final String expectedMessage) {

      mockServer.enqueue(createResponse(400,
        contentType,
        null,
        body));

      restClient = new RestClient(new ClientConfig.Builder()
        .host(baseURL)
        .build());

      Throwable thrown = catchThrowable(() ->
              restClient.request(requestPath, HttpMethod.GET, null, null, null));
      Assertions.assertThat(thrown)
              .isInstanceOf(expectedClass)
              .hasMessage(expectedMessage);
    }

    private static Stream<Arguments> errorFromBodyV3FallbackCases() {
      return Stream.of(
        Arguments.of(
          "missing error with data array falls back to body",
          "ping",
          "application/json",
          "{\"data\":[{\"error_message\":\"bad line\",\"line_number\":2,\"original_line\":\"bad lp\"}]}",
          InfluxDBApiHttpException.class,
          "HTTP status code: 400; Message: "
            + "{\"data\":[{\"error_message\":\"bad line\",\"line_number\":2,\"original_line\":\"bad lp\"}]}"
        ),
        Arguments.of(
          "empty error with data array falls back to body",
          "ping",
          "application/json",
          "{\"error\":\"\",\"data\":[{\"error_message\":\"bad line\",\"line_number\":2,\"original_line\":"
            + "\"bad lp\"}]}",
          InfluxDBApiHttpException.class,
          "HTTP status code: 400; Message: "
            + "{\"error\":\"\",\"data\":[{\"error_message\":\"bad line\",\"line_number\":2,\"original_line\":"
            + "\"bad lp\"}]}"
        ),
        Arguments.of(
          "data object without error_message falls back to error",
          "ping",
          "application/json",
          "{\"error\":\"parsing failed\",\"data\":{}}",
          InfluxDBApiHttpException.class,
          "HTTP status code: 400; Message: parsing failed"
        ),
        Arguments.of(
          "data object with empty error_message falls back to error",
          "ping",
          "application/json",
          "{\"error\":\"parsing failed\",\"data\":{\"error_message\":\"\"}}",
          InfluxDBApiHttpException.class,
          "HTTP status code: 400; Message: parsing failed"
        ),
        Arguments.of(
          "data string falls back to error",
          "ping",
          "application/json",
          "{\"error\":\"parsing failed\",\"data\":\"not-an-object\"}",
          InfluxDBApiHttpException.class,
          "HTTP status code: 400; Message: parsing failed"
        ),
        Arguments.of(
          "data number falls back to error",
          "ping",
          "application/json",
          "{\"error\":\"parsing failed\",\"data\":123}",
          InfluxDBApiHttpException.class,
          "HTTP status code: 400; Message: parsing failed"
        ),
        Arguments.of(
          "partial-write with invalid data string falls back to error",
          "ping",
          "application/json",
          "{\"error\":\"partial write of line protocol occurred\",\"data\":\"invalid\"}",
          InfluxDBApiHttpException.class,
          "HTTP status code: 400; Message: partial write of line protocol occurred"
        ),
        Arguments.of(
          "partial-write with empty data object falls back to error",
          "ping",
          "application/json",
          "{\"error\":\"partial write of line protocol occurred\",\"data\":{}}",
          InfluxDBApiHttpException.class,
          "HTTP status code: 400; Message: partial write of line protocol occurred"
        ),
        Arguments.of(
          "write endpoint ignores line-error parsing for non-json content type",
          "api/v3/write_lp",
          "text/plain",
          "{\"error\":\"partial write of line protocol occurred\",\"data\":[{\"error_message\":\"bad line\","
            + "\"line_number\":2,\"original_line\":\"bad lp\"}]}",
          InfluxDBApiHttpException.class,
          "HTTP status code: 400; Message: "
            + "{\"error\":\"partial write of line protocol occurred\",\"data\":[{\"error_message\":\"bad line\","
            + "\"line_number\":2,\"original_line\":\"bad lp\"}]}"
        ),
        Arguments.of(
          "write endpoint with non-object root falls back to body",
          "api/v3/write_lp",
          "application/json",
          "[]",
          InfluxDBApiHttpException.class,
          "HTTP status code: 400; Message: []"
        ),
        Arguments.of(
          "write endpoint with invalid line-error object type falls back to http exception",
          "api/v3/write_lp",
          "application/json",
          "{\"error\":\"partial write of line protocol occurred\",\"data\":{\"error_message\":\"bad line\","
            + "\"line_number\":{\"x\":2},\"original_line\":\"bad lp\"}}",
          InfluxDBApiHttpException.class,
          "HTTP status code: 400; Message: partial write of line protocol occurred:\n\tbad line"
        ),
        Arguments.of(
          "write endpoint with scalar data falls back to error",
          "api/v3/write_lp",
          "application/json",
          "{\"error\":\"partial write of line protocol occurred\",\"data\":123}",
          InfluxDBApiHttpException.class,
          "HTTP status code: 400; Message: partial write of line protocol occurred"
        ),
        Arguments.of(
          "write endpoint invalid json body falls back to raw body",
          "api/v3/write_lp",
          "application/json",
          "{\"error\":\"partial write of line protocol occurred\"",
          InfluxDBApiHttpException.class,
          "HTTP status code: 400; Message: {\"error\":\"partial write of line protocol occurred\""
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

record PartialWriteTestCase(
        String name,
        int statusCode,
        String contentType,
        String responseBody,
        boolean useV2Api,
        boolean acceptPartial,
        String expectedMsg,
        boolean expectPartial,
        List<InfluxDBPartialWriteException.LineError> expectedLines
) {
    PartialWriteTestCase(final String name, final
                         int statusCode,
                         final String contentType,
                         final String responseBody,
                         final boolean useV2Api,
                         final boolean acceptPartial,
                         final String expectedMsg,
                         final boolean expectPartial) {
        this(name,
                statusCode,
                contentType,
                responseBody,
                useV2Api,
                acceptPartial,
                expectedMsg,
                expectPartial,
                Collections.emptyList());
    }

    @Override
    public @NonNull String toString() {
        return name;
    }
}