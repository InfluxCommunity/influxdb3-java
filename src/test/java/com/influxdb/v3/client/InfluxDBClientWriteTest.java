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

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;

import io.netty.handler.codec.http.HttpResponseStatus;
import mockwebserver3.RecordedRequest;
import okhttp3.HttpUrl;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.influxdb.v3.client.config.ClientConfig;
import com.influxdb.v3.client.write.WriteOptions;
import com.influxdb.v3.client.write.WritePrecision;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.catchThrowable;

class InfluxDBClientWriteTest extends AbstractMockServerTest {

    private InfluxDBClient client;

    @BeforeEach
    void initClient() {
        client = InfluxDBClient.getInstance(baseURL, "my-token".toCharArray(), "my-database");
    }

    @AfterEach
    void closeClient() throws Exception {
        if (client != null) {
            client.close();
        }
    }

    @Test
    void writeToClosedClient() throws Exception {

        client.close();
        Assertions.assertThatThrownBy(() -> client.writeRecord("mem,tag=one value=1.0"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("InfluxDBClient has been closed.");
    }

    @Test
    void writeEmptyBatch() {
        mockServer.enqueue(createResponse(200));

        client.writeRecords(Collections.singletonList(null));

        assertThat(mockServer.getRequestCount()).isEqualTo(0);
    }

    @Test
    void writeNullRecord() {
        mockServer.enqueue(createResponse(200));

        client.writeRecord(null);

        assertThat(mockServer.getRequestCount()).isEqualTo(0);
    }

    @Test
    void writeNullPoint() {
        mockServer.enqueue(createResponse(200));

        client.writePoint(null);

        assertThat(mockServer.getRequestCount()).isEqualTo(0);
    }

    @Test
    void databaseParameter() throws InterruptedException {
        mockServer.enqueue(createResponse(200));

        client.writeRecord("mem,tag=one value=1.0");

        assertThat(mockServer.getRequestCount()).isEqualTo(1);
        RecordedRequest request = mockServer.takeRequest();
        assertThat(request).isNotNull();
        assertThat(request.getUrl()).isNotNull();
        assertThat(request.getUrl().queryParameter("bucket")).isEqualTo("my-database");
    }

    @Test
    void databaseParameterSpecified() throws InterruptedException {
        mockServer.enqueue(createResponse(200));

        client.writeRecord("mem,tag=one value=1.0", new WriteOptions.Builder().database("my-database-2").build());

        assertThat(mockServer.getRequestCount()).isEqualTo(1);
        RecordedRequest request = mockServer.takeRequest();
        assertThat(request).isNotNull();
        assertThat(request.getUrl()).isNotNull();
        assertThat(request.getUrl().queryParameter("bucket")).isEqualTo("my-database-2");
    }

    @Test
    void databaseParameterRequired() throws Exception {
        client.close();
        client = InfluxDBClient.getInstance(baseURL, null, null);
        mockServer.enqueue(createResponse(200));

        Assertions.assertThatThrownBy(() -> client.writeRecord("mem,tag=one value=1.0"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Please specify the 'Database' as a method parameter or use "
                        + "default configuration at 'ClientConfig.database'.");

        assertThat(mockServer.getRequestCount()).isEqualTo(0);
    }


    @Test
    void precisionParameter() throws InterruptedException {
        mockServer.enqueue(createResponse(200));

        client.writeRecord("mem,tag=one value=1.0");

        assertThat(mockServer.getRequestCount()).isEqualTo(1);
        RecordedRequest request = mockServer.takeRequest();
        assertThat(request).isNotNull();
        assertThat(request.getUrl()).isNotNull();
        assertThat(request.getUrl().queryParameter("precision")).isEqualTo("ns");
    }

    @Test
    void precisionParameterSpecified() throws InterruptedException {
        mockServer.enqueue(createResponse(200));

        client.writeRecord("mem,tag=one value=1.0", new WriteOptions.Builder().precision(WritePrecision.S).build());

        assertThat(mockServer.getRequestCount()).isEqualTo(1);
        RecordedRequest request = mockServer.takeRequest();
        assertThat(request).isNotNull();
        assertThat(request.getUrl()).isNotNull();
        assertThat(request.getUrl().queryParameter("precision")).isEqualTo("s");
    }

    @Test
    void gzipParameter() throws InterruptedException {
        mockServer.enqueue(createResponse(200));

        client.writeRecord("mem,tag=one value=1.0");

        assertThat(mockServer.getRequestCount()).isEqualTo(1);
        RecordedRequest request = mockServer.takeRequest();
        assertThat(request).isNotNull();
        assertThat(request.getUrl()).isNotNull();
        assertThat(request.getHeaders().get("Content-Type")).isEqualTo("text/plain; charset=utf-8");
        assertThat(request.getHeaders().get("Content-Encoding")).isNotEqualTo("gzip");
    }

    @Test
    void gzipParameterSpecified() throws InterruptedException {
        mockServer.enqueue(createResponse(200));

        client.writeRecord("mem,tag=one value=1.0", new WriteOptions.Builder().gzipThreshold(1).build());

        assertThat(mockServer.getRequestCount()).isEqualTo(1);
        RecordedRequest request = mockServer.takeRequest();
        assertThat(request).isNotNull();
        assertThat(request.getUrl()).isNotNull();
        assertThat(request.getHeaders().get("Content-Type")).isEqualTo("text/plain; charset=utf-8");
        assertThat(request.getHeaders().get("Content-Encoding")).isEqualTo("gzip");
    }

    @Test
    void writeNoSyncFalseUsesV2API() throws InterruptedException {
        mockServer.enqueue(createResponse(200));

        client.writeRecord("mem,tag=one value=1.0",
                new WriteOptions.Builder().precision(WritePrecision.NS).noSync(false).build());

        assertThat(mockServer.getRequestCount()).isEqualTo(1);
        RecordedRequest request = mockServer.takeRequest();
        assertThat(request).isNotNull();
        assertThat(request.getUrl()).isNotNull();
        assertThat(request.getUrl().encodedPath()).isEqualTo("/api/v2/write");
        assertThat(request.getUrl().queryParameter("no_sync")).isNull();
        assertThat(request.getUrl().queryParameter("precision")).isEqualTo("ns");

    }

    @Test
    void writeNoSyncTrueUsesV3API() throws InterruptedException {
        mockServer.enqueue(createResponse(200));

        client.writeRecord("mem,tag=one value=1.0",
                new WriteOptions.Builder().precision(WritePrecision.NS).noSync(true).build());

        assertThat(mockServer.getRequestCount()).isEqualTo(1);
        RecordedRequest request = mockServer.takeRequest();
        assertThat(request).isNotNull();
        assertThat(request.getUrl()).isNotNull();
        assertThat(request.getUrl().encodedPath()).isEqualTo("/api/v3/write_lp");
        assertThat(request.getUrl().queryParameter("no_sync")).isEqualTo("true");
        assertThat(request.getUrl().queryParameter("precision")).isEqualTo("nanosecond");
    }

    @Test
    void writeNoSyncTrueOnV2ServerThrowsException() throws InterruptedException {
        mockServer.enqueue(createEmptyResponse(HttpResponseStatus.METHOD_NOT_ALLOWED.code()));

        InfluxDBApiHttpException ae = org.junit.jupiter.api.Assertions.assertThrows(InfluxDBApiHttpException.class,
                () -> client.writeRecord("mem,tag=one value=1.0",
                        new WriteOptions.Builder().precision(WritePrecision.MS).noSync(true).build())
        );

        assertThat(mockServer.getRequestCount()).isEqualTo(1);
        RecordedRequest request = mockServer.takeRequest();
        assertThat(request).isNotNull();
        assertThat(request.getUrl()).isNotNull();
        assertThat(request.getUrl().encodedPath()).isEqualTo("/api/v3/write_lp");
        assertThat(request.getUrl().queryParameter("no_sync")).isEqualTo("true");
        assertThat(request.getUrl().queryParameter("precision")).isEqualTo("millisecond");

        assertThat(ae.statusCode()).isEqualTo(HttpResponseStatus.METHOD_NOT_ALLOWED.code());
        assertThat(ae.getMessage()).contains("Server doesn't support write with NoSync=true"
                + " (supported by InfluxDB 3 Core/Enterprise servers only).");
    }

    @Test
    void writeRecordWithDefaultWriteOptionsDefaultConfig() throws Exception {
        mockServer.enqueue(createResponse(200));

        ClientConfig cfg = new ClientConfig.Builder().host(baseURL).token("TOKEN".toCharArray()).database("DB")
                .build();
        try (InfluxDBClient client = InfluxDBClient.getInstance(cfg)) {
            client.writeRecord("mem,tag=one value=1.0");
        }

        checkWriteCalled("/api/v2/write", "DB", "ns", false, false);
    }

    @Test
    void writeRecordWithDefaultWriteOptionsCustomConfig() throws Exception {
        mockServer.enqueue(createResponse(200));

        ClientConfig cfg = new ClientConfig.Builder().host(baseURL).token("TOKEN".toCharArray()).database("DB")
                .writePrecision(WritePrecision.S)
                .writeNoSync(true)
                .gzipThreshold(1)
                .build();
        try (InfluxDBClient client = InfluxDBClient.getInstance(cfg)) {
            client.writeRecord("mem,tag=one value=1.0");
        }

        checkWriteCalled("/api/v3/write_lp", "DB", "second", true, true);
    }

    @Test
    void writeRecordsWithDefaultWriteOptionsDefaultConfig() throws Exception {
        mockServer.enqueue(createResponse(200));

        ClientConfig cfg = new ClientConfig.Builder().host(baseURL).token("TOKEN".toCharArray()).database("DB")
                .build();
        try (InfluxDBClient client = InfluxDBClient.getInstance(cfg)) {
            client.writeRecords(List.of("mem,tag=one value=1.0"));
        }

        checkWriteCalled("/api/v2/write", "DB", "ns", false, false);
    }

    @Test
    void writeRecordsWithDefaultWriteOptionsCustomConfig() throws Exception {
        mockServer.enqueue(createResponse(200));

        ClientConfig cfg = new ClientConfig.Builder().host(baseURL).token("TOKEN".toCharArray()).database("DB")
                .writePrecision(WritePrecision.S)
                .writeNoSync(true)
                .gzipThreshold(1)
                .build();
        try (InfluxDBClient client = InfluxDBClient.getInstance(cfg)) {
            client.writeRecords(List.of("mem,tag=one value=1.0"));
        }

        checkWriteCalled("/api/v3/write_lp", "DB", "second", true, true);
    }

    @Test
    void writePointWithDefaultWriteOptionsDefaultConfig() throws Exception {
        mockServer.enqueue(createResponse(200));

        ClientConfig cfg = new ClientConfig.Builder().host(baseURL).token("TOKEN".toCharArray()).database("DB")
                .build();
        try (InfluxDBClient client = InfluxDBClient.getInstance(cfg)) {
            Point point = new Point("mem");
            point.setTag("tag", "one");
            point.setField("value", 1.0);
            client.writePoint(point);
        }

        checkWriteCalled("/api/v2/write", "DB", "ns", false, false);
    }

    @Test
    void writePointWithDefaultWriteOptionsCustomConfig() throws Exception {
        mockServer.enqueue(createResponse(200));

        ClientConfig cfg = new ClientConfig.Builder().host(baseURL).token("TOKEN".toCharArray()).database("DB")
                .writePrecision(WritePrecision.S)
                .writeNoSync(true)
                .gzipThreshold(1)
                .build();
        try (InfluxDBClient client = InfluxDBClient.getInstance(cfg)) {
            Point point = new Point("mem");
            point.setTag("tag", "one");
            point.setField("value", 1.0);
            client.writePoint(point);
        }

        checkWriteCalled("/api/v3/write_lp", "DB", "second", true, true);
    }

    @Test
    void writePointsWithDefaultWriteOptionsDefaultConfig() throws Exception {
        mockServer.enqueue(createResponse(200));

        ClientConfig cfg = new ClientConfig.Builder().host(baseURL).token("TOKEN".toCharArray()).database("DB")
                .build();
        try (InfluxDBClient client = InfluxDBClient.getInstance(cfg)) {
            Point point = new Point("mem");
            point.setTag("tag", "one");
            point.setField("value", 1.0);
            client.writePoints(List.of(point));
        }

        checkWriteCalled("/api/v2/write", "DB", "ns", false, false);
    }

    @Test
    void writePointsWithDefaultWriteOptionsCustomConfig() throws Exception {
        mockServer.enqueue(createResponse(200));

        ClientConfig cfg = new ClientConfig.Builder().host(baseURL).token("TOKEN".toCharArray()).database("DB")
                .writePrecision(WritePrecision.S)
                .writeNoSync(true)
                .gzipThreshold(1)
                .build();
        try (InfluxDBClient client = InfluxDBClient.getInstance(cfg)) {
            Point point = new Point("mem");
            point.setTag("tag", "one");
            point.setField("value", 1.0);
            client.writePoints(List.of(point));
        }

        checkWriteCalled("/api/v3/write_lp", "DB", "second", true, true);
    }

    private void checkWriteCalled(final String expectedPath, final String expectedDB,
                                  final String expectedPrecision, final boolean expectedNoSync,
                                  final boolean expectedGzip) throws InterruptedException {
        RecordedRequest request = assertThatServerRequested();
        HttpUrl requestUrl = request.getUrl();
        assertThat(requestUrl).isNotNull();
        assertThat(requestUrl.encodedPath()).isEqualTo(expectedPath);
        if (expectedNoSync) {
            assertThat(requestUrl.queryParameter("db")).isEqualTo(expectedDB);
        } else {
            assertThat(requestUrl.queryParameter("bucket")).isEqualTo(expectedDB);
        }
        assertThat(requestUrl.queryParameter("precision")).isEqualTo(expectedPrecision);
        if (expectedNoSync) {
            assertThat(requestUrl.queryParameter("no_sync")).isEqualTo("true");
        } else {
            assertThat(requestUrl.queryParameter("no_sync")).isNull();
        }
        if (expectedGzip) {
            assertThat(request.getHeaders().get("Content-Encoding")).isEqualTo("gzip");
        } else {
            assertThat(request.getHeaders().get("Content-Encoding")).isNull();
        }
    }

    @Nonnull
    private RecordedRequest assertThatServerRequested() throws InterruptedException {
        assertThat(mockServer.getRequestCount()).isEqualTo(1);
        RecordedRequest request = mockServer.takeRequest();
        assertThat(request).isNotNull();
        return request;
    }

    @Test
    void allParameterSpecified() throws InterruptedException {
        mockServer.enqueue(createResponse(200));

        client.writeRecord("mem,tag=one value=1.0",
                new WriteOptions("your-database", WritePrecision.S, 1, false));

        assertThat(mockServer.getRequestCount()).isEqualTo(1);
        RecordedRequest request = mockServer.takeRequest();
        assertThat(request).isNotNull();
        assertThat(request.getUrl()).isNotNull();
        assertThat(request.getHeaders().get("Content-Type")).isEqualTo("text/plain; charset=utf-8");
        assertThat(request.getHeaders().get("Content-Encoding")).isEqualTo("gzip");
        assertThat(request.getUrl().queryParameter("precision")).isEqualTo("s");
        assertThat(request.getUrl().queryParameter("bucket")).isEqualTo("your-database");
    }

    @Test
    void contentHeaders() throws InterruptedException {
        mockServer.enqueue(createResponse(200));

        client.writeRecord("mem,tag=one value=1.0");

        assertThat(mockServer.getRequestCount()).isEqualTo(1);
        RecordedRequest request = mockServer.takeRequest();
        assertThat(request).isNotNull();
        assertThat(request.getHeaders().get("Content-Type")).isEqualTo("text/plain; charset=utf-8");
        assertThat(request.getHeaders().get("Content-Encoding")).isNull();
    }

    @Test
    void bodyRecord() throws InterruptedException {
        mockServer.enqueue(createResponse(200));

        client.writeRecord("mem,tag=one value=1.0");

        assertThat(mockServer.getRequestCount()).isEqualTo(1);
        RecordedRequest request = mockServer.takeRequest();
        assertThat(request).isNotNull();
//        assertThat(request.getBody().readUtf8()).isEqualTo("mem,tag=one value=1.0");
//         assertThat(request.getBody().getUtf8$okio()).isEqualTo("mem,tag=one value=1.0");
        assertThat(request.getBody().utf8()).isEqualTo("mem,tag=one value=1.0");

    }

    @Test
    void bodyPoint() throws InterruptedException {
        mockServer.enqueue(createResponse(200));

        Point point = new Point("mem");
        point.setTag("tag", "one");
        point.setField("value", 1.0);

        client.writePoint(point);

        assertThat(mockServer.getRequestCount()).isEqualTo(1);
        RecordedRequest request = mockServer.takeRequest();
        assertThat(request).isNotNull();
  //      assertThat(request.getBody().readUtf8()).isEqualTo("mem,tag=one value=1.0");
        assertThat(request.getBody().utf8()).isEqualTo("mem,tag=one value=1.0");
    }

    @Test
    void bodyConcat() throws InterruptedException {
        mockServer.enqueue(createResponse(200));

        Point point1 = Point.measurement("mem")
                .setTag("tag", "one")
                .setField("value", 1.0);

        Point point2 = Point.measurement("cpu")
                .setTag("tag", "two")
                .setField("value", 2.0);

        client.writePoints(Arrays.asList(point1, point2));

        assertThat(mockServer.getRequestCount()).isEqualTo(1);
        RecordedRequest request = mockServer.takeRequest();
        assertThat(request).isNotNull();
        // assertThat(request.getBody().readUtf8()).isEqualTo("mem,tag=one value=1.0\ncpu,tag=two value=2.0");
        assertThat(request.getBody().utf8()).isEqualTo("mem,tag=one value=1.0\ncpu,tag=two value=2.0");
    }


    @Test
    void defaultTags() throws InterruptedException {
        mockServer.enqueue(createResponse(200));

        Point point = Point.measurement("mem")
          .setTag("tag", "one")
          .setField("value", 1.0);

        Map<String, String> defaultTags = Map.of("unit", "U2", "model", "M5");

        WriteOptions options = new WriteOptions.Builder().defaultTags(defaultTags).build();

        client.writePoint(point, options);

        assertThat(mockServer.getRequestCount()).isEqualTo(1);
        RecordedRequest request = mockServer.takeRequest();

        assertThat(request).isNotNull();
        // assertThat(request.getBody().readUtf8()).isEqualTo("mem,model=M5,tag=one,unit=U2 value=1.0");
        assertThat(request.getBody().utf8()).isEqualTo("mem,model=M5,tag=one,unit=U2 value=1.0");

    }

    @Test
    public void retryHandled429Test() {
        mockServer.enqueue(createResponse(429,
          "application/json",
          Map.of("retry-after", "42"),
          "{ \"message\" : \"Too Many Requests\" }"));

        Point point = Point.measurement("mem")
          .setTag("tag", "one")
          .setField("value", 1.0);

        Throwable thrown = catchThrowable(() -> client.writePoint(point));

        assertThat(thrown).isNotNull();
        assertThat(thrown).isInstanceOf(InfluxDBApiHttpException.class);
        InfluxDBApiHttpException he = (InfluxDBApiHttpException) thrown;
        assertThat(he.headers()).isNotNull();
        assertThat(he.getHeader("retry-after").get(0))
          .isNotNull().isEqualTo("42");
        assertThat(he.getHeader("content-type").get(0))
          .isNotNull().isEqualTo("application/json");
        assertThat(he.statusCode()).isEqualTo(429);
        assertThat(he.getMessage())
          .isEqualTo("HTTP status code: 429; Message: Too Many Requests");
    }

    @Test
    public void timeoutExceededTest() {

        mockServer.enqueue(createEmptyResponse(200));

        ClientConfig config = new ClientConfig.Builder()
            .host(baseURL)
            .token("my-token".toCharArray())
            .database("my-db")
            .timeout(Duration.ofNanos(5000))
            .build();



        try (InfluxDBClient toClient = InfluxDBClient.getInstance(config)) {

            Point point = Point.measurement("mem")
                .setTag("tag", "one")
                .setField("value", 1.0);

            Throwable thrown = catchThrowable(() -> {
                toClient.writePoint(point);
            });
            assertThat(thrown).isNotNull();
            assertThat(thrown).isInstanceOf(InfluxDBApiException.class);
            assertThat(thrown.getMessage()).contains("java.net.http.HttpConnectTimeoutException");

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void writeTimeoutExceededTest() {

        mockServer.enqueue(createEmptyResponse(200));

        ClientConfig config = new ClientConfig.Builder()
            .host(baseURL)
            .token("my-token".toCharArray())
            .database("my-db")
            .writeTimeout(Duration.ofNanos(5000))
            .build();

        try (InfluxDBClient toClient = InfluxDBClient.getInstance(config)) {

            Point point = Point.measurement("mem")
                .setTag("tag", "one")
                .setField("value", 1.0);

            Throwable thrown = catchThrowable(() -> {
                toClient.writePoint(point);
            });
            assertThat(thrown).isNotNull();
            assertThat(thrown).isInstanceOf(InfluxDBApiException.class);
            assertThat(thrown.getMessage()).contains("java.net.http.HttpConnectTimeoutException");

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void writeTimeoutOKTest() {
        mockServer.enqueue(createResponse(200));

        Duration testDuration = Duration.ofMillis(2000);

        ClientConfig config = new ClientConfig.Builder()
            .host(baseURL)
            .token("my-token".toCharArray())
            .database("my-db")
            .writeTimeout(testDuration)
            .build();

        try (InfluxDBClient toClient = InfluxDBClient.getInstance(config)) {

            Point point = Point.measurement("mem")
                .setTag("tag", "one")
                .setField("value", 1.0);

                Instant start = Instant.now();
                toClient.writePoint(point);
                Instant end = Instant.now();
                RecordedRequest request = mockServer.takeRequest();
                assertThat(request).isNotNull();
                assertThat(request.getBody()).isNotNull();
                String utf8Body = request.getBody().utf8();
                assertThat(utf8Body).isNotNull();
                assertThat(utf8Body).isEqualTo("mem,tag=one value=1.0");
                assertThat(Duration.between(start, end).toMillis()).isLessThan(testDuration.toMillis());

        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }
}
