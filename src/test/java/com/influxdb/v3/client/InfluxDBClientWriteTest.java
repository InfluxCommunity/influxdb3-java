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

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import okhttp3.mockwebserver.RecordedRequest;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.influxdb.v3.client.write.WriteOptions;
import com.influxdb.v3.client.write.WritePrecision;

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

        Assertions.assertThat(mockServer.getRequestCount()).isEqualTo(0);
    }

    @Test
    void writeNullRecord() {
        mockServer.enqueue(createResponse(200));

        client.writeRecord(null);

        Assertions.assertThat(mockServer.getRequestCount()).isEqualTo(0);
    }

    @Test
    void writeNullPoint() {
        mockServer.enqueue(createResponse(200));

        client.writePoint(null);

        Assertions.assertThat(mockServer.getRequestCount()).isEqualTo(0);
    }

    @Test
    void databaseParameter() throws InterruptedException {
        mockServer.enqueue(createResponse(200));

        client.writeRecord("mem,tag=one value=1.0");

        Assertions.assertThat(mockServer.getRequestCount()).isEqualTo(1);
        RecordedRequest request = mockServer.takeRequest();
        Assertions.assertThat(request).isNotNull();
        Assertions.assertThat(request.getRequestUrl()).isNotNull();
        Assertions.assertThat(request.getRequestUrl().queryParameter("bucket")).isEqualTo("my-database");
    }

    @Test
    void databaseParameterSpecified() throws InterruptedException {
        mockServer.enqueue(createResponse(200));

        client.writeRecord("mem,tag=one value=1.0", new WriteOptions.Builder().database("my-database-2").build());

        Assertions.assertThat(mockServer.getRequestCount()).isEqualTo(1);
        RecordedRequest request = mockServer.takeRequest();
        Assertions.assertThat(request).isNotNull();
        Assertions.assertThat(request.getRequestUrl()).isNotNull();
        Assertions.assertThat(request.getRequestUrl().queryParameter("bucket")).isEqualTo("my-database-2");
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

        Assertions.assertThat(mockServer.getRequestCount()).isEqualTo(0);
    }


    @Test
    void precisionParameter() throws InterruptedException {
        mockServer.enqueue(createResponse(200));

        client.writeRecord("mem,tag=one value=1.0");

        Assertions.assertThat(mockServer.getRequestCount()).isEqualTo(1);
        RecordedRequest request = mockServer.takeRequest();
        Assertions.assertThat(request).isNotNull();
        Assertions.assertThat(request.getRequestUrl()).isNotNull();
        Assertions.assertThat(request.getRequestUrl().queryParameter("precision")).isEqualTo("ns");
    }

    @Test
    void precisionParameterSpecified() throws InterruptedException {
        mockServer.enqueue(createResponse(200));

        client.writeRecord("mem,tag=one value=1.0", new WriteOptions.Builder().precision(WritePrecision.S).build());

        Assertions.assertThat(mockServer.getRequestCount()).isEqualTo(1);
        RecordedRequest request = mockServer.takeRequest();
        Assertions.assertThat(request).isNotNull();
        Assertions.assertThat(request.getRequestUrl()).isNotNull();
        Assertions.assertThat(request.getRequestUrl().queryParameter("precision")).isEqualTo("s");
    }

    @Test
    void gzipParameter() throws InterruptedException {
        mockServer.enqueue(createResponse(200));

        client.writeRecord("mem,tag=one value=1.0");

        Assertions.assertThat(mockServer.getRequestCount()).isEqualTo(1);
        RecordedRequest request = mockServer.takeRequest();
        Assertions.assertThat(request).isNotNull();
        Assertions.assertThat(request.getRequestUrl()).isNotNull();
        Assertions.assertThat(request.getHeader("Content-Type")).isEqualTo("text/plain; charset=utf-8");
        Assertions.assertThat(request.getHeader("Content-Encoding")).isNotEqualTo("gzip");
    }

    @Test
    void gzipParameterSpecified() throws InterruptedException {
        mockServer.enqueue(createResponse(200));

        client.writeRecord("mem,tag=one value=1.0", new WriteOptions.Builder().gzipThreshold(1).build());

        Assertions.assertThat(mockServer.getRequestCount()).isEqualTo(1);
        RecordedRequest request = mockServer.takeRequest();
        Assertions.assertThat(request).isNotNull();
        Assertions.assertThat(request.getRequestUrl()).isNotNull();
        Assertions.assertThat(request.getHeader("Content-Type")).isEqualTo("text/plain; charset=utf-8");
        Assertions.assertThat(request.getHeader("Content-Encoding")).isEqualTo("gzip");
    }

    @Test
    void allParameterSpecified() throws InterruptedException {
        mockServer.enqueue(createResponse(200));

        client.writeRecord("mem,tag=one value=1.0",
                new WriteOptions("your-database", WritePrecision.S, 1));

        Assertions.assertThat(mockServer.getRequestCount()).isEqualTo(1);
        RecordedRequest request = mockServer.takeRequest();
        Assertions.assertThat(request).isNotNull();
        Assertions.assertThat(request.getRequestUrl()).isNotNull();
        Assertions.assertThat(request.getHeader("Content-Type")).isEqualTo("text/plain; charset=utf-8");
        Assertions.assertThat(request.getHeader("Content-Encoding")).isEqualTo("gzip");
        Assertions.assertThat(request.getRequestUrl().queryParameter("precision")).isEqualTo("s");
        Assertions.assertThat(request.getRequestUrl().queryParameter("bucket")).isEqualTo("your-database");
    }

    @Test
    void contentHeaders() throws InterruptedException {
        mockServer.enqueue(createResponse(200));

        client.writeRecord("mem,tag=one value=1.0");

        Assertions.assertThat(mockServer.getRequestCount()).isEqualTo(1);
        RecordedRequest request = mockServer.takeRequest();
        Assertions.assertThat(request).isNotNull();
        Assertions.assertThat(request.getHeader("Content-Type")).isEqualTo("text/plain; charset=utf-8");
        Assertions.assertThat(request.getHeader("Content-Encoding")).isNull();
    }

    @Test
    void bodyRecord() throws InterruptedException {
        mockServer.enqueue(createResponse(200));

        client.writeRecord("mem,tag=one value=1.0");

        Assertions.assertThat(mockServer.getRequestCount()).isEqualTo(1);
        RecordedRequest request = mockServer.takeRequest();
        Assertions.assertThat(request).isNotNull();
        Assertions.assertThat(request.getBody().readUtf8()).isEqualTo("mem,tag=one value=1.0");
    }

    @Test
    void bodyPoint() throws InterruptedException {
        mockServer.enqueue(createResponse(200));

        Point point = new Point("mem");
        point.setTag("tag", "one");
        point.setField("value", 1.0);

        client.writePoint(point);

        Assertions.assertThat(mockServer.getRequestCount()).isEqualTo(1);
        RecordedRequest request = mockServer.takeRequest();
        Assertions.assertThat(request).isNotNull();
        Assertions.assertThat(request.getBody().readUtf8()).isEqualTo("mem,tag=one value=1.0");
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

        Assertions.assertThat(mockServer.getRequestCount()).isEqualTo(1);
        RecordedRequest request = mockServer.takeRequest();
        Assertions.assertThat(request).isNotNull();
        Assertions.assertThat(request.getBody().readUtf8()).isEqualTo("mem,tag=one value=1.0\ncpu,tag=two value=2.0");
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

        Assertions.assertThat(mockServer.getRequestCount()).isEqualTo(1);
        RecordedRequest request = mockServer.takeRequest();

        Assertions.assertThat(request).isNotNull();
        Assertions.assertThat(request.getBody().readUtf8()).isEqualTo("mem,model=M5,tag=one,unit=U2 value=1.0");

    }

    @Test
    public void retryHandled429Test() throws InterruptedException {
        mockServer.enqueue(createResponse(429)
          .setBody("{ \"message\" : \"Too Many Requests\" }")
          .setHeader("retry-after", "42")
          .setHeader("content-type", "application/json")
        );

        Point point = Point.measurement("mem")
          .setTag("tag", "one")
          .setField("value", 1.0);

        Throwable thrown = catchThrowable(() -> client.writePoint(point));

        Assertions.assertThat(thrown).isNotNull();
        Assertions.assertThat(thrown).isInstanceOf(InfluxDBApiHttpException.class);
        InfluxDBApiHttpException he = (InfluxDBApiHttpException) thrown;
        Assertions.assertThat(he.headers()).isNotNull();
        Assertions.assertThat(he.getHeader("retry-after").get(0))
          .isNotNull().isEqualTo("42");
        Assertions.assertThat(he.getHeader("content-type").get(0))
          .isNotNull().isEqualTo("application/json");
        Assertions.assertThat(he.statusCode()).isEqualTo(429);
        Assertions.assertThat(he.getMessage())
          .isEqualTo("HTTP status code: 429; Message: Too Many Requests");
    }
}
