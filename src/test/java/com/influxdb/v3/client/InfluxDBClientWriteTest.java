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

import okhttp3.mockwebserver.RecordedRequest;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.influxdb.v3.client.write.Point;
import com.influxdb.v3.client.write.WriteParameters;
import com.influxdb.v3.client.write.WritePrecision;

class InfluxDBClientWriteTest extends AbstractMockServerTest {

    private InfluxDBClient client;

    @BeforeEach
    void initClient() {
        client = InfluxDBClient.getInstance(baseURL, "my-token", "my-database");
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
        mockServer.enqueue(createResponse());

        client.writeRecords(Collections.singletonList(null));

        Assertions.assertThat(mockServer.getRequestCount()).isEqualTo(0);
    }

    @Test
    void databaseParameter() throws InterruptedException {
        mockServer.enqueue(createResponse());

        client.writeRecord("mem,tag=one value=1.0");

        Assertions.assertThat(mockServer.getRequestCount()).isEqualTo(1);
        RecordedRequest request = mockServer.takeRequest();
        Assertions.assertThat(request).isNotNull();
        Assertions.assertThat(request.getRequestUrl()).isNotNull();
        Assertions.assertThat(request.getRequestUrl().queryParameter("bucket")).isEqualTo("my-database");
    }

    @Test
    void databaseParameterSpecified() throws InterruptedException {
        mockServer.enqueue(createResponse());

        client.writeRecord("mem,tag=one value=1.0", new WriteParameters("my-database-2", null, null));

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
        mockServer.enqueue(createResponse());

        Assertions.assertThatThrownBy(() -> client.writeRecord("mem,tag=one value=1.0"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Please specify the 'Database' as a method parameter or use "
                        + "default configuration at 'InfluxDBClientConfigs.database'.");

        Assertions.assertThat(mockServer.getRequestCount()).isEqualTo(0);
    }


    @Test
    void precisionParameter() throws InterruptedException {
        mockServer.enqueue(createResponse());

        client.writeRecord("mem,tag=one value=1.0");

        Assertions.assertThat(mockServer.getRequestCount()).isEqualTo(1);
        RecordedRequest request = mockServer.takeRequest();
        Assertions.assertThat(request).isNotNull();
        Assertions.assertThat(request.getRequestUrl()).isNotNull();
        Assertions.assertThat(request.getRequestUrl().queryParameter("precision")).isEqualTo("ns");
    }

    @Test
    void precisionParameterSpecified() throws InterruptedException {
        mockServer.enqueue(createResponse());

        client.writeRecord("mem,tag=one value=1.0", new WriteParameters(null, null, WritePrecision.S));

        Assertions.assertThat(mockServer.getRequestCount()).isEqualTo(1);
        RecordedRequest request = mockServer.takeRequest();
        Assertions.assertThat(request).isNotNull();
        Assertions.assertThat(request.getRequestUrl()).isNotNull();
        Assertions.assertThat(request.getRequestUrl().queryParameter("precision")).isEqualTo("s");
    }

    @Test
    void orgParameterSpecified() throws InterruptedException {
        mockServer.enqueue(createResponse());

        client.writePoint(
                Point
                        .measurement("h2o")
                        .addTag("location", "europe")
                        .addField("level", 2),
                new WriteParameters(null, "my-org", null)
        );

        Assertions.assertThat(mockServer.getRequestCount()).isEqualTo(1);
        RecordedRequest request = mockServer.takeRequest();
        Assertions.assertThat(request).isNotNull();
        Assertions.assertThat(request.getRequestUrl()).isNotNull();
        Assertions.assertThat(request.getRequestUrl().queryParameter("org")).isEqualTo("my-org");
    }

    @Test
    void orgParameterNotSpecified() throws InterruptedException {
        mockServer.enqueue(createResponse());

        client.writePoint(Point
                .measurement("h2o")
                .addTag("location", "europe")
                .addField("level", 2)
        );

        Assertions.assertThat(mockServer.getRequestCount()).isEqualTo(1);
        RecordedRequest request = mockServer.takeRequest();
        Assertions.assertThat(request).isNotNull();
        Assertions.assertThat(request.getRequestUrl()).isNotNull();
        Assertions.assertThat(request.getRequestUrl().queryParameter("org")).isNull();
    }

    @Test
    void contentType() throws InterruptedException {
        mockServer.enqueue(createResponse());

        client.writeRecord("mem,tag=one value=1.0");

        Assertions.assertThat(mockServer.getRequestCount()).isEqualTo(1);
        RecordedRequest request = mockServer.takeRequest();
        Assertions.assertThat(request).isNotNull();
        Assertions.assertThat(request.getHeader("Content-Type")).isEqualTo("text/plain; charset=utf-8");
    }

    @Test
    void body() throws InterruptedException {
        mockServer.enqueue(createResponse());

        client.writeRecord("mem,tag=one value=1.0");

        Assertions.assertThat(mockServer.getRequestCount()).isEqualTo(1);
        RecordedRequest request = mockServer.takeRequest();
        Assertions.assertThat(request).isNotNull();
        Assertions.assertThat(request.getBody().readUtf8()).isEqualTo("mem,tag=one value=1.0");
    }

    @Test
    void bodyConcat() throws InterruptedException {
        mockServer.enqueue(createResponse());

        Point point1 = Point.measurement("mem")
                .addTag("tag", "one")
                .addField("value", 1.0);

        Point point2 = Point.measurement("cpu")
                .addTag("tag", "two")
                .addField("value", 2.0);

        client.writePoints(Arrays.asList(point1, point2));

        Assertions.assertThat(mockServer.getRequestCount()).isEqualTo(1);
        RecordedRequest request = mockServer.takeRequest();
        Assertions.assertThat(request).isNotNull();
        Assertions.assertThat(request.getBody().readUtf8()).isEqualTo("mem,tag=one value=1.0\ncpu,tag=two value=2.0");
    }
}
