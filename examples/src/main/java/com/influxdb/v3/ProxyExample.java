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
package com.influxdb.v3;

import java.net.InetSocketAddress;
import java.net.ProxySelector;
import java.net.URI;
import java.util.stream.Stream;

import io.grpc.HttpConnectProxiedSocketAddress;
import io.grpc.ProxyDetector;

import com.influxdb.v3.client.InfluxDBClient;
import com.influxdb.v3.client.Point;
import com.influxdb.v3.client.PointValues;
import com.influxdb.v3.client.config.ClientConfig;

public final class ProxyExample {

    private ProxyExample() { }

    public static void main(final String[] args) throws Exception {
        // Run docker-compose.yml file to start Envoy proxy

        String proxyUrl = "http://127.0.0.1:10000";
        String certificateFilePath = "src/test/java/com/influxdb/v3/client/testdata/valid-certificates.pem";
        ClientConfig clientConfig = new ClientConfig.Builder()
                .host(System.getenv("TESTING_INFLUXDB_URL"))
                .token(System.getenv("TESTING_INFLUXDB_TOKEN").toCharArray())
                .database(System.getenv("TESTING_INFLUXDB_DATABASE"))
                .proxyUrl(proxyUrl)
                .certificateFilePath(certificateFilePath)
                .build();

        InfluxDBClient influxDBClient = InfluxDBClient.getInstance(clientConfig);
        influxDBClient.writePoint(
                Point.measurement("test1")
                        .setField("field", "field1")
        );

        try (Stream<PointValues> stream = influxDBClient.queryPoints("SELECT * FROM test1")) {
            stream.findFirst()
                    .ifPresent(pointValues -> {
                        Assertions.assertThat(pointValues.getField("field")).isEqualTo("field1");
                    });
        }
    }
}

