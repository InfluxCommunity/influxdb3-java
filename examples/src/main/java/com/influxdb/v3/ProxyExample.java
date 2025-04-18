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

import java.util.UUID;
import java.util.stream.Stream;

import com.influxdb.v3.client.InfluxDBClient;
import com.influxdb.v3.client.Point;
import com.influxdb.v3.client.PointValues;
import com.influxdb.v3.client.config.ClientConfig;

public final class ProxyExample {

    private ProxyExample() {
    }

    public static void main(final String[] args) throws Exception {
        // Run docker-compose.yml file to start Envoy proxy

        String proxyUrl = "http://localhost:10000";
        String sslRootsFilePath = "src/test/java/com/influxdb/v3/client/testdata/influxdb-certificate.pem";
        ClientConfig clientConfig = new ClientConfig.Builder()
                .host(System.getenv("INFLUXDB_URL"))
                .token(System.getenv("INFLUXDB_TOKEN").toCharArray())
                .database(System.getenv("INFLUXDB_DATABASE"))
                .proxyUrl(proxyUrl)
                .sslRootsFilePath(sslRootsFilePath)
                .build();

        InfluxDBClient influxDBClient = InfluxDBClient.getInstance(clientConfig);
        String testId = UUID.randomUUID().toString();
        Point point = Point.measurement("My_Home")
                .setTag("room", "Kitchen")
                .setField("temp", 12.7)
                .setField("hum", 37)
                .setField("testId", testId);
        influxDBClient.writePoint(point);

        String query = String.format("SELECT * FROM \"My_Home\" WHERE \"testId\" = '%s'", testId);
        try (Stream<PointValues> stream = influxDBClient.queryPoints(query)) {
            stream.findFirst().ifPresent(values -> {
                assert values.getTimestamp() != null;
                System.out.printf("room[%s]: %s, temp: %3.2f, hum: %d",
                        new java.util.Date(values.getTimestamp().longValue() / 1000000),
                        values.getTag("room"),
                        (Double) values.getField("temp"),
                        (Long) values.getField("hum"));
            });
        }
    }
}

