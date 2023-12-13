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

import java.time.Instant;
import java.util.stream.Stream;

import com.influxdb.v3.client.InfluxDBClient;
import com.influxdb.v3.client.Point;
import com.influxdb.v3.client.PointValues;
import com.influxdb.v3.client.query.QueryOptions;

/**
 * The example depends on the "influxdb3-java" module and this module should be built first
 * by running "mvn install" in the root directory.
 */
public final class IOxExample {
    private IOxExample() {
    }

    public static void main(final String[] args) throws Exception {
        String host = "https://us-east-1-1.aws.cloud2.influxdata.com";
        String token = "my-token";
        String database = "my-database";

        try (InfluxDBClient client = InfluxDBClient.getInstance(host, token.toCharArray(), database)) {

            //
            // Write by Point
            //
            Point point = Point.measurement("temperature")
                    .setTag("location", "west")
                    .setField("value", 55.15)
                    .setTimestamp(Instant.now().minusSeconds(-10));
            client.writePoint(point);

            //
            // Write by LineProtocol
            //
            String record = "temperature,location=north value=60.0";
            client.writeRecord(record);

            //
            // Query by SQL
            //
            System.out.printf("--------------------------------------------------------%n");
            System.out.printf("| %-8s | %-8s | %-30s |%n", "location", "value", "time");
            System.out.printf("--------------------------------------------------------%n");

            String sql = "select time,location,value from temperature order by time desc limit 10";
            try (Stream<Object[]> stream = client.query(sql)) {
                stream.forEach(row -> System.out.printf("| %-8s | %-8s | %-30s |%n", row[1], row[2], row[0]));
            }

            System.out.printf("--------------------------------------------------------%n%n");

            //
            // Query by InfluxQL
            //
            System.out.printf("-----------------------------------------%n");
            System.out.printf("| %-16s | %-18s |%n", "time", "mean");
            System.out.printf("-----------------------------------------%n");

            String influxQL =
                    "select MEAN(value) from temperature group by time(1d) fill(none) order by time desc limit 10";
            try (Stream<Object[]> stream = client.query(influxQL, QueryOptions.INFLUX_QL)) {
                stream.forEach(row -> System.out.printf("| %-16s | %-18s |%n", row[1], row[2]));
            }

            System.out.printf("-----------------------------------------%n%n");


            System.out.printf("--------------------------------------------------------%n");
            System.out.printf("| %-8s | %-8s | %-30s |%n", "location", "value", "time");
            System.out.printf("--------------------------------------------------------%n");

            //
            // Query by SQL into Points
            //
            try (Stream<PointValues> stream = client.queryPoints(sql)) {
                stream.forEach(
                    (PointValues p) -> {
                        var time = p.getTimestamp();
                        var location = p.getTag("location");
                        var value = p.getField("value", Double.class);

                        System.out.printf("| %-8s | %-8s | %-30s |%n", location, value, time);
                });
            }

            System.out.printf("--------------------------------------------------------%n%n");
        }
    }
}
