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

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import com.influxdb.v3.client.InfluxDBClient;
import com.influxdb.v3.client.PointValues;
import com.influxdb.v3.client.config.ClientConfig;

/**
 * This example shows how to set universal timeouts for writes and queries.
 * <p>
 * The example depends on the "influxdb3-java" module and this module should be built first
 * by running "mvn install" in the root directory.
 */
public final class TimeoutsExample {

    public static String resolveProperty(final String property, final String fallback) {
        return System.getProperty(property, System.getenv(property)) == null
            ? fallback : System.getProperty(property, System.getenv(property));
    }

    private TimeoutsExample() { }

    public static void main(final String[] args) {
        // timeout to use for writes.  Experiment with lower values to see timeout exceptions.
        Duration writeTimeout = Duration.ofMillis(5000L);
        // timeout to use for queries.  Experiment with lower values to see timeout exceptions.
        Duration queryTimeout = Duration.ofMillis(5000L);

        String host = resolveProperty("INFLUX_HOST", "http://localhost:8181");
        String token = resolveProperty("INFLUX_TOKEN", "my-token");
        String database = resolveProperty("INFLUX_DATABASE", "my-database");

        String measurement = "timeout_example";

        ClientConfig config = new ClientConfig.Builder()
            .host(host)
            .token(token.toCharArray())
            .database(database)
            .writeTimeout(writeTimeout) // set timeout to be used with the Write API
            .queryTimeout(queryTimeout) // set timeout to be used with the Query API
            .build();

        try (InfluxDBClient client = InfluxDBClient.getInstance(config)) {
            client.writeRecord(String.format("%s,id=0001 temp=30.14,ticks=42i", measurement));

            TimeUnit.SECONDS.sleep(1);
            String sql = String.format("SELECT * FROM %s ORDER BY time DESC", measurement);
            try (Stream<PointValues> values = client.queryPoints(sql)) {
                values.forEach(pv -> {
                    String sv = measurement + ","
                        + " id: " + pv.getTag("id") + ","
                        + " fVal: " + pv.getFloatField("temp") + ","
                        + " iVal: " + pv.getIntegerField("ticks") + ","
                        + " " + pv.getTimestamp();
                    System.out.println(sv);
                });
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
