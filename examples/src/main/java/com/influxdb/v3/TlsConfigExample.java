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

import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import com.influxdb.v3.client.InfluxDBClient;
import com.influxdb.v3.client.PointValues;
import com.influxdb.v3.client.config.ClientConfig;

public final class TlsConfigExample {

    public static String resolveProperty(final String property, final String fallback) {
        return System.getProperty(property, System.getenv(property)) == null
                ? fallback : System.getProperty(property, System.getenv(property));
    }

    private TlsConfigExample() { }

    public static void main(final String[] args) {
        String host = resolveProperty("INFLUX_HOST", "https://localhost:8181");
        String token = resolveProperty("INFLUX_TOKEN", "my-token");
        String database = resolveProperty("INFLUX_DATABASE", "my-database");

        String measurement = "tls_example";

        // ClientConfig config = configTls(host, token, database);

        // ClientConfig config = configMutualTls(host, token, database);

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

    public ClientConfig configTls(
            @Nonnull final String host,
            @Nonnull final String token,
            @Nonnull final String database) {
        ClientConfig.Builder configBuilder = new ClientConfig.Builder()
                .host(host)
                .token(token.toCharArray())
                .database(database)
                .sslRootsFilePath("path/to/server.crt");

        // Optional, set this if client certificate is password protected.
        configBuilder.sslRootFilePassword("password".toCharArray());
        return configBuilder.build();
    }

    public ClientConfig configMutualTls(
            @Nonnull final String host,
            @Nonnull final String token,
            @Nonnull final String database) {
        ClientConfig.Builder configBuilder = new ClientConfig.Builder()
                .host(host)
                .token(token.toCharArray())
                .database(database)
                .sslRootsFilePath("path/to/server.crt");

        // Optional, set this if server certificate is password-protected.
        configBuilder.sslRootFilePassword("password".toCharArray());

        // Config client certificate by either using client certificate .p12 file format
        // or separated .crt and .key files format, but NOT both.

        // Using separated .crt and .key files format.
        configBuilder.certificateFilePath("path/to/client.crt", "path/to/client.key");

        // Using .p12 file format.
        // configBuilder.certificateP12FilePath("path/to/client.p12", "password".toCharArray());


        return configBuilder.build();
    }
}
