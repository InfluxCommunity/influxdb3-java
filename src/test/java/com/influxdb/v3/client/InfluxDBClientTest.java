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

import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import com.influxdb.v3.client.config.ClientConfig;

public class InfluxDBClientTest {

    @Test
    void withProxyUrl() {
        String proxyUrl = "http://localhost:10000";
        ClientConfig.Builder builder = new ClientConfig.Builder();
        builder.proxyUrl(proxyUrl);
        ClientConfig clientConfig = builder.build();
        Assertions.assertThat(clientConfig.getProxyUrl()).isEqualTo(proxyUrl);
    }

    @Test
    void parseIpv6() throws UnknownHostException, URISyntaxException {
        record Test(String url, boolean isCorrect) {
        }
        var tests = List.of(
                new Test("http://[2001:db8::1]/", true),
                new Test("http://[2001:db8:a0b:12f0::1]/index.html", true),
                new Test("http://[2001:db8:a0b:12f0::1]:80/index.html", true),
                new Test("https://[2001:db8:a0b:12f0::1%25eth0]:15000/", true),
                new Test("http://[2607:f8b0:4005:802::1007]/", true),
                new Test("http://2001:db8::1/", false),
                new Test("http://2001:db8::1:8080/", false)
        );
        for (Test test : tests) {
            if (!test.isCorrect()) {
                Assertions.assertThatThrownBy(() -> {
                    try (var client = InfluxDBClient.getInstance(test.url(), "my-token".toCharArray(), "bucket0")) {
                        client.getServerVersion();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }).hasMessageContaining("Invalid URL.");
            } else {
                Assertions.assertThatNoException().isThrownBy(() -> {
                    try (var ignored = InfluxDBClient.getInstance(test.url(), "my-token".toCharArray(), "bucket0")) {
                        Assertions.assertThat(true);
                    }
                });
            }
        }
    }

    @Test
    void withSslRootsFilePath() {
        String path = "/path/to/cert";
        ClientConfig.Builder builder = new ClientConfig.Builder();
        builder.sslRootsFilePath(path);
        ClientConfig clientConfig = builder.build();
        Assertions.assertThat(clientConfig.sslRootsFilePath()).isEqualTo(path);
    }

    @Test
    void requiredHost() {
        Assertions.assertThatThrownBy(() -> InfluxDBClient.getInstance(null, "my-token".toCharArray(), "my-database"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Invalid URL.");

        Assertions.assertThatThrownBy(() -> InfluxDBClient.getInstance(" ", "my-token".toCharArray(), "my-database"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Invalid URL.");
    }

    @Test
    void requiredHostConnectionString() {

        Assertions.assertThatThrownBy(() -> InfluxDBClient.getInstance("?token=my-token&database=my-database"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("no protocol");
    }

    @Test
    void fromParameters() throws Exception {

        try (InfluxDBClient client = InfluxDBClient.getInstance("http://localhost:8086",
                "my-token".toCharArray(), "my-database")) {
            Assertions.assertThat(client).isNotNull();
        }
    }

    @Test
    void fromConnectionString() throws Exception {

        try (InfluxDBClient client = InfluxDBClient.getInstance("http://localhost:8086"
                + "?token=my-token&database=my-db")) {
            Assertions.assertThat(client).isNotNull();
        }
    }

    @Test
    void fromEnvOrProperties() throws Exception {

        final Properties old = System.getProperties();
        final Properties p = new Properties();
        p.put("influx.host", "http://localhost:8086");
        p.put("influx.token", "my-token");
        p.put("influx.database", "my-db");
        System.setProperties(p);

        try (InfluxDBClient client = InfluxDBClient.getInstance()) {
            Assertions.assertThat(client).isNotNull();
        } finally {
            System.setProperties(old);
        }
    }

    @Test
    void withDefaultTags() throws Exception {

        Map<String, String> defaultTags = Map.of("unit", "U2", "model", "M1");

        try (InfluxDBClient client = InfluxDBClient.getInstance(
          "http://localhost:8086",
          "MY-TOKEN".toCharArray(),
          "MY-DATABASE",
          defaultTags)) {
            Assertions.assertThat(client).isNotNull();
        }
    }

    @Test
    public void unsupportedQueryParams() throws Exception {
        try (InfluxDBClient client = InfluxDBClient.getInstance("http://localhost:8086",
                "my-token".toCharArray(), "my-database")) {

            String query = "select * from cpu where client=$client";
            Map<String, Object> parameters = Map.of("client", client);

            Assertions.assertThatThrownBy(() -> client.queryPoints(query, parameters))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("The parameter client value has unsupported type: "
                            + "class com.influxdb.v3.client.internal.InfluxDBClientImpl");
        }
    }
}
