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
                .hasMessage("The URL of the InfluxDB server has to be defined.");
    }

    @Test
    void requiredHostConnectionString() {

        Assertions.assertThatThrownBy(() -> InfluxDBClient.getInstance("?token=my-token&database=my-database"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("no protocol");
    }

    @Test
    void requiredHostEnvOrProperties() {

        Assertions.assertThatThrownBy(InfluxDBClient::getInstance)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("The URL of the InfluxDB server has to be defined.");
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

    @EnabledIfEnvironmentVariable(named = "TESTING_INFLUXDB_URL", matches = ".*")
    @EnabledIfEnvironmentVariable(named = "TESTING_INFLUXDB_TOKEN", matches = ".*")
    @EnabledIfEnvironmentVariable(named = "TESTING_INFLUXDB_DATABASE", matches = ".*")
    @Test
    public void testQuery() throws Exception {
        try (InfluxDBClient client = InfluxDBClient.getInstance(
                System.getenv("TESTING_INFLUXDB_URL"),
                System.getenv("TESTING_INFLUXDB_TOKEN").toCharArray(),
                System.getenv("TESTING_INFLUXDB_DATABASE"),
                null)) {
            String uuid = UUID.randomUUID().toString();
            long timestamp = Instant.now().getEpochSecond();
            String record = String.format(
                    "host12,tag=empty "
                            + "name=\"intel\","
                            + "mem_total=2048,"
                            + "disk_free=100i,"
                            + "temperature=100.86,"
                            + "isActive=true,"
                            + "testId=\"%s\" %d",
                    uuid,
                    timestamp
            );
            client.writeRecord(record, new WriteOptions(null, WritePrecision.S, null));

            Map<String, Object> parameters = Map.of("testId", uuid);
            String sql = "Select * from host12 where \"testId\"=$testId";
            try (Stream<Object[]> stream = client.query(sql, parameters)) {
                stream.findFirst()
                      .ifPresent(objects -> {
                          Assertions.assertThat(objects[0].getClass()).isEqualTo(Long.class);
                          Assertions.assertThat(objects[0]).isEqualTo(100L);

                          Assertions.assertThat(objects[1].getClass()).isEqualTo(Boolean.class);
                          Assertions.assertThat(objects[1]).isEqualTo(true);

                          Assertions.assertThat(objects[2].getClass()).isEqualTo(Double.class);
                          Assertions.assertThat(objects[2]).isEqualTo(2048.0);

                          Assertions.assertThat(objects[3].getClass()).isEqualTo(String.class);
                          Assertions.assertThat(objects[3]).isEqualTo("intel");

                          Assertions.assertThat(objects[4].getClass()).isEqualTo(String.class);
                          Assertions.assertThat(objects[4]).isEqualTo("empty");

                          Assertions.assertThat(objects[7].getClass()).isEqualTo(BigInteger.class);
                          Assertions.assertThat(objects[7]).isEqualTo(BigInteger.valueOf(timestamp * 1_000_000_000));
                      });
            }
        }
    }

    @EnabledIfEnvironmentVariable(named = "TESTING_INFLUXDB_URL", matches = ".*")
    @EnabledIfEnvironmentVariable(named = "TESTING_INFLUXDB_TOKEN", matches = ".*")
    @EnabledIfEnvironmentVariable(named = "TESTING_INFLUXDB_DATABASE", matches = ".*")
    @Test
    public void testQueryRows() throws Exception {
        try (InfluxDBClient client = InfluxDBClient.getInstance(
                System.getenv("TESTING_INFLUXDB_URL"),
                System.getenv("TESTING_INFLUXDB_TOKEN").toCharArray(),
                System.getenv("TESTING_INFLUXDB_DATABASE"),
                null)) {
            String uuid = UUID.randomUUID().toString();
            long timestamp = Instant.now().getEpochSecond();
            String record = String.format(
                    "host12,tag=tagValue "
                            + "name=\"intel\","
                            + "mem_total=2048,"
                            + "disk_free=100i,"
                            + "temperature=100.86,"
                            + "isActive=true,"
                            + "testId=\"%s\" %d",
                    uuid,
                    timestamp
            );
            client.writeRecord(record, new WriteOptions(null, WritePrecision.S, null));

            Map<String, Object> parameters = Map.of("testId", uuid);
            String sql = "Select * from host12 where \"testId\"=$testId";
            try (Stream<Map<String, Object>> stream = client.queryRows(sql, parameters)) {
                stream.findFirst()
                      .ifPresent(map -> {
                          Assertions.assertThat(map.get("tag").getClass()).isEqualTo(String.class);
                          Assertions.assertThat(map.get("tag")).isEqualTo("tagValue");

                          Assertions.assertThat(map.get("name").getClass()).isEqualTo(String.class);
                          Assertions.assertThat(map.get("name")).isEqualTo("intel");

                          Assertions.assertThat(map.get("mem_total").getClass()).isEqualTo(Double.class);
                          Assertions.assertThat(map.get("mem_total")).isEqualTo(2048.0);

                          Assertions.assertThat(map.get("disk_free").getClass()).isEqualTo(Long.class);
                          Assertions.assertThat(map.get("disk_free")).isEqualTo(100L);

                          Assertions.assertThat(map.get("isActive").getClass()).isEqualTo(Boolean.class);
                          Assertions.assertThat(map.get("isActive")).isEqualTo(true);

                          Assertions.assertThat(map.get("time").getClass()).isEqualTo(BigInteger.class);
                          Assertions.assertThat(map.get("time")).isEqualTo(BigInteger.valueOf(timestamp * 1_000_000_000));
                      });
            }
        }
    }
}
