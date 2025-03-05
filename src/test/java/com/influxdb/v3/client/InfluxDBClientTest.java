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

import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.net.ProxySelector;
import java.net.URI;
import java.time.Instant;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Stream;

import io.grpc.HttpConnectProxiedSocketAddress;
import io.grpc.ProxyDetector;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import com.influxdb.v3.client.config.ClientConfig;
import com.influxdb.v3.client.write.WriteOptions;
import com.influxdb.v3.client.write.WritePrecision;

public class InfluxDBClientTest {

    @EnabledIfEnvironmentVariable(named = "TESTING_INFLUXDB_URL", matches = ".*")
    @EnabledIfEnvironmentVariable(named = "TESTING_INFLUXDB_TOKEN", matches = ".*")
    @EnabledIfEnvironmentVariable(named = "TESTING_INFLUXDB_DATABASE", matches = ".*")
    @Test
    void testQueryProxy() throws Exception {
        URI queryProxyUri = new URI("http://127.0.0.1:10000");
        URI uri = new URI(System.getenv("TESTING_INFLUXDB_URL"));

        ProxyDetector proxyDetector = (targetServerAddress) -> {
            InetSocketAddress targetAddress = (InetSocketAddress) targetServerAddress;
            if (uri.getHost().equals(targetAddress.getHostString())) {
                return HttpConnectProxiedSocketAddress.newBuilder()
                        .setProxyAddress(new InetSocketAddress(queryProxyUri.getHost(), queryProxyUri.getPort()))
                        .setTargetAddress(targetAddress)
                        .build();
            }
            return null;
        };
        ProxySelector proxy = ProxySelector.of(new InetSocketAddress(queryProxyUri.getHost(), queryProxyUri.getPort()));
        ClientConfig clientConfig = new ClientConfig.Builder()
                .host(uri.toString())
                .token(System.getenv("TESTING_INFLUXDB_TOKEN").toCharArray())
                .database(System.getenv("TESTING_INFLUXDB_DATABASE"))
                .proxy(proxy)
                .queryApiProxy(proxyDetector)
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
                    "host10,tag=empty "
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
            String sql = "Select * from host10 where \"testId\"=$testId";
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

                          Assertions.assertThat(objects[7].getClass()).isEqualTo(BigInteger.class);
                          Assertions.assertThat(objects[7]).isEqualTo(BigInteger.valueOf(timestamp * 1_000_000_000));
                      });
            }
        }
    }
}
