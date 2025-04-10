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
package com.influxdb.v3.client.integration;

import java.math.BigInteger;
import java.net.ConnectException;
import java.net.URL;
import java.net.URLConnection;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.logging.Logger;
import java.util.stream.Stream;
import javax.annotation.Nonnull;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import com.influxdb.v3.client.InfluxDBClient;
import com.influxdb.v3.client.Point;
import com.influxdb.v3.client.PointValues;
import com.influxdb.v3.client.config.ClientConfig;
import com.influxdb.v3.client.write.WriteOptions;
import com.influxdb.v3.client.write.WritePrecision;

import static org.junit.jupiter.api.Assumptions.assumeFalse;

public class E2ETest {

    private static final java.util.logging.Logger LOG = Logger.getLogger(E2ETest.class.getName());

    @EnabledIfEnvironmentVariable(named = "TESTING_INFLUXDB_URL", matches = ".*")
    @EnabledIfEnvironmentVariable(named = "TESTING_INFLUXDB_TOKEN", matches = ".*")
    @EnabledIfEnvironmentVariable(named = "TESTING_INFLUXDB_DATABASE", matches = ".*")
    @Test
    void testQueryWithProxy() {
        String proxyUrl = "http://localhost:10000";

        try {
            // Continue to run this test only if Envoy proxy is running on this address http://localhost:10000
            String url = String.format("http://%s:%d", "localhost", 10000);
            URLConnection hpCon = new URL(url).openConnection();
            hpCon.connect();
        } catch (Exception e) {
            if (e instanceof ConnectException && e.getMessage().contains("Connection refused")) {
                LOG.warning("Tests with proxy have been skipped because no proxy is running on " + proxyUrl);
                assumeFalse(e.getMessage().contains("Connection refused"));
                return;
            }
        }

        ClientConfig clientConfig = new ClientConfig.Builder()
                .host(System.getenv("TESTING_INFLUXDB_URL"))
                .token(System.getenv("TESTING_INFLUXDB_TOKEN").toCharArray())
                .database(System.getenv("TESTING_INFLUXDB_DATABASE"))
                .proxyUrl(proxyUrl)
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

    @EnabledIfEnvironmentVariable(named = "TESTING_INFLUXDB_URL", matches = ".*")
    @EnabledIfEnvironmentVariable(named = "TESTING_INFLUXDB_TOKEN", matches = ".*")
    @EnabledIfEnvironmentVariable(named = "TESTING_INFLUXDB_DATABASE", matches = ".*")
    @Test
    void correctSslCertificates() throws Exception {
        // This is real certificate downloaded from https://cloud2.influxdata.com
        String influxDBcertificateFile = "src/test/java/com/influxdb/v3/client/testdata/influxdb-certificate.pem";

        ClientConfig clientConfig = new ClientConfig.Builder()
                .host(System.getenv("TESTING_INFLUXDB_URL"))
                .token(System.getenv("TESTING_INFLUXDB_TOKEN").toCharArray())
                .database(System.getenv("TESTING_INFLUXDB_DATABASE"))
                .sslRootsFilePath(influxDBcertificateFile)
                .build();
        InfluxDBClient influxDBClient = InfluxDBClient.getInstance(clientConfig);
        assertGetDataSuccess(influxDBClient);
    }

    @EnabledIfEnvironmentVariable(named = "TESTING_INFLUXDB_URL", matches = ".*")
    @EnabledIfEnvironmentVariable(named = "TESTING_INFLUXDB_TOKEN", matches = ".*")
    @EnabledIfEnvironmentVariable(named = "TESTING_INFLUXDB_DATABASE", matches = ".*")
    @Test
    void wrongSslCertificate() {
        String certificateFile = "src/test/java/com/influxdb/v3/client/testdata/docker.com.pem";

        ClientConfig clientConfig = new ClientConfig.Builder()
                .host(System.getenv("TESTING_INFLUXDB_URL"))
                .token(System.getenv("TESTING_INFLUXDB_TOKEN").toCharArray())
                .database(System.getenv("TESTING_INFLUXDB_DATABASE"))
                .sslRootsFilePath(certificateFile)
                .build();
        InfluxDBClient influxDBClient = InfluxDBClient.getInstance(clientConfig);
        Assertions.assertThatThrownBy(() -> assertGetDataSuccess(influxDBClient))
                .isInstanceOf(Exception.class);
    }

    @EnabledIfEnvironmentVariable(named = "TESTING_INFLUXDB_URL", matches = ".*")
    @EnabledIfEnvironmentVariable(named = "TESTING_INFLUXDB_TOKEN", matches = ".*")
    @EnabledIfEnvironmentVariable(named = "TESTING_INFLUXDB_DATABASE", matches = ".*")
    @Test
    void disableServerCertificateValidation() {
        String wrongCertificateFile = "src/test/java/com/influxdb/v3/client/testdata/docker.com.pem";

        ClientConfig clientConfig = new ClientConfig.Builder()
                .host(System.getenv("TESTING_INFLUXDB_URL"))
                .token(System.getenv("TESTING_INFLUXDB_TOKEN").toCharArray())
                .database(System.getenv("TESTING_INFLUXDB_DATABASE"))
                .disableServerCertificateValidation(true)
                .sslRootsFilePath(wrongCertificateFile)
                .build();

        // Test succeeded with wrong certificate file because disableServerCertificateValidation is true
        InfluxDBClient influxDBClient = InfluxDBClient.getInstance(clientConfig);
        assertGetDataSuccess(influxDBClient);
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

    private void assertGetDataSuccess(@Nonnull final InfluxDBClient influxDBClient) {
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
