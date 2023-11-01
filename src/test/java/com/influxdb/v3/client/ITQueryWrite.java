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

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.assertj.core.api.Assertions;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import com.influxdb.v3.client.config.ClientConfig;
import com.influxdb.v3.client.query.QueryOptions;

class ITQueryWrite {

    private InfluxDBClient client;

    @AfterEach
    void closeClient() throws Exception {
        if (client != null) {
            client.close();
        }
    }

    @EnabledIfEnvironmentVariable(named = "TESTING_INFLUXDB_URL", matches = ".*")
    @EnabledIfEnvironmentVariable(named = "TESTING_INFLUXDB_TOKEN", matches = ".*")
    @EnabledIfEnvironmentVariable(named = "TESTING_INFLUXDB_DATABASE", matches = ".*")
    @Test
    void queryWrite() {
        client = getInstance();

        String measurement = "integration_test";
        int testId = (int) System.currentTimeMillis();
        client.writeRecord(measurement + ",type=used value=123.0,testId=" + testId);

        String sql = String.format("SELECT value FROM %s WHERE \"testId\"=%d", measurement, testId);
        try (Stream<Object[]> stream = client.query(sql)) {

            stream.forEach(row -> {

                Assertions.assertThat(row).hasSize(1);
                Assertions.assertThat(row[0]).isEqualTo(123.0);
            });
        }

        try (Stream<Object[]> stream = client.query(sql)) {

            List<Object[]> rows = stream.collect(Collectors.toList());

            Assertions.assertThat(rows).hasSize(1);
        }

        String influxQL = String.format("SELECT MEAN(value) FROM %s WHERE \"testId\"=%d "
                + "group by time(1s) fill(none) order by time desc limit 1", measurement, testId);
        try (Stream<Object[]> stream = client.query(influxQL, QueryOptions.INFLUX_QL)) {

            List<Object[]> rows = stream.collect(Collectors.toList());

            Assertions.assertThat(rows).hasSize(1);
        }
    }

    @EnabledIfEnvironmentVariable(named = "TESTING_INFLUXDB_URL", matches = ".*")
    @EnabledIfEnvironmentVariable(named = "TESTING_INFLUXDB_TOKEN", matches = ".*")
    @EnabledIfEnvironmentVariable(named = "TESTING_INFLUXDB_DATABASE", matches = ".*")
    @Test
    void queryBatches() {
        client = getInstance();

        try (Stream<VectorSchemaRoot> batches = client.queryBatches("SELECT * FROM integration_test")) {

            List<VectorSchemaRoot> batchesAsList = batches.collect(Collectors.toList());

            Assertions.assertThat(batchesAsList.size()).isGreaterThanOrEqualTo(1);
        }
    }

    @EnabledIfEnvironmentVariable(named = "TESTING_INFLUXDB_URL", matches = ".*")
    @EnabledIfEnvironmentVariable(named = "TESTING_INFLUXDB_TOKEN", matches = ".*")
    @EnabledIfEnvironmentVariable(named = "TESTING_INFLUXDB_DATABASE", matches = ".*")
    @Test
    void queryWriteGzip() {
        client = InfluxDBClient.getInstance(new ClientConfig.Builder()
                .host(System.getenv("TESTING_INFLUXDB_URL"))
                .token(System.getenv("TESTING_INFLUXDB_TOKEN").toCharArray())
                .database(System.getenv("TESTING_INFLUXDB_DATABASE"))
                .gzipThreshold(1)
                .build());

        String measurement = "integration_test";
        int testId = (int) System.currentTimeMillis();
        client.writeRecord(measurement + ",type=used value=123.0,testId=" + testId);

        String sql = String.format("SELECT value FROM %s WHERE \"testId\"=%d", measurement, testId);
        try (Stream<Object[]> stream = client.query(sql)) {
            stream.forEach(row -> {
                Assertions.assertThat(row).hasSize(1);
                Assertions.assertThat(row[0]).isEqualTo(123.0);
            });
        }
    }

    @EnabledIfEnvironmentVariable(named = "TESTING_INFLUXDB_URL", matches = ".*")
    @EnabledIfEnvironmentVariable(named = "TESTING_INFLUXDB_TOKEN", matches = ".*")
    @EnabledIfEnvironmentVariable(named = "TESTING_INFLUXDB_DATABASE", matches = ".*")
    @Test
    void iteratingMoreVectorSchemaRoots() {
        client = InfluxDBClient.getInstance(new ClientConfig.Builder()
                .host(System.getenv("TESTING_INFLUXDB_URL"))
                .token(System.getenv("TESTING_INFLUXDB_TOKEN").toCharArray())
                .database(System.getenv("TESTING_INFLUXDB_DATABASE"))
                .gzipThreshold(1)
                .build());

        String query = "SELECT name FROM (VALUES ('Alice', 4.56), ('Bob', 8.1)) AS data(name, value) group by name";
        try (Stream<Object[]> stream = client.query(query)) {
            Object[] names = stream.map(row -> row[0].toString()).toArray();

            Assertions.assertThat(names).contains("Alice");
            Assertions.assertThat(names).contains("Bob");
        }
    }

    @NotNull
    private static InfluxDBClient getInstance() {
        return InfluxDBClient.getInstance(
                System.getenv("TESTING_INFLUXDB_URL"),
                System.getenv("TESTING_INFLUXDB_TOKEN").toCharArray(),
                System.getenv("TESTING_INFLUXDB_DATABASE"));
    }
}
