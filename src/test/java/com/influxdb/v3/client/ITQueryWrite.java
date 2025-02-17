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

import java.io.IOException;
import java.math.BigInteger;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.assertj.core.api.Assertions;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import com.influxdb.v3.client.config.ClientConfig;
import com.influxdb.v3.client.query.QueryOptions;
import com.influxdb.v3.client.write.WriteOptions;
import com.influxdb.v3.client.write.WritePrecision;

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
        long testId = System.currentTimeMillis();
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

        try (Stream<PointValues> stream = client.queryPoints(sql)) {

            List<PointValues> rows = stream.collect(Collectors.toList());

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
        long testId = System.currentTimeMillis();
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
    void queryWriteParameters() {
        client = InfluxDBClient.getInstance(new ClientConfig.Builder()
                .host(System.getenv("TESTING_INFLUXDB_URL"))
                .token(System.getenv("TESTING_INFLUXDB_TOKEN").toCharArray())
                .database(System.getenv("TESTING_INFLUXDB_DATABASE"))
                .build());

        String measurement = "integration_test";
        long testId = System.currentTimeMillis();
        client.writeRecord(measurement + ",type=used value=124.0,testId=" + testId);

        Map<String, Object> parameters = Map.of("id", testId);
        String sql = String.format("SELECT value FROM %s WHERE \"testId\"=$id", measurement);
        try (Stream<Object[]> stream = client.query(sql, parameters)) {
            stream.forEach(row -> {
                Assertions.assertThat(row).hasSize(1);
                Assertions.assertThat(row[0]).isEqualTo(124.0);
            });
        }
        try (Stream<PointValues> stream = client.queryPoints(sql, parameters)) {
            stream.forEach(row -> Assertions.assertThat(row.getField("value")).isEqualTo(124.0));
        }
        try (Stream<VectorSchemaRoot> batches = client.queryBatches(sql, parameters)) {

            Assertions.assertThat(batches.count()).isGreaterThanOrEqualTo(1);
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

    @EnabledIfEnvironmentVariable(named = "TESTING_INFLUXDB_URL", matches = ".*")
    @EnabledIfEnvironmentVariable(named = "TESTING_INFLUXDB_TOKEN", matches = ".*")
    @EnabledIfEnvironmentVariable(named = "TESTING_INFLUXDB_DATABASE", matches = ".*")
    @Test
    void pointValues() {

        Instant timestamp = Instant.now().minus(1, ChronoUnit.DAYS);

        client = InfluxDBClient.getInstance(new ClientConfig.Builder()
                .host(System.getenv("TESTING_INFLUXDB_URL"))
                .token(System.getenv("TESTING_INFLUXDB_TOKEN").toCharArray())
                .database(System.getenv("TESTING_INFLUXDB_DATABASE"))
                .build());

        long testId = System.currentTimeMillis();
        client.writeRecord(String.format("integration_test,location=north value=60.0,testId=%d %s",
                testId, timestamp.toEpochMilli()), new WriteOptions.Builder().precision(WritePrecision.MS).build());

        String sql = String.format("SELECT * FROM integration_test WHERE \"testId\"=%d", testId);
        try (Stream<PointValues> stream = client.queryPoints(sql)) {
            List<PointValues> values = stream.collect(Collectors.toList());

            Assertions.assertThat(values).hasSize(1);

            Assertions.assertThat(values.get(0).getTag("location")).isEqualTo("north");
            Assertions.assertThat(values.get(0).getFloatField("value")).isEqualTo(60.0);

            BigInteger expected = BigInteger.valueOf(timestamp.toEpochMilli() * 1_000_000);
            Assertions.assertThat((BigInteger) values.get(0).getTimestamp()).isEqualByComparingTo(expected);
        }
    }

    /*
    Motivated by EAR 5718, useful exception INVALID_ARGUMENT was being masked by
    INTERNAL: http2 exception - Header size exceeded max allowed size (10240).
     */
    @EnabledIfEnvironmentVariable(named = "TESTING_INFLUXDB_URL", matches = ".*")
    @EnabledIfEnvironmentVariable(named = "TESTING_INFLUXDB_TOKEN", matches = ".*")
    @EnabledIfEnvironmentVariable(named = "TESTING_INFLUXDB_DATABASE", matches = ".*")
    @Test
    public void handleFlightRuntimeException() throws IOException {
        Instant now = Instant.now();
        String measurement = String.format(
          "/%d/test/com/influxdb/v3/client/ITQueryWrite/handleFlightRuntimeException", now.toEpochMilli()
        );

        client = getInstance();

        int extraTagLength = 512;
        Map<String, String> extraTags = new HashMap<String, String>();
        for (int i = 0; i < 22; i++) {
            extraTags.put(makeLengthyTag(extraTagLength, 64, (byte) '/'), "extra-tag-" + i);
        }

        Point p = Point.measurement(measurement)
          .setTag("id", "thx1138")
          .setTag("model", "xc11")
          .setTags(extraTags)
          .setFloatField("speed", 3.14)
          .setFloatField("bearing", 3.14 * 0.5)
          .setIntegerField("ticks", 42)
          .setStringField("location", "/earth/4/12/9/15/1")
          .setTimestamp(now);

        try {
            client.writePoint(p);
        } catch (InfluxDBApiException idbae) {
            Assertions.fail(idbae);
        }

        String faultyQuery = String.format("SELECT * FROM \"%s\" WHERE idx = 'thx1138'", measurement);

        try (Stream<Object[]> stream = client.query(faultyQuery)) {
            stream.forEach(row -> {
                for (Object o : row) {
                    System.out.print(o + " ");
                }
                System.out.print("\n");
            });
        } catch (FlightRuntimeException fre) {
            Assertions.assertThat(fre.getMessage()).doesNotContain("http2 exception");
            Assertions.assertThat(fre.status().code()).isNotEqualTo(CallStatus.INTERNAL.code());
            Assertions.assertThat(fre.status().code()).
              as(String.format("Flight runtime exception was UNAVAILABLE.  "
                  + "Target test case was not fully tested.  "
                  + "Check limits of test account and target database %s.",
                System.getenv("TESTING_INFLUXDB_DATABASE")))
              .isNotEqualTo(CallStatus.UNAVAILABLE.code());
            Assertions.assertThat(fre.status().code()).
              as("Flight runtime exception was UNAUTHENTICATED.  "
                + "Target test case was not fully tested. Check test account token.")
              .isNotEqualTo(CallStatus.UNAUTHENTICATED.code());
            return;
        } catch (Exception e) {
            Assertions.fail(String.format("FlightRuntimeException should have been thrown.  "
              + "Instead received %s.", e));
        }

        Assertions.fail("FlightRuntimeException should have been thrown.  Instead final query passed.");

    }

    @NotNull
    private static InfluxDBClient getInstance() {
        return InfluxDBClient.getInstance(
                System.getenv("TESTING_INFLUXDB_URL"),
                System.getenv("TESTING_INFLUXDB_TOKEN").toCharArray(),
                System.getenv("TESTING_INFLUXDB_DATABASE"));
    }

    private String makeLengthyTag(final int length, final int maxPartLength, final byte separator) {
        final String legalVals = "0123456789abcdefghijklmnopqrstuvwxyz";
        byte[] bytes = new byte[length];
        int nextPartAddress = 0;
        for (int i = 0; i < length; i++) {
            if (i == nextPartAddress) {
                bytes[i] = separator;
                nextPartAddress = i + (int) (Math.random() * (maxPartLength - 3));
            } else {
                bytes[i] = legalVals.getBytes()[(int) (Math.random() * legalVals.length())];
            }
        }
        return new String(bytes);
    }
}
