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
package com.influxdb.v3.client.internal;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.zip.GZIPOutputStream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import io.netty.handler.codec.http.HttpMethod;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;

import com.influxdb.v3.client.InfluxDBApiException;
import com.influxdb.v3.client.InfluxDBClient;
import com.influxdb.v3.client.Point;
import com.influxdb.v3.client.PointValues;
import com.influxdb.v3.client.config.ClientConfig;
import com.influxdb.v3.client.query.QueryOptions;
import com.influxdb.v3.client.write.WriteOptions;
import com.influxdb.v3.client.write.WritePrecision;

/**
 * Implementation of the InfluxDBClient. It is thread-safe and can be safely shared between threads.
 * <p>
 * Please use {@link InfluxDBClient} to create an instance.
 */
public final class InfluxDBClientImpl implements InfluxDBClient {

    private static final Logger LOG = Logger.getLogger(InfluxDBClientImpl.class.getName());

    private static final String DATABASE_REQUIRED_MESSAGE = "Please specify the 'Database' as a method parameter "
            + "or use default configuration at 'ClientConfig.database'.";

    private static final Map<String, Object> NO_PARAMETERS = Map.of();
    private static final List<Class<?>> ALLOWED_NAMED_PARAMETER_TYPES = List.of(
            String.class,
            Integer.class,
            Long.class,
            Float.class,
            Double.class,
            Boolean.class
    );

    private boolean closed = false;
    private final ClientConfig config;

    private final RestClient restClient;
    private final FlightSqlClient flightSqlClient;

    /**
     * Creates an instance using the specified config.
     * <p>
     * Please use {@link InfluxDBClient} to create an instance.
     *
     * @param config the client config.
     */
    public InfluxDBClientImpl(@Nonnull final ClientConfig config) {
        this(config, null, null);
    }

    /**
     * Constructor for testing purposes.
     *
     * @param config          the client config
     * @param restClient      the rest client, if null a new client will be created
     * @param flightSqlClient the flight sql client, if null a new client will be created
     */
    InfluxDBClientImpl(@Nonnull final ClientConfig config,
                       @Nullable final RestClient restClient,
                       @Nullable final FlightSqlClient flightSqlClient) {
        Arguments.checkNotNull(config, "config");

        config.validate();

        this.config = config;
        this.restClient = restClient != null ? restClient : new RestClient(config);
        this.flightSqlClient = flightSqlClient != null ? flightSqlClient : new FlightSqlClient(config);
    }

    @Override
    public void writeRecord(@Nullable final String record) {
        writeRecord(record, WriteOptions.DEFAULTS);
    }

    @Override
    public void writeRecord(@Nullable final String record, @Nonnull final WriteOptions options) {
        if (record == null) {
            return;
        }

        writeRecords(Collections.singletonList(record), options);
    }

    @Override
    public void writeRecords(@Nonnull final List<String> records) {
        writeRecords(records, WriteOptions.DEFAULTS);
    }

    @Override
    public void writeRecords(@Nonnull final List<String> records, @Nonnull final WriteOptions options) {
        writeData(records, options);
    }

    @Override
    public void writePoint(@Nullable final Point point) {
        writePoint(point, WriteOptions.DEFAULTS);
    }

    @Override
    public void writePoint(@Nullable final Point point, @Nonnull final WriteOptions options) {
        if (point == null) {
            return;
        }

        writePoints(Collections.singletonList(point), options);
    }

    @Override
    public void writePoints(@Nonnull final List<Point> points) {
        writePoints(points, WriteOptions.DEFAULTS);
    }

    @Override
    public void writePoints(@Nonnull final List<Point> points, @Nonnull final WriteOptions options) {
        writeData(points, options);
    }

    @Nonnull
    @Override
    public Stream<Object[]> query(@Nonnull final String query) {
        return query(query, NO_PARAMETERS, QueryOptions.DEFAULTS);
    }

    @Nonnull
    @Override
    public Stream<Object[]> query(@Nonnull final String query, @Nonnull final QueryOptions options) {
        return query(query, NO_PARAMETERS, options);
    }

    @Nonnull
    @Override
    public Stream<Object[]> query(@Nonnull final String query, @Nonnull final Map<String, Object> parameters) {
        return query(query, parameters, QueryOptions.DEFAULTS);
    }

    @Nonnull
    @Override
    public Stream<Object[]> query(@Nonnull final String query,
                                  @Nonnull final Map<String, Object> parameters,
                                  @Nonnull final QueryOptions options) {
        return queryData(query, parameters, options)
                .flatMap(vector -> IntStream.range(0, vector.getRowCount())
                                        .mapToObj(rowNumber ->
                                        VectorSchemaRootConverter.INSTANCE
                                                                 .getArrayObjectFromVectorSchemaRoot(
                                                                         vector,
                                                                         rowNumber
                                                                 )));
    }

    @Nonnull
    @Override
    public Stream<Map<String, Object>> queryRows(@Nonnull final String query) {
        return queryRows(query, NO_PARAMETERS, QueryOptions.DEFAULTS);
    }

    @Nonnull
    @Override
    public Stream<Map<String, Object>> queryRows(@Nonnull final String query,
                                                 @Nonnull final Map<String, Object> parameters
    ) {
        return queryRows(query, parameters, QueryOptions.DEFAULTS);
    }

    @Nonnull
    @Override
    public Stream<Map<String, Object>> queryRows(@Nonnull final String query, @Nonnull final QueryOptions options) {
        return queryRows(query, NO_PARAMETERS, options);
    }

    @Nonnull
    @Override
    public Stream<Map<String, Object>> queryRows(@Nonnull final String query,
                                                 @Nonnull final Map<String, Object> parameters,
                                                 @Nonnull final QueryOptions options) {
        return queryData(query, parameters, options)
                .flatMap(vector -> IntStream.range(0, vector.getRowCount())
                                            .mapToObj(rowNumber ->
                                                              VectorSchemaRootConverter.INSTANCE
                                                                      .getMapFromVectorSchemaRoot(
                                                                              vector,
                                                                              rowNumber
                                                                      )));
    }

    @Nonnull
    @Override
    public Stream<PointValues> queryPoints(@Nonnull final String query) {
        return queryPoints(query, QueryOptions.DEFAULTS);
    }

    @Nonnull
    @Override
    public Stream<PointValues> queryPoints(@Nonnull final String query, @Nonnull final QueryOptions options) {
        return queryPoints(query, NO_PARAMETERS, options);
    }

    @Nonnull
    @Override
    public Stream<PointValues> queryPoints(@Nonnull final String query, @Nonnull final Map<String, Object> parameters) {
        return queryPoints(query, parameters, QueryOptions.DEFAULTS);
    }

    @Nonnull
    @Override
    public Stream<PointValues> queryPoints(@Nonnull final String query,
                                           @Nonnull final Map<String, Object> parameters,
                                           @Nonnull final QueryOptions options) {
        return queryData(query, parameters, options)
                .flatMap(vector -> {
                    List<FieldVector> fieldVectors = vector.getFieldVectors();
                    return IntStream
                            .range(0, vector.getRowCount())
                            .mapToObj(row ->
                                    VectorSchemaRootConverter.INSTANCE.toPointValues(row, fieldVectors));
                });
    }

    @Nonnull
    @Override
    public Stream<VectorSchemaRoot> queryBatches(@Nonnull final String query) {
        return queryBatches(query, QueryOptions.DEFAULTS);
    }

    @Nonnull
    @Override
    public Stream<VectorSchemaRoot> queryBatches(@Nonnull final String query, @Nonnull final QueryOptions options) {
        return queryBatches(query, NO_PARAMETERS, options);
    }

    @Nonnull
    @Override
    public Stream<VectorSchemaRoot> queryBatches(@Nonnull final String query,
                                                 @Nonnull final Map<String, Object> parameters) {
        return queryBatches(query, parameters, QueryOptions.DEFAULTS);
    }

    @Nonnull
    @Override
    public Stream<VectorSchemaRoot> queryBatches(@Nonnull final String query,
                                                 @Nonnull final Map<String, Object> parameters,
                                                 @Nonnull final QueryOptions options) {
        return queryData(query, parameters, options);
    }

    @Override
    public void close() throws Exception {
        restClient.close();
        flightSqlClient.close();
        closed = true;
    }

    private <T> void writeData(@Nonnull final List<T> data, @Nonnull final WriteOptions options) {

        Arguments.checkNotNull(data, "data");
        Arguments.checkNotNull(options, "options");

        if (closed) {
            throw new IllegalStateException("InfluxDBClient has been closed.");
        }

        String database = options.databaseSafe(config);
        if (database == null || database.isEmpty()) {
            throw new IllegalStateException("Please specify the 'Database' as a method parameter "
                    + "or use default configuration at 'ClientConfig.database'.");
        }

        WritePrecision precision = options.precisionSafe(config);

        Map<String, String> queryParams = new HashMap<>() {{
            put("bucket", database);
            put("org", config.getOrganization());
            put("precision", precision.name().toLowerCase());
        }};

        Map<String, String> defaultTags = options.defaultTagsSafe(config);

        String lineProtocol = data.stream().map(item -> {
                    if (item == null) {
                        return null;
                    } else if (item instanceof Point) {
                        for (String key : defaultTags.keySet()) {
                            ((Point) item).setTag(key, defaultTags.get(key));
                        }
                        return ((Point) item).toLineProtocol();
                    } else {
                        return item.toString();
                    }
                })
                .filter(it -> it != null && !it.isEmpty())
                .collect(Collectors.joining("\n"));

        if (lineProtocol.isEmpty()) {
            LOG.warning("No data to write, please check your input data.");
            return;
        }

        Map<String, String> headers = new HashMap<>(Map.of("Content-Type", "text/plain; charset=utf-8"));
        byte[] body = lineProtocol.getBytes(StandardCharsets.UTF_8);
        if (lineProtocol.length() >= options.gzipThresholdSafe(config)) {
            try {
                body = gzipData(lineProtocol.getBytes(StandardCharsets.UTF_8));
                headers.put("Content-Encoding", "gzip");
            } catch (IOException e) {
                throw new InfluxDBApiException(e);
            }
        }
        headers.putAll(options.headersSafe());

        restClient.request("api/v2/write", HttpMethod.POST, body, queryParams, headers);
    }

    @Nonnull
    private Stream<VectorSchemaRoot> queryData(@Nonnull final String query,
                                               @Nonnull final Map<String, Object> parameters,
                                               @Nonnull final QueryOptions options) {

        Arguments.checkNonEmpty(query, "query");
        Arguments.checkNotNull(parameters, "parameters");
        Arguments.checkNotNull(options, "options");

        if (closed) {
            throw new IllegalStateException("InfluxDBClient has been closed.");
        }

        String database = options.databaseSafe(config);
        if (database == null || database.isEmpty()) {
            throw new IllegalStateException(DATABASE_REQUIRED_MESSAGE);
        }

        parameters.forEach((k, v) -> {
            if (!Objects.isNull(v) && !ALLOWED_NAMED_PARAMETER_TYPES.contains(v.getClass())) {
                throw new IllegalArgumentException(String.format("The parameter %s value has unsupported type: %s",
                        k, v.getClass()));
            }
        });

        return flightSqlClient.execute(query, database, options.queryTypeSafe(), parameters, options.headersSafe());
    }

    @Nonnull
    private byte[] gzipData(@Nonnull final byte[] data) throws IOException {
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        final GZIPOutputStream gzip = new GZIPOutputStream(out);
        gzip.write(data);
        gzip.close();

        return out.toByteArray();
    }
}
