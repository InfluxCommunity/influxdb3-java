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
import java.time.Instant;
import java.util.ArrayList;
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
import org.apache.arrow.vector.util.Text;

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
        Arguments.checkNotNull(config, "config");

        config.validate();

        this.config = config;
        this.restClient = new RestClient(config);
        this.flightSqlClient = new FlightSqlClient(config);
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
        return query(query, QueryOptions.DEFAULTS);
    }

    @Nonnull
    @Override
    public Stream<Object[]> query(@Nonnull final String query, @Nonnull final QueryOptions options) {
        return queryData(query, options)
                .flatMap(vector -> {
                    List<FieldVector> fieldVectors = vector.getFieldVectors();
                    return IntStream
                            .range(0, vector.getRowCount())
                            .mapToObj(rowNumber -> {

                                ArrayList<Object> row = new ArrayList<>();
                                for (FieldVector fieldVector : fieldVectors) {
                                    row.add(fieldVector.getObject(rowNumber));
                                }
                                return row.toArray();
                            });
                });
    }

	@Nonnull
	@Override
	public Stream<PointValues> queryPoints(@Nonnull final String query)  {
		return queryPoints(query, QueryOptions.DEFAULTS);
	}

	@Nonnull
    @Override
    public Stream<PointValues> queryPoints(@Nonnull final String query, @Nonnull final QueryOptions options) {
        return queryData(query, options)
                .flatMap(vector -> {
                    List<FieldVector> fieldVectors = vector.getFieldVectors();
                    return IntStream
                            .range(0, vector.getRowCount())
                            .mapToObj(rowNumber -> {
                                PointValues p = new PointValues();


                                ArrayList<Object> row = new ArrayList<>();
                                for (int i = 0; i < fieldVectors.size(); i++) {
                                    var schema = vector.getSchema().getFields().get(i);
                                    var value = fieldVectors.get(i).getObject(rowNumber);
                                    var name = schema.getName();
                                    var meta = schema.getMetadata();

                                    if (value instanceof Text) {
                                        value = value.toString();
                                    }

                                    if ((Objects.equals(name, "measurement")
                                            || Objects.equals(name, "iox::measurement"))
                                        && value instanceof String) {
                                        p.setMeasurement((String) value);
                                        continue;
                                    }

                                    if (!meta.isEmpty()) {
                                        if (Objects.equals(name, "time") && value instanceof Instant) {
                                            p.setTimestamp((Instant) value);
                                        } else {
                                            // just push as field If you don't know what type is it
                                            p.setField(name, value);
                                        }

                                        continue;
                                    }

                                    String type = schema.getMetadata().get("iox::column::type");
                                    String[] parts = type.split(":");
                                    String valueType = parts[2];

                                    if ("field".equals(valueType)) {
                                        p.setField(name, value);
                                    } else if ("tag".equals(valueType) && value instanceof String) {
                                        p.setTag(name, (String) value);
                                    } else if ("timestamp".equals(valueType) && value instanceof Instant) {
                                        p.setTimestamp((Instant) value);
                                    }
                                }

                                return p;
                            });
                });
    }

    @Nonnull
    @Override
    public Stream<VectorSchemaRoot> queryBatches(@Nonnull final String query) {
        return queryBatches(query, QueryOptions.DEFAULTS);
    }

    @Nonnull
    @Override
    public Stream<VectorSchemaRoot> queryBatches(@Nonnull final String query,
                                                 @Nonnull final QueryOptions options) {
        return queryData(query, options);
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

        String lineProtocol = data.stream().map(item -> {
                    if (item == null) {
                        return null;
                    } else if (item instanceof Point) {
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

        restClient.request("api/v2/write", HttpMethod.POST, body, headers, queryParams);
    }

    @Nonnull
    private Stream<VectorSchemaRoot> queryData(@Nonnull final String query,
                                               @Nonnull final QueryOptions options) {

        Arguments.checkNonEmpty(query, "query");
        Arguments.checkNotNull(options, "options");

        if (closed) {
            throw new IllegalStateException("InfluxDBClient has been closed.");
        }

        String database = options.databaseSafe(config);
        if (database == null || database.isEmpty()) {
            throw new IllegalStateException(DATABASE_REQUIRED_MESSAGE);
        }

        return flightSqlClient.execute(query, database, options.queryTypeSafe());
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
