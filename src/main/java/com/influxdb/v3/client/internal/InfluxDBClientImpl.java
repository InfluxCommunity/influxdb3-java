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

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import io.netty.handler.codec.http.HttpMethod;

import com.influxdb.v3.client.InfluxDBClient;
import com.influxdb.v3.client.config.InfluxDBClientConfigs;
import com.influxdb.v3.client.write.Point;
import com.influxdb.v3.client.write.WriteParameters;
import com.influxdb.v3.client.write.WritePrecision;

public final class InfluxDBClientImpl implements InfluxDBClient {

    private static final Logger LOG = Logger.getLogger(InfluxDBClientImpl.class.getName());

    private boolean closed = false;

    private final InfluxDBClientConfigs configs;
    private final RestClient restClient;

    public InfluxDBClientImpl(@Nonnull final InfluxDBClientConfigs configs) {
        Arguments.checkNotNull(configs, "configs");

        configs.validate();

        this.configs = configs;
        this.restClient = new RestClient(configs);
    }

    @Override
    public void writeRecord(@Nullable final String record) {
        writeRecord(record, WriteParameters.DEFAULTS);
    }

    @Override
    public void writeRecord(@Nullable final String record, @Nonnull final WriteParameters parameters) {
        if (record == null) {
            return;
        }

        writeRecords(Collections.singletonList(record), parameters);
    }

    @Override
    public void writeRecords(@Nonnull final List<String> records) {
        writeRecords(records, WriteParameters.DEFAULTS);
    }

    @Override
    public void writeRecords(@Nonnull final List<String> records, @Nonnull final WriteParameters parameters) {
        writeData(records, parameters);
    }

    @Override
    public void writePoint(@Nullable final Point point) {
        writePoint(point, WriteParameters.DEFAULTS);
    }

    @Override
    public void writePoint(@Nullable final Point point, @Nonnull final WriteParameters parameters) {
        if (point == null) {
            return;
        }

        writePoints(Collections.singletonList(point), parameters);
    }

    @Override
    public void writePoints(@Nonnull final List<Point> points) {
        writePoints(points, WriteParameters.DEFAULTS);
    }

    @Override
    public void writePoints(@Nonnull final List<Point> points, @Nonnull final WriteParameters parameters) {
        writeData(points, parameters);
    }

    @Override
    public void close() {
        restClient.close();
        closed = true;
    }

    private <T> void writeData(@Nonnull final List<T> data, @Nonnull final WriteParameters parameters) {

        Arguments.checkNotNull(data, "data");
        Arguments.checkNotNull(parameters, "parameters");

        if (closed) {
            throw new IllegalStateException("InfluxDBClient has been closed.");
        }

        String database = parameters.databaseSafe(configs);
        if (database == null || database.isEmpty()) {
            throw new IllegalStateException("Please specify the 'Database' as a method parameter "
                    + "or use default configuration at 'InfluxDBClientConfigs.database'.");
        }

        WritePrecision precision = parameters.precisionSafe(configs);
        Map<String, String> queryParams = new HashMap<String, String>() {{
            put("bucket", database);
            put("org", parameters.organizationSafe(configs));
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

        restClient.request("/api/v2/write", HttpMethod.POST, lineProtocol, "text/plain; charset=utf-8", queryParams);
    }
}
