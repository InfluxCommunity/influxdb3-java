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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.influxdb.v3.client.config.InfluxDBClientConfigs;
import com.influxdb.v3.client.internal.InfluxDBClientImpl;

/**
 * The InfluxDBClient interface provides a client for interact with InfluxDB 3.
 * This client supports both writing data to InfluxDB and querying data using the FlightSQL client,
 * which allows you to execute SQL queries against InfluxDB IOx.
 */
public interface InfluxDBClient extends AutoCloseable {

    /**
     * Creates a new instance of the {@link InfluxDBClient} for interacting with an InfluxDB server, simplifying
     * common operations such as writing, querying.
     *
     * @param hostUrl   the hostname or IP address of the InfluxDB server
     * @param authToken the authentication token for accessing the InfluxDB server, can be null
     * @param database  the database to be used for InfluxDB operations, can be null
     * @return new instance of the {@link InfluxDBClient}
     */
    @Nonnull
    static InfluxDBClient getInstance(@Nonnull final String hostUrl,
                                      @Nullable final String authToken,
                                      @Nullable final String database) {
        InfluxDBClientConfigs configs = new InfluxDBClientConfigs.Builder()
                .hostUrl(hostUrl)
                .authToken(authToken)
                .database(database)
                .build();

        return getInstance(configs);
    }

    /**
     * Creates a new instance of the {@link InfluxDBClient} for interacting with an InfluxDB server, simplifying
     * common operations such as writing, querying.
     *
     * @param configs the configuration for the InfluxDB client
     * @return new instance of the {@link InfluxDBClient}
     */
    @Nonnull
    static InfluxDBClient getInstance(@Nonnull final InfluxDBClientConfigs configs) {
        return new InfluxDBClientImpl(configs);
    }
}
