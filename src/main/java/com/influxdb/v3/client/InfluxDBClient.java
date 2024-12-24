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

import java.net.MalformedURLException;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.arrow.vector.VectorSchemaRoot;

import com.influxdb.v3.client.config.ClientConfig;
import com.influxdb.v3.client.internal.InfluxDBClientImpl;
import com.influxdb.v3.client.query.QueryOptions;
import com.influxdb.v3.client.write.WriteOptions;

/**
 * The InfluxDBClient interface provides a client for interact with InfluxDB 3.
 * This client supports both writing data to InfluxDB and querying data using the FlightSQL client,
 * which allows you to execute SQL queries against InfluxDB IOx.
 */
public interface InfluxDBClient extends AutoCloseable {

    /**
     * Write a record specified in the InfluxDB Line Protocol to the InfluxDB server.
     *
     * @param record the record specified in the InfluxDB Line Protocol, can be null
     */
    void writeRecord(@Nullable final String record);

    /**
     * Write a record specified in the InfluxDB Line Protocol to the InfluxDB server.
     *
     * @param record  the record specified in the InfluxDB Line Protocol, can be null
     * @param options the options for writing data to InfluxDB
     */
    void writeRecord(@Nullable final String record, @Nonnull final WriteOptions options);

    /**
     * Write records specified in the InfluxDB Line Protocol to the InfluxDB server.
     *
     * @param records the records specified in the InfluxDB Line Protocol, cannot be null
     */
    void writeRecords(@Nonnull final List<String> records);

    /**
     * Write records specified in the InfluxDB Line Protocol to the InfluxDB server.
     *
     * @param records the records specified in the InfluxDB Line Protocol, cannot be null
     * @param options the options for writing data to InfluxDB
     */
    void writeRecords(@Nonnull final List<String> records, @Nonnull final WriteOptions options);

    /**
     * Write a {@link Point} to the InfluxDB server.
     *
     * @param point the {@link Point} to write, can be null
     */
    void writePoint(@Nullable final Point point);

    /**
     * Write a {@link Point} to the InfluxDB server.
     *
     * @param point   the {@link Point} to write, can be null
     * @param options the options for writing data to InfluxDB
     */
    void writePoint(@Nullable final Point point, @Nonnull final WriteOptions options);

    /**
     * Write a list of {@link Point} to the InfluxDB server.
     *
     * @param points the list of {@link Point} to write, cannot be null
     */
    void writePoints(@Nonnull final List<Point> points);

    /**
     * Write a list of {@link Point} to the InfluxDB server.
     *
     * @param points  the list of {@link Point} to write, cannot be null
     * @param options the options for writing data to InfluxDB
     */
    void writePoints(@Nonnull final List<Point> points, @Nonnull final WriteOptions options);

    /**
     * Query data from InfluxDB IOx using FlightSQL.
     * <p>
     * The result stream should be closed after use, you can use try-resource pattern to close it automatically:
     * <pre>
     * try (Stream&lt;Object[]&gt; rows = client.query("select * from cpu")) {
     *      rows.forEach(row -&gt; {
     *          // process row
     *      }
     * });
     * </pre>
     *
     * @param query the SQL query string to execute, cannot be null
     * @return Batches of rows returned by the query
     */
    @Nonnull
    Stream<Object[]> query(@Nonnull final String query);

    /**
     * Query data from InfluxDB IOx using FlightSQL.
     * <p>
     * The result stream should be closed after use, you can use try-resource pattern to close it automatically:
     * <pre>
     * try (Stream&lt;Object[]&gt; rows = client.query("select * from cpu where host=$host",
     *                                                 Map.of("host", "server-a")) {
     *      rows.forEach(row -&gt; {
     *          // process row
     *      }
     * });
     * </pre>
     *
     * @param query      the SQL query string to execute, cannot be null
     * @param parameters query named parameters
     * @return Batches of rows returned by the query
     */
    @Nonnull
    Stream<Object[]> query(@Nonnull final String query, @Nonnull final Map<String, Object> parameters);

    /**
     * Query data from InfluxDB IOx using FlightSQL.
     * <p>
     * The result stream should be closed after use, you can use try-resource pattern to close it automatically:
     * <pre>
     * try (Stream&lt;Object[]&gt; rows = client.query("select * from cpu", options)) {
     *      rows.forEach(row -&gt; {
     *          // process row
     *      }
     * });
     * </pre>
     *
     * @param query   the query string to execute, cannot be null
     * @param options the options for querying data from InfluxDB
     * @return Batches of rows returned by the query
     */
    @Nonnull
    Stream<Object[]> query(@Nonnull final String query, @Nonnull final QueryOptions options);

    /**
     * Query data from InfluxDB IOx using FlightSQL.
     * <p>
     * The result stream should be closed after use, you can use try-resource pattern to close it automatically:
     * <pre>
     * try (Stream&lt;Object[]&gt; rows = client.query("select * from cpu where host=$host",
     *                                                 Map.of("host", "server-a"), options)) {
     *      rows.forEach(row -&gt; {
     *          // process row
     *      }
     * });
     * </pre>
     *
     * @param query      the query string to execute, cannot be null
     * @param parameters query named parameters
     * @param options    the options for querying data from InfluxDB
     * @return Batches of rows returned by the query
     */
    @Nonnull
    Stream<Object[]> query(@Nonnull final String query,
                           @Nonnull final Map<String, Object> parameters,
                           @Nonnull final QueryOptions options);

    /**
     * Query data from InfluxDB IOx using FlightSQL.
     * <p>
     * The result stream should be closed after use, you can use try-resource pattern to close it automatically:
     * <pre>
     * try (Stream&lt;Map&lt;String, Object&gt;&gt; rows = client.query("select * from cpu where host=$host",;
     *                                                 Map.of("host", "server-a"), options)) {
     *      rows.forEach(row -&gt; {
     *          // process row
     *      }
     * });
     * </pre>
     *
     * @param query      the query string to execute, cannot be null
     * @return Batches of rows returned by the query
     */
    @Nonnull
    Stream<Map<String, Object>> queryRows(@Nonnull final String query);

    /**
     * Query data from InfluxDB IOx using FlightSQL.
     * <p>
     * The result stream should be closed after use, you can use try-resource pattern to close it automatically:
     * <pre>
     * try (Stream&lt;Map&lt;String, Object&gt;&gt; rows = client.query("select * from cpu where host=$host",;
     *                                                 Map.of("host", "server-a"), options)) {
     *      rows.forEach(row -&gt; {
     *          // process row
     *      }
     * });
     * </pre>
     *
     * @param query      the query string to execute, cannot be null
     * @param parameters query named parameters
     * @return Batches of rows returned by the query
     */
    @Nonnull
    Stream<Map<String, Object>> queryRows(@Nonnull final String query, @Nonnull final Map<String, Object> parameters);

    /**
     * Query data from InfluxDB IOx using FlightSQL.
     * <p>
     * The result stream should be closed after use, you can use try-resource pattern to close it automatically:
     * <pre>
     * try (Stream&lt;Map&lt;String, Object&gt;&gt; rows = client.query("select * from cpu where host=$host",;
     *                                                 Map.of("host", "server-a"), options)) {
     *      rows.forEach(row -&gt; {
     *          // process row
     *      }
     * });
     * </pre>
     *
     * @param query      the query string to execute, cannot be null
     * @param options the options for querying data from InfluxDB
     * @return Batches of rows returned by the query
     */
    @Nonnull
    Stream<Map<String, Object>> queryRows(@Nonnull final String query, @Nonnull final QueryOptions options);

    /**
     * Query data from InfluxDB IOx using FlightSQL.
     * <p>
     * The result stream should be closed after use, you can use try-resource pattern to close it automatically:
     * <pre>
     * try (Stream&lt;Map&lt;String, Object&gt;&gt; rows = client.query("select * from cpu where host=$host",;
     *                                                 Map.of("host", "server-a"), options)) {
     *      rows.forEach(row -&gt; {
     *          // process row
     *      }
     * });
     * </pre>
     *
     * @param query      the query string to execute, cannot be null
     * @param parameters query named parameters
     * @param options    the options for querying data from InfluxDB
     * @return Batches of rows returned by the query
     */
    @Nonnull
    Stream<Map<String, Object>> queryRows(@Nonnull final String query,
                           @Nonnull final Map<String, Object> parameters,
                           @Nonnull final QueryOptions options);

    /**
     * Query data from InfluxDB IOx into Point structure using FlightSQL.
     * <p>
     * The result stream should be closed after use, you can use try-resource pattern to close it automatically:
     * <pre>
     * try (Stream&lt;PointValues&gt; rows = client.queryPoints("select * from cpu", options)) {
     *      rows.forEach(row -&gt; {
     *          // process row
     *      }
     * });
     * </pre>
     *
     * @param query the SQL query string to execute, cannot be null
     * @return Batches of PointValues returned by the query
     */
    @Nonnull
    Stream<PointValues> queryPoints(@Nonnull final String query);

    /**
     * Query data from InfluxDB IOx into Point structure using FlightSQL.
     * <p>
     * The result stream should be closed after use, you can use try-resource pattern to close it automatically:
     * <pre>
     * try (Stream&lt;PointValues&gt; rows = client.queryPoints("select * from cpu where host=$host",
     *                                                          Map.of("host", "server-a"))) {
     *      rows.forEach(row -&gt; {
     *          // process row
     *      }
     * });
     * </pre>
     *
     * @param query      the SQL query string to execute, cannot be null
     * @param parameters query named parameters
     * @return Batches of PointValues returned by the query
     */
    @Nonnull
    Stream<PointValues> queryPoints(@Nonnull final String query, @Nonnull final Map<String, Object> parameters);

    /**
     * Query data from InfluxDB IOx into Point structure using FlightSQL.
     * <p>
     * The result stream should be closed after use, you can use try-resource pattern to close it automatically:
     * <pre>
     * try (Stream&lt;PointValues&gt; rows = client.queryPoints("select * from cpu", options)) {
     *      rows.forEach(row -&gt; {
     *          // process row
     *      }
     * });
     * </pre>
     *
     * @param query   the query string to execute, cannot be null
     * @param options the options for querying data from InfluxDB
     * @return Batches of PointValues returned by the query
     */
    @Nonnull
    Stream<PointValues> queryPoints(@Nonnull final String query, @Nonnull final QueryOptions options);

    /**
     * Query data from InfluxDB IOx into Point structure using FlightSQL.
     * <p>
     * The result stream should be closed after use, you can use try-resource pattern to close it automatically:
     * <pre>
     * try (Stream&lt;PointValues&gt; rows = client.queryPoints("select * from cpu where host=$host",
     *                                                          Map.of("host", "server-a"),
     *                                                          options)) {
     *      rows.forEach(row -&gt; {
     *          // process row
     *      }
     * });
     * </pre>
     *
     * @param query      the query string to execute, cannot be null
     * @param parameters query named parameters
     * @param options    the options for querying data from InfluxDB
     * @return Batches of PointValues returned by the query
     */
    @Nonnull
    Stream<PointValues> queryPoints(@Nonnull final String query,
                                    @Nonnull final Map<String, Object> parameters,
                                    @Nonnull final QueryOptions options);

    /**
     * Query data from InfluxDB IOx using FlightSQL.
     * <p>
     * The result stream should be closed after use, you can use try-resource pattern to close it automatically:
     * <pre>
     * try (Stream&lt;VectorSchemaRoot&gt; batches = client.queryBatches("select * from cpu")) {
     *      batches.forEach(batch -&gt; {
     *          // process batch
     *      }
     * });
     * </pre>
     *
     * @param query the SQL query string to execute, cannot be null
     * @return Batches of rows returned by the query
     */
    @Nonnull
    Stream<VectorSchemaRoot> queryBatches(@Nonnull final String query);

    /**
     * Query data from InfluxDB IOx using FlightSQL.
     * <p>
     * The result stream should be closed after use, you can use try-resource pattern to close it automatically:
     * <pre>
     * try (Stream&lt;VectorSchemaRoot&gt; batches = client.queryBatches("select * from cpu where host=$host",
     *                                                                   Map.of("host", "server-a"))) {
     *      batches.forEach(batch -&gt; {
     *          // process batch
     *      }
     * });
     * </pre>
     *
     * @param query      the SQL query string to execute, cannot be null
     * @param parameters query named parameters
     * @return Batches of rows returned by the query
     */
    @Nonnull
    Stream<VectorSchemaRoot> queryBatches(@Nonnull final String query, @Nonnull final Map<String, Object> parameters);

    /**
     * Query data from InfluxDB IOx using FlightSQL.
     * <pre>
     * try (Stream&lt;VectorSchemaRoot&gt; batches = client.queryBatches("select * from cpu", options)) {
     *      batches.forEach(batch -&gt; {
     *          // process batch
     *      }
     * });
     * </pre>
     *
     * @param query   the query string to execute, cannot be null
     * @param options the options for querying data from InfluxDB
     * @return Batches of rows returned by the query
     */
    @Nonnull
    Stream<VectorSchemaRoot> queryBatches(@Nonnull final String query, @Nonnull final QueryOptions options);

    /**
     * Query data from InfluxDB IOx using FlightSQL.
     * <pre>
     * try (Stream&lt;VectorSchemaRoot&gt; batches = client.queryBatches("select * from cpu where host=$host",
     *                                                                   Map.of("host", "server-a"),
     *                                                                   options)) {
     *      batches.forEach(batch -&gt; {
     *          // process batch
     *      }
     * });
     * </pre>
     *
     * @param query      the query string to execute, cannot be null
     * @param parameters query named parameters
     * @param options    the options for querying data from InfluxDB
     * @return Batches of rows returned by the query
     */
    @Nonnull
    Stream<VectorSchemaRoot> queryBatches(@Nonnull final String query,
                                          @Nonnull final Map<String, Object> parameters,
                                          @Nonnull final QueryOptions options);

    /**
     * Creates a new instance of the {@link InfluxDBClient} for interacting with an InfluxDB server, simplifying
     * common operations such as writing, querying.
     *
     * @param host     the URL of the InfluxDB server
     * @param token    the authentication token for accessing the InfluxDB server, can be null
     * @param database the database to be used for InfluxDB operations, can be null
     * @return new instance of the {@link InfluxDBClient}
     */
    @Nonnull
    static InfluxDBClient getInstance(@Nonnull final String host,
                                      @Nullable final char[] token,
                                      @Nullable final String database) {
        ClientConfig config = new ClientConfig.Builder()
                .host(host)
                .token(token)
                .database(database)
                .build();

        return getInstance(config);
    }

    /**
     * Creates a new instance of the {@link InfluxDBClient} for interacting with an InfluxDB server, simplifying
     * common operations such as writing, querying.
     *
     * @param host        the URL of the InfluxDB server
     * @param token       the authentication token for accessing the InfluxDB server, can be null
     * @param database    the database to be used for InfluxDB operations, can be null
     * @param defaultTags tags to be added by default to writes of points
     * @return new instance of the {@link InfluxDBClient}
     */
    @Nonnull
    static InfluxDBClient getInstance(@Nonnull final String host,
                                      @Nullable final char[] token,
                                      @Nullable final String database,
                                      @Nullable Map<String, String> defaultTags) {

        ClientConfig config = new ClientConfig.Builder()
                .host(host)
                .token(token)
                .database(database)
                .defaultTags(defaultTags)
                .build();

        return getInstance(config);
    }

    /**
     * Creates a new instance of the {@link InfluxDBClient} for interacting with an InfluxDB server, simplifying
     * common operations such as writing, querying.
     * For possible configuration options see {@link ClientConfig}.
     *
     * @param config the configuration for the InfluxDB client
     * @return new instance of the {@link InfluxDBClient}
     */
    @Nonnull
    static InfluxDBClient getInstance(@Nonnull final ClientConfig config) {
        return new InfluxDBClientImpl(config);
    }

    /**
     * Creates a new instance of the {@link InfluxDBClient} from the connection string in URL format.
     * <p>
     * Example:
     * <pre>
     * client = InfluxDBClient.getInstance("https://us-east-1-1.aws.cloud2.influxdata.com/"
     *         + "?token=my-token&amp;database=my-database");
     * </pre>
     * <p>
     * Supported parameters:
     * <ul>
     *   <li>token - authentication token <i>(required)</i></li>
     *   <li>org - organization name</li>
     *   <li>database - database (bucket) name</li>
     *   <li>precision - timestamp precision when writing data</li>
     *   <li>gzipThreshold - payload size size for gzipping data</li>
     * </ul>
     *
     * @param connectionString connection string
     * @return instance of {@link InfluxDBClient}
     */
    @Nonnull
    static InfluxDBClient getInstance(@Nonnull final String connectionString) {
        try {
            return getInstance(new ClientConfig.Builder().build(connectionString));
        } catch (MalformedURLException e) {
            throw new IllegalArgumentException(e); // same exception as ClientConfig.validate()
        }
    }

    /**
     * Creates a new instance of the {@link InfluxDBClient} from environment variables and/or system properties.
     * Environment variables take precedence over system properties.
     * <p>
     * Example:
     * <pre>
     * client = InfluxDBClient.getInstance();
     * </pre>
     * <p>
     * Supported environment variables:
     * <ul>
     *   <li>INFLUX_HOST - cloud/server URL <i>required</i></li>
     *   <li>INFLUX_TOKEN - authentication token <i>required</i></li>
     *   <li>INFLUX_AUTH_SCHEME - authentication scheme</li>
     *   <li>INFLUX_ORG - organization name</li>
     *   <li>INFLUX_DATABASE - database (bucket) name</li>
     *   <li>INFLUX_PRECISION - timestamp precision when writing data</li>
     *   <li>INFLUX_GZIP_THRESHOLD - payload size size for gzipping data</li>
     * </ul>
     * Supported system properties:
     * <ul>
     *   <li>influx.host - cloud/server URL <i>required</i></li>
     *   <li>influx.token - authentication token <i>required</i></li>
     *   <li>influx.authScheme - authentication scheme</li>
     *   <li>influx.org - organization name</li>
     *   <li>influx.database - database (bucket) name</li>
     *   <li>influx.precision - timestamp precision when writing data</li>
     *   <li>influx.gzipThreshold - payload size size for gzipping data</li>
     * </ul>
     *
     * @return instance of {@link InfluxDBClient}
     */
    @Nonnull
    static InfluxDBClient getInstance() {
        return getInstance(new ClientConfig.Builder().build(System.getenv(), System.getProperties()));
    }
}
