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
package com.influxdb.v3.client.query;

import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import com.influxdb.v3.client.config.ClientConfig;
import com.influxdb.v3.client.internal.Arguments;
import com.influxdb.v3.client.internal.GrpcCallOptions;

/**
 * Query API options.
 * <p>
 * Supports to specify:
 * <ul>
 *     <li><code>database</code> - specifies the database to be used for InfluxDB operations</li>
 *     <li><code>queryType</code> - specifies the type of query sent to InfluxDB. Default to 'SQL'.</li>
 *     <li><code>headers</code> - specifies the headers to be added to query request</li>
 * </ul>
 * <p>
 * To add custom headers to the query request, use the following code:
 * <pre>
 * QueryOptions options = new QueryOptions(Map.of("X-Tracing-Id", "123"));
 * Stream&lt;Object[]&gt; rows = client.query("select * from cpu", queryOptions);
 * </pre>
 */
@ThreadSafe
@SuppressWarnings("ConstantConditions")
public final class QueryOptions {

    /**
     * Default QueryAPI options.<br>
     * Deprecated: use {@link #defaultQueryOptions} instead.
     */
    @Deprecated(forRemoval = true)
    public static final QueryOptions DEFAULTS = new QueryOptions(null, QueryType.SQL);

    /**
     * Default QueryAPI options for InfluxQL.<br>
     * Deprecated: use {@link #defaultInfluxQlQueryOptions} instead.
     */
    @Deprecated(forRemoval = true)
    public static final QueryOptions INFLUX_QL = new QueryOptions(null, QueryType.InfluxQL);

    private final String database;
    private final QueryType queryType;
    private final Map<String, String> headers;
    private GrpcCallOptions grpcCallOptions = GrpcCallOptions.getDefaultOptions();

    /**
     * Provides default query options with no database specified and using SQL as the query type.
     *
     * @return A {@code QueryOptions} instance with default settings, including a null database
     *         and {@code QueryType.SQL} as the query type.
     */
    public static QueryOptions defaultQueryOptions() {
        return new QueryOptions(null, QueryType.SQL);
    }

    /**
     * Provides default query options for executing InfluxQL queries with no database specified.
     *
     * @return A {@code QueryOptions} instance configured with a null database and {@code QueryType.InfluxQL}.
     */
    public static QueryOptions defaultInfluxQlQueryOptions() {
        return new QueryOptions(null, QueryType.InfluxQL);
    }

    /**
     * Construct QueryAPI options. The query type is set to SQL.
     *
     * @param database The database to be used for InfluxDB operations.
     */
    public QueryOptions(@Nonnull final String database) {
        this(database, QueryType.SQL);
    }

    /**
     * Construct QueryAPI options.
     *
     * @param queryType The type of query sent to InfluxDB.
     */
    public QueryOptions(@Nonnull final QueryType queryType) {
        this(null, queryType);
    }

    /**
     * Construct QueryAPI options. The query type is set to SQL.
     *
     * @param headers The headers to be added to query request.
     *                The headers specified here are preferred over the headers specified in the client configuration.
     */
    public QueryOptions(@Nullable final Map<String, String> headers) {
        this(null, QueryType.SQL, headers);
    }

    /**
     * Construct QueryAPI options.
     *
     * @param database  The database to be used for InfluxDB operations.
     *                  If it is not specified then use {@link ClientConfig#getDatabase()}.
     * @param queryType The type of query sent to InfluxDB. If it is not specified then use {@link QueryType#SQL}.
     */
    public QueryOptions(@Nullable final String database, @Nullable final QueryType queryType) {
        this(database, queryType, null);
    }

    /**
     * Construct QueryAPI options.
     *
     * @param database  The database to be used for InfluxDB operations.
     *                  If it is not specified then use {@link ClientConfig#getDatabase()}.
     * @param queryType The type of query sent to InfluxDB. If it is not specified then use {@link QueryType#SQL}.
     * @param headers   The headers to be added to query request.
     *                  The headers specified here are preferred over the headers specified in the client configuration.
     */
    public QueryOptions(@Nullable final String database,
                        @Nullable final QueryType queryType,
                        @Nullable final Map<String, String> headers) {
        this.database = database;
        this.queryType = queryType;
        this.headers = headers == null ? Map.of() : headers;
    }

    /**
     * @param config with default value
     * @return The destination database for writes.
     */
    @Nullable
    public String databaseSafe(@Nonnull final ClientConfig config) {
        Arguments.checkNotNull(config, "config");
        return isNotDefined(database) ? config.getDatabase() : database;
    }

    /**
     * @return The type of query sent to InfluxDB, cannot be null.
     */
    @Nonnull
    public QueryType queryTypeSafe() {
        return queryType == null ? QueryType.SQL : queryType;
    }

    /**
     * @return The headers to be added to query request, cannot be null.
     */
    @Nonnull
    public Map<String, String> headersSafe() {
        return headers;
    }

    /**
     * Sets the GrpcCallOptions object.
     * @param grpcCallOptions the grpcCallOptions
     */
    public void setGrpcCallOptions(@Nonnull final GrpcCallOptions grpcCallOptions) {
        Arguments.checkNotNull(grpcCallOptions, "grpcCallOptions");
        this.grpcCallOptions = grpcCallOptions;
    }

    /**
     * @return the GrpcCallOptions object.
     */
    @Nonnull
    public GrpcCallOptions grpcCallOptions() {
        return grpcCallOptions;
    }

    private boolean isNotDefined(final String option) {
        return option == null || option.isEmpty();
    }
}
