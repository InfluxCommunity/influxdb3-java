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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import com.influxdb.v3.client.config.ClientConfig;
import com.influxdb.v3.client.internal.Arguments;

/**
 * Query API options.
 * <p>
 * Supports to specify:
 * <ul>
 *     <li><code>database</code> - specifies the database to be used for InfluxDB operations</li>
 *     <li><code>queryType</code> - specifies the type of query sent to InfluxDB. Default to 'SQL'.</li>
 * </ul>
 */
@ThreadSafe
@SuppressWarnings("ConstantConditions")
public final class QueryOptions {

    /**
     * Default QueryAPI options.
     */
    public static final QueryOptions DEFAULTS = new QueryOptions(null, QueryType.SQL);
    /**
     * Default QueryAPI options for InfluxQL.
     */
    public static final QueryOptions INFLUX_QL = new QueryOptions(null, QueryType.InfluxQL);

    private final String database;
    private final QueryType queryType;

    /**
     * Construct QueryAPI options.
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
     * Construct QueryAPI options.
     *
     * @param database  The database to be used for InfluxDB operations.
     *                  If it is not specified then use {@link ClientConfig#getDatabase()}.
     * @param queryType The type of query sent to InfluxDB. If it is not specified then use {@link QueryType#SQL}.
     */
    public QueryOptions(@Nullable final String database, @Nullable final QueryType queryType) {
        this.database = database;
        this.queryType = queryType;
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

    private boolean isNotDefined(final String option) {
        return option == null || option.isEmpty();
    }
}
