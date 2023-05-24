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
package com.influxdb.v3.client.write;

import java.util.Objects;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import com.influxdb.v3.client.config.InfluxDBClientConfigs;
import com.influxdb.v3.client.internal.Arguments;

/**
 * Write API parameters.
 * <p>
 * Supports to specify:
 * <ul>
 *     <li><code>database</code> - specifies the database to be used for InfluxDB operations</li>
 *     <li><code>organization</code> - specifies the organization to be used for InfluxDB operations</li>
 *     <li><code>precision</code> - specified the precision to use for the timestamp of points</li>
 * </ul>
 */
@ThreadSafe
@SuppressWarnings("ConstantConditions")
public final class WriteParameters {

    public static final WritePrecision DEFAULT_WRITE_PRECISION = WritePrecision.NS;
    public static final WriteParameters DEFAULTS = new WriteParameters(null, null, null);

    private final String database;
    private final String organization;
    private final WritePrecision precision;

    /**
     * Construct WriteAPI parameters.
     *
     * @param database     The database to be used for InfluxDB operations.
     *                     If it is not specified then use {@link InfluxDBClientConfigs#getDatabase()}.
     * @param organization The organization to be used for InfluxDB operations.
     *                     If it is not specified then use {@link InfluxDBClientConfigs#getOrganization()}.
     * @param precision    The precision to use for the timestamp of points.
     *                     If it is not specified then use {@link WritePrecision#NS}.
     */
    public WriteParameters(@Nullable final String database,
                           @Nullable final String organization,
                           @Nullable final WritePrecision precision) {
        this.database = database;
        this.organization = organization;
        this.precision = precision;
    }

    /**
     * @param configs with default value
     * @return The destination organization for writes.
     */
    @Nullable
    public String organizationSafe(@Nonnull final InfluxDBClientConfigs configs) {
        Arguments.checkNotNull(configs, "configs");
        return isNotDefined(organization) ? configs.getOrganization() : organization;
    }

    /**
     * @param configs with default value
     * @return The destination database for writes.
     */
    @Nullable
    public String databaseSafe(@Nonnull final InfluxDBClientConfigs configs) {
        Arguments.checkNotNull(configs, "configs");
        return isNotDefined(database) ? configs.getDatabase() : database;
    }

    /**
     * @param configs with default value
     * @return Precision for unix timestamps in the line protocol of the request payload.
     */
    @Nonnull
    public WritePrecision precisionSafe(@Nonnull final InfluxDBClientConfigs configs) {
        Arguments.checkNotNull(configs, "configs");
        return precision != null ? precision
                : (configs.getWritePrecision() != null ? configs.getWritePrecision() : DEFAULT_WRITE_PRECISION);
    }

    /**
     * Copy current parameters with new precision.
     *
     * @param precision new precision
     * @param configs   default values
     * @return copied parameters
     */
    @Nonnull
    public WriteParameters copy(@Nonnull final WritePrecision precision,
                                @Nonnull final InfluxDBClientConfigs configs) {

        Arguments.checkNotNull(precision, "precision");
        Arguments.checkNotNull(configs, "configs");

        return new WriteParameters(databaseSafe(configs), organizationSafe(configs), precision);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        WriteParameters that = (WriteParameters) o;
        return Objects.equals(database, that.database) && Objects.equals(organization, that.organization)
                && precision == that.precision;
    }

    @Override
    public int hashCode() {
        return Objects.hash(database, organization, precision);
    }

    private boolean isNotDefined(final String option) {
        return option == null || option.isEmpty();
    }
}
