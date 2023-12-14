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

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import com.influxdb.v3.client.config.ClientConfig;
import com.influxdb.v3.client.internal.Arguments;

/**
 * Write options.
 * <p>
 * Supports to specify:
 * <ul>
 *     <li><code>database</code> - specifies the database to be used for InfluxDB operations</li>
 *     <li><code>organization</code> - specifies the organization to be used for InfluxDB operations</li>
 *     <li><code>precision</code> - specifies the precision to use for the timestamp of points</li>
 *     <li><code>defaultTags</code> - specifies tags to be added by default to all write operations using points.</li>
 * </ul>
 */
@ThreadSafe
@SuppressWarnings("ConstantConditions")
public final class WriteOptions {

    /**
     * Default WritePrecision.
     */
    public static final WritePrecision DEFAULT_WRITE_PRECISION = WritePrecision.NS;
    /**
     * Default GZIP threshold.
     */
    public static final Integer DEFAULT_GZIP_THRESHOLD = 1000;
    /**
     * Default WriteOptions.
     */
    public static final WriteOptions DEFAULTS = new WriteOptions(null, DEFAULT_WRITE_PRECISION, DEFAULT_GZIP_THRESHOLD);

    private final String database;
    private final WritePrecision precision;
    private final Integer gzipThreshold;
    private final Map<String, String> defaultTags;

    /**
     * Construct WriteAPI options.
     *
     * @param database     The database to be used for InfluxDB operations.
     * @param precision    The precision to use for the timestamp of points.
     * @param gzipThreshold The threshold for compressing request body.
     */
    public WriteOptions(@Nonnull final String database,
                        @Nonnull final WritePrecision precision,
                        @Nonnull final Integer gzipThreshold) {
        this.database = database;
        this.precision = precision;
        this.gzipThreshold = gzipThreshold;
        this.defaultTags = new HashMap<>();
    }

    /**
     * Construct WriteAPI options.
     *
     * @param database     The database to be used for InfluxDB operations.
     * @param precision    The precision to use for the timestamp of points.
     * @param gzipThreshold The threshold for compressing request body.
     * @param defaultTags   Default tags to be added when writing points.
     */
    public WriteOptions(@Nonnull final String database,
                        @Nonnull final WritePrecision precision,
                        @Nonnull final Integer gzipThreshold,
                        @Nonnull final Map<String, String> defaultTags) {
        this.database = database;
        this.precision = precision;
        this.gzipThreshold = gzipThreshold;
        this.defaultTags = defaultTags;
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
     * @param config with default value
     * @return Precision for unix timestamps in the line protocol of the request payload.
     */
    @Nonnull
    public WritePrecision precisionSafe(@Nonnull final ClientConfig config) {
        Arguments.checkNotNull(config, "config");
        return precision != null ? precision
                : (config.getWritePrecision() != null ? config.getWritePrecision() : DEFAULT_WRITE_PRECISION);
    }

    /**
     * @param config with/without defaultTags defined
     * @return defaultTags - can be an empty map if none are defined.
     */
    @Nonnull
    public Map<String, String> defaultTagsSafe(@Nonnull final ClientConfig config) {
        Arguments.checkNotNull(config, "config");
        return defaultTags.size() > 0 ? defaultTags
          : (config.getDefaultTags() != null ? config.getDefaultTags() : defaultTags);
    }

    /**
     * @param config with default value
     * @return Payload size threshold for compressing it.
     */
    @Nonnull
    public Integer gzipThresholdSafe(@Nonnull final ClientConfig config) {
        Arguments.checkNotNull(config, "config");
        return gzipThreshold != null ? gzipThreshold
                : (config.getWritePrecision() != null ? config.getGzipThreshold() : DEFAULT_GZIP_THRESHOLD);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        WriteOptions that = (WriteOptions) o;
        return Objects.equals(database, that.database)
                && precision == that.precision
                && Objects.equals(gzipThreshold, that.gzipThreshold)
                && defaultTags.equals(that.defaultTags);
    }

    @Override
    public int hashCode() {
        return Objects.hash(database, precision, gzipThreshold, defaultTags);
    }

    private boolean isNotDefined(final String option) {
        return option == null || option.isEmpty();
    }

    /**
     * A builder for {@code WriteOptions}.
     * <p>
     * Mutable.
     */
    public static final class Builder {
        private String database;
        private WritePrecision precision;
        private Integer gzipThreshold;
        private Map<String, String> defaultTags = new HashMap<>();

        /**
         * Sets the database.
         *
         * @param database database
         * @return this
         */
        @Nonnull
        public Builder database(@Nonnull final String database) {

            this.database = database;
            return this;
        }

        /**
         * Sets the precision.
         *
         * @param precision precision
         * @return this
         */
        @Nonnull
        public Builder precision(@Nonnull final WritePrecision precision) {

            this.precision = precision;
            return this;
        }

        /**
         * Sets the GZIp threshold.
         *
         * @param gzipThreshold body size threshold for compression using GZIP
         * @return this
         */
        @Nonnull
        public Builder gzipThreshold(@Nonnull final Integer gzipThreshold) {

            this.gzipThreshold = gzipThreshold;
            return this;
        }

        /**
         * Sets defaultTags
         *
         * @param defaultTags to be used when writing points
         * @return this
         */
        @Nonnull
        public Builder defaultTags(@Nonnull final Map<String, String> defaultTags) {
            this.defaultTags = defaultTags;
            return this;
        }

        /**
         * Build an instance of {@code ClientConfig}.
         *
         * @return the configuration for an {@code InfluxDBClient}.
         */
        @Nonnull
        public WriteOptions build() {
            return new WriteOptions(this);
        }

    }

    private WriteOptions(@Nonnull final Builder builder) {
        this.database = builder.database;
        this.precision = builder.precision;
        this.gzipThreshold = builder.gzipThreshold;
        this.defaultTags = builder.defaultTags;
    }
}