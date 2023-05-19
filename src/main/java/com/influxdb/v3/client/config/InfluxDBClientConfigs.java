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
package com.influxdb.v3.client.config;

import com.influxdb.v3.client.write.WritePrecision;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.time.Duration;
import java.util.Objects;
import java.util.StringJoiner;

/**
 * Configuration properties for an {@code InfluxDBClient}.
 * <p>
 * Immutable class.
 */
public final class InfluxDBClientConfigs {

    private final String hostUrl;
    private final String authToken;
    private final String organization;
    private final String database;
    private final WritePrecision writePrecision;
    private final Duration responseTimeout;
    private final Boolean allowHttpRedirects;
    private final Boolean disableServerCertificateValidation;

    /**
     * Gets hostname or IP address of the InfluxDB server.
     *
     * @return hostname or IP address of the InfluxDB server
     */
    @Nonnull
    public String getHostUrl() {
        return hostUrl;
    }

    /**
     * Gets authentication token for accessing the InfluxDB server.
     *
     * @return authentication token for accessing the InfluxDB server, may be null
     */
    @Nullable
    public String getAuthToken() {
        return authToken;
    }

    /**
     * Gets organization to be used for operations.
     *
     * @return organization to be used for operations, may be null
     */
    @Nullable
    public String getOrganization() {
        return organization;
    }

    /**
     * Gets database to be used for InfluxDB operations.
     *
     * @return database to be used for InfluxDB operations, may be null
     */
    @Nullable
    public String getDatabase() {
        return database;
    }

    /**
     * Gets the default precision to use for the timestamp of points.
     *
     * @return the default precision to use for the timestamp of points, may be null
     */
    @Nullable
    public WritePrecision getWritePrecision() {
        return writePrecision;
    }

    /**
     * Gets the default response timeout to use for the API calls. Default to '10 seconds'.
     *
     * @return the default response timeout to use for the API calls
     */
    @Nonnull
    public Duration getResponseTimeout() {
        return responseTimeout;
    }

    /**
     * Gets the automatically following HTTP 3xx redirects. Default to 'false'.
     *
     * @return the automatically following HTTP 3xx redirects
     */
    @Nonnull
    public Boolean getAllowHttpRedirects() {
        return allowHttpRedirects;
    }

    /**
     * Gets the disable server SSL certificate validation. Default to 'false'.
     *
     * @return the disable server SSL certificate validation
     */
    public Boolean getDisableServerCertificateValidation() {
        return disableServerCertificateValidation;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        InfluxDBClientConfigs that = (InfluxDBClientConfigs) o;
        return Objects.equals(hostUrl, that.hostUrl)
                && Objects.equals(authToken, that.authToken)
                && Objects.equals(organization, that.organization)
                && Objects.equals(database, that.database)
                && writePrecision == that.writePrecision
                && Objects.equals(responseTimeout, that.responseTimeout)
                && Objects.equals(allowHttpRedirects, that.allowHttpRedirects)
                && Objects.equals(disableServerCertificateValidation, that.disableServerCertificateValidation);
    }

    @Override
    public int hashCode() {
        return Objects.hash(hostUrl, authToken, organization, database, writePrecision, responseTimeout,
                allowHttpRedirects, disableServerCertificateValidation);
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", InfluxDBClientConfigs.class.getSimpleName() + "InfluxDBClientConfigs[", "]")
                .add("hostUrl='" + hostUrl + "'")
                .add("authToken='" + authToken + "'")
                .add("organization='" + organization + "'")
                .add("database='" + database + "'")
                .add("writePrecision=" + writePrecision)
                .add("responseTimeout=" + responseTimeout)
                .add("allowHttpRedirects=" + allowHttpRedirects)
                .add("disableServerCertificateValidation=" + disableServerCertificateValidation)
                .toString();
    }

    /**
     * A builder for {@code InfluxDBClientConfigs}.
     * <p>
     * Mutable.
     */
    public static final class Builder {
        private String hostUrl;
        private String authToken;
        private String organization;
        private String database;
        private WritePrecision writePrecision;
        private Duration responseTimeout;
        private Boolean allowHttpRedirects;
        private Boolean disableServerCertificateValidation;

        /**
         * Sets the hostname or IP address of the InfluxDB server.
         *
         * @param hostUrl hostname or IP address of the InfluxDB server
         * @return this
         */
        @Nonnull
        public Builder hostUrl(@Nonnull final String hostUrl) {

            this.hostUrl = hostUrl;
            return this;
        }

        /**
         * Sets the authentication token for accessing the InfluxDB server.
         *
         * @param authToken authentication token for accessing the InfluxDB server
         * @return this
         */
        @Nonnull
        public Builder authToken(@Nullable final String authToken) {

            this.authToken = authToken;
            return this;
        }

        /**
         * Sets organization to be used for operations.
         *
         * @param organization organization to be used for operations
         * @return this
         */
        @Nonnull
        public Builder organization(@Nullable final String organization) {

            this.organization = organization;
            return this;
        }

        /**
         * Sets database to be used for InfluxDB operations.
         *
         * @param database database to be used for InfluxDB operations
         * @return this
         */
        @Nonnull
        public Builder database(@Nullable final String database) {

            this.database = database;
            return this;
        }

        /**
         * Sets the default precision to use for the timestamp of points
         * if no precision is specified in the write API call.
         *
         * @param writePrecision default precision to use for the timestamp of points
         *                       if no precision is specified in the write API call
         * @return this
         */
        @Nonnull
        public Builder writePrecision(@Nullable final WritePrecision writePrecision) {

            this.writePrecision = writePrecision;
            return this;
        }

        /**
         * Sets the default response timeout to use for the API calls. Default to '10 seconds'.
         *
         * @param responseTimeout default response timeout to use for the API calls. Default to '10 seconds'.
         * @return this
         */
        @Nonnull
        public Builder responseTimeout(@Nullable final Duration responseTimeout) {

            this.responseTimeout = responseTimeout;
            return this;
        }

        /**
         * Sets the automatically following HTTP 3xx redirects. Default to 'false'.
         *
         * @param allowHttpRedirects automatically following HTTP 3xx redirects. Default to 'false'.
         * @return this
         */
        @Nonnull
        public Builder allowHttpRedirects(@Nullable final Boolean allowHttpRedirects) {

            this.allowHttpRedirects = allowHttpRedirects;
            return this;
        }

        /**
         * Sets the disable server SSL certificate validation. Default to 'false'.
         *
         * @param disableServerCertificateValidation Disable server SSL certificate validation. Default to 'false'.
         * @return this
         */
        @Nonnull
        public Builder disableServerCertificateValidation(@Nullable final Boolean disableServerCertificateValidation) {

            this.disableServerCertificateValidation = disableServerCertificateValidation;
            return this;
        }

        /**
         * Build an instance of {@code InfluxDBClientConfigs}.
         *
         * @return the configuration for an {@code InfluxDBClient}.
         */
        @Nonnull
        public InfluxDBClientConfigs build() {
            return new InfluxDBClientConfigs(this);
        }
    }

    private InfluxDBClientConfigs(@Nonnull final Builder builder) {
        hostUrl = builder.hostUrl;
        authToken = builder.authToken;
        organization = builder.organization;
        database = builder.database;
        writePrecision = builder.writePrecision;
        responseTimeout = builder.responseTimeout != null ? builder.responseTimeout : Duration.ofSeconds(10);
        allowHttpRedirects = builder.allowHttpRedirects != null ? builder.allowHttpRedirects : false;
        disableServerCertificateValidation = builder.disableServerCertificateValidation != null
                ? builder.disableServerCertificateValidation : false;
    }
}
