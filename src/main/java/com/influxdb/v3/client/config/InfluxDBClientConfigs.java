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

import java.net.Authenticator;
import java.net.ProxySelector;
import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.StringJoiner;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.influxdb.v3.client.write.WritePrecision;

/**
 * The <code>InfluxDBClientConfigs</code> holds the configurations for the
 * {@link com.influxdb.v3.client.InfluxDBClient} client.
 * <p>
 * You can configure following properties:
 * <ul>
 *     <li><code>hostUrl</code> - hostname or IP address of the InfluxDB server</li>
 *     <li><code>authToken</code> - authentication token for accessing the InfluxDB server</li>
 *     <li><code>organization</code> - organization to be used for operations</li>
 *     <li><code>database</code> - database to be used for InfluxDB operations</li>
 *     <li><code>writePrecision</code> - precision to use when writing points to InfluxDB</li>
 *     <li><code>gzipThreshold</code> - threshold when gzip compression is used for writing points to InfluxDB</li>
 *     <li><code>responseTimeout</code> - timeout when connecting to InfluxDB</li>
 *     <li><code>allowHttpRedirects</code> - allow redirects for InfluxDB connections</li>
 *     <li><code>disableServerCertificateValidation</code> -
 *          disable server certificate validation for HTTPS connections
 *     </li>
 * </ul>
 * <p>
 * If you want to create a client with custom configuration, you can use following code:
 * <pre>
 * InfluxDBClientConfigs configs = new InfluxDBClientConfigs.Builder()
 *     .hostUrl("https://us-east-1-1.aws.cloud2.influxdata.com")
 *     .authToken("my-token".toCharArray())
 *     .database("my-database")
 *     .writePrecision(WritePrecision.S)
 *     .gzipThreshold(4096)
 *     .build();
 *
 * try (InfluxDBClient client = InfluxDBClient.getInstance(configs)) {
 *     //
 *     // your code here
 *     //
 * } catch (Exception e) {
 *     throw new RuntimeException(e);
 * }
 * </pre>
 * Immutable class.
 */
public final class InfluxDBClientConfigs {

    private final String hostUrl;
    private final char[] authToken;
    private final String organization;
    private final String database;
    private final WritePrecision writePrecision;
    private final Integer gzipThreshold;
    private final Duration responseTimeout;
    private final Boolean allowHttpRedirects;
    private final Boolean disableServerCertificateValidation;
    private final ProxySelector proxy;
    private final Authenticator authenticator;
    private final Map<String, String> headers;

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
    public char[] getAuthToken() {
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
     * Gets the threshold for compressing request body using GZIP.
     *
     * @return the threshold in bytes
     */
    @Nonnull
    public Integer getGzipThreshold() {
        return gzipThreshold;
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
    @Nonnull
    public Boolean getDisableServerCertificateValidation() {
        return disableServerCertificateValidation;
    }

    /**
     * Gets the proxy.
     *
     * @return the proxy, may be null
     */
    @Nullable
    public ProxySelector getProxy() {
        return proxy;
    }

    /**
     * Gets the (proxy) authenticator.
     *
     * @return the (proxy) authenticator
     */
    @Nullable
    public Authenticator getAuthenticator() {
        return authenticator;
    }

    /**
     * Gets custom HTTP headers.
     *
     * @return the HTTP headers
     */
    @Nullable
    public Map<String, String> getHeaders() {
        return headers;
    }

    /**
     * Validates the configuration properties.
     */
    public void validate() {
        if (hostUrl == null || hostUrl.isEmpty()) {
            throw new IllegalArgumentException("The hostname or IP address of the InfluxDB server has to be defined.");
        }
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        InfluxDBClientConfigs that = (InfluxDBClientConfigs) o;
        return Objects.equals(hostUrl, that.hostUrl)
                && Arrays.equals(authToken, that.authToken)
                && Objects.equals(organization, that.organization)
                && Objects.equals(database, that.database)
                && writePrecision == that.writePrecision
                && Objects.equals(gzipThreshold, that.gzipThreshold)
                && Objects.equals(responseTimeout, that.responseTimeout)
                && Objects.equals(allowHttpRedirects, that.allowHttpRedirects)
                && Objects.equals(disableServerCertificateValidation, that.disableServerCertificateValidation)
                && Objects.equals(proxy, that.proxy)
                && Objects.equals(authenticator, that.authenticator)
                && Objects.equals(headers, that.headers);
    }

    @Override
    public int hashCode() {
        return Objects.hash(hostUrl, Arrays.hashCode(authToken), organization, database, writePrecision, gzipThreshold,
                responseTimeout, allowHttpRedirects, disableServerCertificateValidation, proxy, authenticator, headers);
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", InfluxDBClientConfigs.class.getSimpleName() + "InfluxDBClientConfigs[", "]")
                .add("hostUrl='" + hostUrl + "'")
                .add("organization='" + organization + "'")
                .add("database='" + database + "'")
                .add("writePrecision=" + writePrecision)
                .add("gzipThreshold=" + gzipThreshold)
                .add("responseTimeout=" + responseTimeout)
                .add("allowHttpRedirects=" + allowHttpRedirects)
                .add("disableServerCertificateValidation=" + disableServerCertificateValidation)
                .add("proxy=" + proxy)
                .add("authenticator=" + authenticator)
                .add("headers=" + headers)
                .toString();
    }

    /**
     * A builder for {@code InfluxDBClientConfigs}.
     * <p>
     * Mutable.
     */
    public static final class Builder {
        private String hostUrl;
        private char[] authToken;
        private String organization;
        private String database;
        private WritePrecision writePrecision;
        private Integer gzipThreshold;
        private Duration responseTimeout;
        private Boolean allowHttpRedirects;
        private Boolean disableServerCertificateValidation;
        private ProxySelector proxy;
        private Authenticator authenticator;
        private Map<String, String> headers;

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
        public Builder authToken(@Nullable final char[] authToken) {

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
         * Sets the threshold for request body to be gzipped.
         *
         * @param gzipThreshold threshold in bytes for request body to be gzipped
         * @return this
         */
        @Nonnull
        public Builder gzipThreshold(@Nullable final Integer gzipThreshold) {

            this.gzipThreshold = gzipThreshold;
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
         * Sets the proxy. Default is 'null'.
         *
         * @param proxy Proxy selector.
         * @return this
         */
        @Nonnull
        public Builder proxy(@Nullable final ProxySelector proxy) {

            this.proxy = proxy;
            return this;
        }

        /**
         * Sets the (proxy) authenticator.
         *
         * @param authenticator Proxy authenticator. Ignored if 'proxy' is null.
         * @return this
         */
        @Nonnull
        public Builder authenticator(@Nullable final Authenticator authenticator) {

            this.authenticator = authenticator;
            return this;
        }

        /**
         * Sets the custom HTTP headers that will be included in requests.
         *
         * @param headers Set of HTTP headers.
         * @return this
         */
        @Nonnull
        public Builder headers(@Nullable final Map<String, String> headers) {

            this.headers = headers;
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

    @SuppressWarnings("MagicNumber")
    private InfluxDBClientConfigs(@Nonnull final Builder builder) {
        hostUrl = builder.hostUrl;
        authToken = builder.authToken;
        organization = builder.organization;
        database = builder.database;
        writePrecision = builder.writePrecision != null ? builder.writePrecision : WritePrecision.NS;
        gzipThreshold = builder.gzipThreshold != null ? builder.gzipThreshold : 1000;
        responseTimeout = builder.responseTimeout != null ? builder.responseTimeout : Duration.ofSeconds(10);
        allowHttpRedirects = builder.allowHttpRedirects != null ? builder.allowHttpRedirects : false;
        disableServerCertificateValidation = builder.disableServerCertificateValidation != null
                ? builder.disableServerCertificateValidation : false;
        proxy = builder.proxy;
        authenticator = builder.authenticator;
        headers = builder.headers;
    }
}
