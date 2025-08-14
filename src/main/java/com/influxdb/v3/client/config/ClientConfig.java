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
import java.net.MalformedURLException;
import java.net.ProxySelector;
import java.net.URL;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.StringJoiner;
import java.util.function.BiFunction;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.influxdb.v3.client.write.WriteOptions;
import com.influxdb.v3.client.write.WritePrecision;

// TODO ensure that if writeTimeout is not defined but timeout is that writeTimeout is initialized to timeout

/**
 * The <code>ClientConfig</code> holds the configurations for the
 * {@link com.influxdb.v3.client.InfluxDBClient} client.
 * <p>
 * You can configure following properties:
 * <ul>
 *     <li><code>host</code> - hostname or IP address of the InfluxDB server</li>
 *     <li><code>token</code> - authentication token for accessing the InfluxDB server</li>
 *     <li><code>authScheme</code> - authentication scheme</li>
 *     <li><code>organization</code> - organization to be used for operations</li>
 *     <li><code>database</code> - database to be used for InfluxDB operations</li>
 *     <li><code>writePrecision</code> - precision to use when writing points to InfluxDB</li>
 *     <li><code>defaultTags</code> - defaultTags added when writing points to InfluxDB</li>
 *     <li><code>gzipThreshold</code> - threshold when gzip compression is used for writing points to InfluxDB</li>
 *     <li><code>writeNoSync</code> - skip waiting for WAL persistence on write</li>
 *     <li><code>timeout</code> - <i>deprecated in 1.4.0</i> timeout when connecting to InfluxDB,
 *     please use more informative properties <code>writeTimeout</code> and <code>queryTimeout</code></li>
 *     <li><code>writeTimeout</code> - timeout when writing data to InfluxDB</li>
 *     <li><code>queryTimeout</code> - timeout used to calculate a default GRPC deadline when querying InfluxDB.
 *     Can be <code>null</code>, in which case queries can potentially run forever.</li>
 *     <li><code>allowHttpRedirects</code> - allow redirects for InfluxDB connections</li>
 *     <li><code>disableServerCertificateValidation</code> -
 *          disable server certificate validation for HTTPS connections
 *     </li>
 *     <li><code>proxyUrl</code> - proxy url for query api and write api</li>
 *     <li><code>authenticator</code> - HTTP proxy authenticator</li>
 *     <li><code>headers</code> - headers to be added to requests</li>
 *     <li><code>sslRootsFilePath</code> - path to the stored certificates file in PEM format</li>
 * </ul>
 * <p>
 * If you want to create a client with custom configuration, you can use following code:
 * <pre>
 * ClientConfig config = new ClientConfig.Builder()
 *     .host("<a href="https://us-east-1-1.aws.cloud2.influxdata.com">
 *         https://us-east-1-1.aws.cloud2.influxdata.com
 *         </a>")
 *     .token("my-token".toCharArray())
 *     .database("my-database")
 *     .writePrecision(WritePrecision.S)
 *     .gzipThreshold(4096)
 *     .writeNoSync(true)
 *     .proxyUrl("http://localhost:10000")
 *     .build();
 *
 * try (InfluxDBClient client = InfluxDBClient.getInstance(config)) {
 *     //
 *     // your code here
 *     //
 * } catch (Exception e) {
 *     throw new RuntimeException(e);
 * }
 * </pre>
 * Immutable class.
 */
public final class ClientConfig {
    private final String host;
    private final char[] token;
    private final String authScheme;
    private final String organization;
    private final String database;
    private final WritePrecision writePrecision;
    private final Integer gzipThreshold;
    private final Boolean writeNoSync;
    private final Map<String, String> defaultTags;
    @Deprecated
    private final Duration timeout;
    private final Duration writeTimeout;
    private final Duration queryTimeout;
    private final Boolean allowHttpRedirects;
    private final Boolean disableServerCertificateValidation;
    private final String proxyUrl;
    private final Authenticator authenticator;
    private final Map<String, String> headers;
    private final String sslRootsFilePath;

    /**
     * Deprecated use {@link #proxyUrl}.
     */
    @Deprecated
    private final ProxySelector proxy;

    /**
     * Gets URL of the InfluxDB server.
     *
     * @return URL of the InfluxDB server
     */
    @Nonnull
    public String getHost() {
        return host;
    }

    /**
     * Gets authentication token for accessing the InfluxDB server.
     *
     * @return authentication token for accessing the InfluxDB server, may be null
     */
    @Nullable
    public char[] getToken() {
        return token;
    }

    /**
     * Gets authentication scheme.
     *
     * @return authentication scheme, may be null
     */
    @Nullable
    public String getAuthScheme() {
        return authScheme;
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
     * If no precision is specified then 'ns' is used.
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
     * Skip waiting for WAL persistence on write?
     *
     * @return skip waiting for WAL persistence on write
     */
    @Nonnull
    public Boolean getWriteNoSync() {
        return writeNoSync;
    }

    /**
     * Gets default tags used when writing points.
     * @return default tags
     */
    public Map<String, String> getDefaultTags() {
        return defaultTags;
    }

    /**
     * Gets the default timeout to use for the API calls. Default to '10 seconds'.
     *
     * @return the default timeout to use for the API calls
     */
    @Nonnull
    public Duration getTimeout() {
        return timeout;
    }

    /**
     * Gets the default timeout to use for REST Write API calls.  Default is
     * {@value com.influxdb.v3.client.write.WriteOptions#DEFAULT_WRITE_TIMEOUT}
     *
     * @return the default timeout to use for REST Write API calls.
     */
    @Nonnull
    public Duration getWriteTimeout() {
        return writeTimeout;
    }

    /**
     * Gets the default timeout in seconds to use for calculating a GRPC Deadline when making Query API calls.
     * Can be null, in which case queries can potentially wait or run forever.
     *
     * @return the default timeout in seconds to use for Query API calls.
     */
    @Nullable
    public Duration getQueryTimeout() {
        return queryTimeout;
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
     * Deprecated use {@link #proxyUrl}
     */
    @Nullable
    @Deprecated
    public ProxySelector getProxy() {
        return proxy;
    }

    /**
     * Gets the proxy url.
     *
     * @return the proxy url, may be null
     */
    @Nullable
    public String getProxyUrl() {
        return proxyUrl;
    }

    /**
     * Gets certificates file path.
     *
     * @return the certificates file path, may be null
     */
    @Nullable
    public String sslRootsFilePath() {
        return sslRootsFilePath;
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
     * Gets custom headers for requests.
     *
     * @return the headers
     */
    @Nullable
    public Map<String, String> getHeaders() {
        return headers;
    }

    /**
     * Validates the configuration properties.
     */
    public void validate() {
        if (host == null || host.isBlank()) {
            throw new IllegalArgumentException("The URL of the InfluxDB server has to be defined.");
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
        ClientConfig that = (ClientConfig) o;
        return Objects.equals(host, that.host)
                && Arrays.equals(token, that.token)
                && Objects.equals(authScheme, that.authScheme)
                && Objects.equals(organization, that.organization)
                && Objects.equals(database, that.database)
                && writePrecision == that.writePrecision
                && Objects.equals(gzipThreshold, that.gzipThreshold)
                && Objects.equals(writeNoSync, that.writeNoSync)
                && Objects.equals(defaultTags, that.defaultTags)
                && Objects.equals(timeout, that.timeout)
                && Objects.equals(writeTimeout, that.writeTimeout)
                && Objects.equals(queryTimeout, that.queryTimeout)
                && Objects.equals(allowHttpRedirects, that.allowHttpRedirects)
                && Objects.equals(disableServerCertificateValidation, that.disableServerCertificateValidation)
                && Objects.equals(proxy, that.proxy)
                && Objects.equals(proxyUrl, that.proxyUrl)
                && Objects.equals(authenticator, that.authenticator)
                && Objects.equals(headers, that.headers)
                && Objects.equals(sslRootsFilePath, that.sslRootsFilePath);
    }

    @Override
    public int hashCode() {
        return Objects.hash(host, Arrays.hashCode(token), authScheme, organization,
                database, writePrecision, gzipThreshold, writeNoSync,
                timeout, writeTimeout, queryTimeout, allowHttpRedirects, disableServerCertificateValidation,
                proxy, proxyUrl, authenticator, headers,
                defaultTags, sslRootsFilePath);
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", ClientConfig.class.getSimpleName() + "ClientConfig[", "]")
                .add("host='" + host + "'")
                .add("organization='" + organization + "'")
                .add("database='" + database + "'")
                .add("writePrecision=" + writePrecision)
                .add("gzipThreshold=" + gzipThreshold)
                .add("writeNoSync=" + writeNoSync)
                .add("timeout=" + timeout)
                .add("writeTimeout=" + writeTimeout)
                .add("queryTimeout=" + queryTimeout)
                .add("allowHttpRedirects=" + allowHttpRedirects)
                .add("disableServerCertificateValidation=" + disableServerCertificateValidation)
                .add("proxy=" + proxy)
                .add("proxyUrl=" + proxyUrl)
                .add("authenticator=" + authenticator)
                .add("headers=" + headers)
                .add("defaultTags=" + defaultTags)
                .add("sslRootsFilePath=" + sslRootsFilePath)
                .toString();
    }

    /**
     * A builder for {@code ClientConfig}.
     * <p>
     * Mutable.
     */
    public static final class Builder {
        private String host;
        private char[] token;
        private String authScheme;
        private String organization;
        private String database;
        private WritePrecision writePrecision;
        private Integer gzipThreshold;
        private Boolean writeNoSync;
        private Map<String, String> defaultTags;
        private Duration timeout;
        private Duration writeTimeout;
        private Duration queryTimeout;
        private Boolean allowHttpRedirects;
        private Boolean disableServerCertificateValidation;
        private ProxySelector proxy;
        private String proxyUrl;
        private Authenticator authenticator;
        private Map<String, String> headers;
        private String sslRootsFilePath;

        /**
         * Sets the URL of the InfluxDB server.
         *
         * @param host URL of the InfluxDB server
         * @return this
         */
        @Nonnull
        public Builder host(@Nonnull final String host) {

            this.host = host;
            return this;
        }

        /**
         * Sets the authentication token for accessing the InfluxDB server.
         *
         * @param token authentication token for accessing the InfluxDB server
         * @return this
         */
        @Nonnull
        public Builder token(@Nullable final char[] token) {

            this.token = token;
            return this;
        }

        /**
         * Sets authentication scheme.
         *
         * @param authScheme authentication scheme.
         *                   Default <code>null</code> for Cloud access, set to 'Bearer' for Edge.
         * @return this
         */
        @Nonnull
        public Builder authScheme(@Nullable final String authScheme) {

            this.authScheme = authScheme;
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
         * Sets whether to skip waiting for WAL persistence on write.
         *
         * @param writeNoSync skip waiting for WAL persistence on write
         * @return this
         */
        @Nonnull
        public Builder writeNoSync(@Nullable final Boolean writeNoSync) {

            this.writeNoSync = writeNoSync;
            return this;
        }

        /**
         * Sets default tags to be written with points.
         *
         * @param defaultTags - tags to be used.
         * @return this
         */
        @Nonnull
        public Builder defaultTags(@Nullable final Map<String, String> defaultTags) {

            this.defaultTags = defaultTags;
            return this;
        }

        /**
         * Sets the default timeout to use for the API calls. Default to '10 seconds'.
         * <p>
         * Note that this parameter is being superseded by clearer writeTimeout.
         *
         * @param timeout default timeout to use for the API calls. Default to '10 seconds'.
         * @return this
         */
        @Deprecated
        @Nonnull
        public Builder timeout(@Nullable final Duration timeout) {

            this.timeout = timeout;
            return this;
        }

        /**
         *  Sets the default writeTimeout to use for Write API calls in the REST client.
         *  Default is {@value com.influxdb.v3.client.write.WriteOptions#DEFAULT_WRITE_TIMEOUT}
         *
         * @param writeTimeout default timeout to use for REST API write calls. Default is
         * {@value com.influxdb.v3.client.write.WriteOptions#DEFAULT_WRITE_TIMEOUT}
         * @return - this
         */
        @Nonnull
        public Builder writeTimeout(@Nullable final Duration writeTimeout) {

          this.writeTimeout = writeTimeout;
          return this;
        }

        /**
         * Sets standard query timeout used to calculate a GRPC deadline when making Query API calls.
         * If <code>null</code>, queries can potentially wait or run forever.
         *
         * @param queryTimeout default timeout used to calculate deadline for Query API calls.
         *                     If <code>null</code>, queries can potentially wait or run forever.
         *                     Default value is <code>null</code>.
         * @return this
         */
        @Nonnull
        public Builder queryTimeout(@Nullable final Duration queryTimeout) {
          this.queryTimeout = queryTimeout;
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
         * Sets the proxy selector. Default is 'null'.
         *
         * @param proxy Proxy selector.
         * @return this
         * Deprecated use {@link #proxyUrl}
         */
        @Nonnull
        public Builder proxy(@Nullable final ProxySelector proxy) {

            this.proxy = proxy;
            return this;
        }

        /**
         * Sets the proxy url. Default is 'null'.
         *
         * @param proxyUrl Proxy url.
         * @return this
         */
        @Nonnull
        public Builder proxyUrl(@Nullable final String proxyUrl) {

            this.proxyUrl = proxyUrl;
            return this;
        }

        /**
         * Sets the proxy authenticator. Default is 'null'.
         *
         * @param authenticator Proxy authenticator. Ignored if 'proxy' is not set.
         * @return this
         */
        @Nonnull
        public Builder authenticator(@Nullable final Authenticator authenticator) {

            this.authenticator = authenticator;
            return this;
        }

        /**
         * Sets the custom headers that will be added to requests. This is useful for adding custom headers to requests,
         * such as tracing headers. To add custom headers use following code:
         * <pre>
         * ClientConfig config = new ClientConfig.Builder()
         *     .host("<a href="https://us-east-1-1.aws.cloud2.influxdata.com">
         *         https://us-east-1-1.aws.cloud2.influxdata.com
         *         </a>")
         *     .token("my-token".toCharArray())
         *     .database("my-database")
         *     .headers(Map.of("X-Tracing-Id", "123"))
         *     .build();
         *
         * try (InfluxDBClient client = InfluxDBClient.getInstance(config)) {
         *     //
         *     // your code here
         *     //
         * } catch (Exception e) {
         *     throw new RuntimeException(e);
         * }
         * </pre>
         *
         * @param headers the headers to be added to requests
         * @return this
         */
        @Nonnull
        public Builder headers(@Nullable final Map<String, String> headers) {

            this.headers = headers;
            return this;
        }

        /**
         * Sets certificate file path. Default is 'null'.
         *
         * @param sslRootsFilePath The certificate file path
         * @return this
         */
        @Nonnull
        public Builder sslRootsFilePath(@Nullable final String sslRootsFilePath) {

            this.sslRootsFilePath = sslRootsFilePath;
            return this;
        }

        /**
         * Build an instance of {@code ClientConfig}.
         *
         * @return the configuration for an {@code InfluxDBClient}.
         */
        @Nonnull
        public ClientConfig build() {
            return new ClientConfig(this);
        }

        /**
         * Build an instance of {@code ClientConfig} from connection string.
         *
         * @param connectionString connection string in URL format
         * @return the configuration for an {@code InfluxDBClient}
         * @throws MalformedURLException when argument is not valid URL
         */
        @Nonnull
        public ClientConfig build(@Nonnull final String connectionString) throws MalformedURLException {
            final URL url = new URL(connectionString);
            final Map<String, String> parameters = new HashMap<>();
            final String[] pairs = url.getQuery().split("&");
            for (String pair : pairs) {
                int idx = pair.indexOf("=");
                parameters.put(pair.substring(0, idx), pair.substring(idx + 1));
            }
            this.host(new URL(url.getProtocol(), url.getHost(), url.getPort(), url.getPath()).toString());
            if (parameters.containsKey("token")) {
                this.token(parameters.get("token").toCharArray());
            }
            if (parameters.containsKey("authScheme")) {
                this.authScheme(parameters.get("authScheme"));
            }
            if (parameters.containsKey("org")) {
                this.organization(parameters.get("org"));
            }
            if (parameters.containsKey("database")) {
                this.database(parameters.get("database"));
            }
            if (parameters.containsKey("precision")) {
                this.writePrecision(parsePrecision(parameters.get("precision")));
            }
            if (parameters.containsKey("gzipThreshold")) {
                this.gzipThreshold(Integer.parseInt(parameters.get("gzipThreshold")));
            }
            if (parameters.containsKey("writeNoSync")) {
                this.writeNoSync(Boolean.parseBoolean(parameters.get("writeNoSync")));
            }

            return new ClientConfig(this);
        }

        /**
         * Build an instance of {@code ClientConfig} from environment variables and/or system properties.
         *
         * @param env        environment variables
         * @param properties system properties
         * @return the configuration for an {@code InfluxDBClient}.
         */
        @Nonnull
        public ClientConfig build(@Nonnull final Map<String, String> env, final Properties properties) {
            final BiFunction<String, String, String> get = (String name, String key) -> {
                String envVar = env.get(name);
                if (envVar != null) {
                    return envVar;
                }
                if (properties != null) {
                    return properties.getProperty(key);
                }
                return null;
            };
            this.host(get.apply("INFLUX_HOST", "influx.host"));
            final String token = get.apply("INFLUX_TOKEN", "influx.token");
            if (token != null) {
                this.token(token.toCharArray());
            }
            final String authScheme = get.apply("INFLUX_AUTH_SCHEME", "influx.authScheme");
            if (authScheme != null) {
                this.authScheme(authScheme);
            }
            final String org = get.apply("INFLUX_ORG", "influx.org");
            if (org != null) {
                this.organization(org);
            }
            final String database = get.apply("INFLUX_DATABASE", "influx.database");
            if (database != null) {
                this.database(database);
            }
            final String precision = get.apply("INFLUX_PRECISION", "influx.precision");
            if (precision != null) {
                this.writePrecision(parsePrecision(precision));
            }
            final String gzipThreshold = get.apply("INFLUX_GZIP_THRESHOLD", "influx.gzipThreshold");
            if (gzipThreshold != null) {
                this.gzipThreshold(Integer.parseInt(gzipThreshold));
            }
            final String writeNoSync = get.apply("INFLUX_WRITE_NO_SYNC", "influx.writeNoSync");
            if (writeNoSync != null) {
                this.writeNoSync(Boolean.parseBoolean(writeNoSync));
            }
            final String writeTimeout = get.apply("INFLUX_WRITE_TIMEOUT", "influx.writeTimeout");
            if (writeTimeout != null) {
                long to = Long.parseLong(writeTimeout);
                this.writeTimeout(Duration.ofSeconds(to));
            }
            final String queryTimeout = get.apply("INFLUX_QUERY_TIMEOUT", "influx.queryTimeout");
            if (queryTimeout != null) {
                long to = Long.parseLong(queryTimeout);
                this.queryTimeout(Duration.ofSeconds(to));
            }

            return new ClientConfig(this);
        }

        private WritePrecision parsePrecision(@Nonnull final String precision) {
            WritePrecision writePrecision;
            switch (precision) {
                case "ns":
                case "nanosecond":
                    writePrecision = WritePrecision.NS;
                    break;
                case "us":
                case "microsecond":
                    writePrecision = WritePrecision.US;
                    break;
                case "ms":
                case "millisecond":
                    writePrecision = WritePrecision.MS;
                    break;
                case "s":
                case "second":
                    writePrecision = WritePrecision.S;
                    break;
                default:
                    throw new IllegalArgumentException(String.format("unsupported precision '%s'", precision));
            }

            return writePrecision;
        }
    }

    @SuppressWarnings("MagicNumber")
    private ClientConfig(@Nonnull final Builder builder) {
        host = builder.host;
        token = builder.token;
        authScheme = builder.authScheme;
        organization = builder.organization;
        database = builder.database;
        writePrecision = builder.writePrecision != null ? builder.writePrecision : WriteOptions.DEFAULT_WRITE_PRECISION;
        gzipThreshold = builder.gzipThreshold != null ? builder.gzipThreshold : WriteOptions.DEFAULT_GZIP_THRESHOLD;
        writeNoSync = builder.writeNoSync != null ? builder.writeNoSync : WriteOptions.DEFAULT_NO_SYNC;
        defaultTags = builder.defaultTags;
        timeout = builder.timeout != null ? builder.timeout : Duration.ofSeconds(WriteOptions.DEFAULT_WRITE_TIMEOUT);
        writeTimeout = builder.writeTimeout != null
            ? builder.writeTimeout : builder.timeout != null
            ? builder.timeout : Duration.ofSeconds(WriteOptions.DEFAULT_WRITE_TIMEOUT);
        queryTimeout = builder.queryTimeout;
        allowHttpRedirects = builder.allowHttpRedirects != null ? builder.allowHttpRedirects : false;
        disableServerCertificateValidation = builder.disableServerCertificateValidation != null
                ? builder.disableServerCertificateValidation : false;
        proxy = builder.proxy;
        proxyUrl = builder.proxyUrl;
        authenticator = builder.authenticator;
        headers = builder.headers;
        sslRootsFilePath = builder.sslRootsFilePath;
    }
}
