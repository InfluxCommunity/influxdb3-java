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

import com.influxdb.v3.client.write.WritePrecision;

/**
 * The <code>ClientConfig</code> holds the configurations for the
 * {@link com.influxdb.v3.client.InfluxDBClient} client.
 * <p>
 * You can configure following properties:
 * <ul>
 *     <li><code>host</code> - hostname or IP address of the InfluxDB server</li>
 *     <li><code>token</code> - authentication token for accessing the InfluxDB server</li>
 *     <li><code>organization</code> - organization to be used for operations</li>
 *     <li><code>database</code> - database to be used for InfluxDB operations</li>
 *     <li><code>writePrecision</code> - precision to use when writing points to InfluxDB</li>
 *     <li><code>defaultTags</code> - defaultTags added when writing points to InfluxDB</li>
 *     <li><code>gzipThreshold</code> - threshold when gzip compression is used for writing points to InfluxDB</li>
 *     <li><code>responseTimeout</code> - timeout when connecting to InfluxDB</li>
 *     <li><code>allowHttpRedirects</code> - allow redirects for InfluxDB connections</li>
 *     <li><code>disableServerCertificateValidation</code> -
 *          disable server certificate validation for HTTPS connections
 *     </li>
 *     <li><code>proxy</code> - HTTP proxy selector</li>
 *     <li><code>authenticator</code> - HTTP proxy authenticator</li>
 *     <li><code>headers</code> - set of HTTP headers to be added to requests</li>
 * </ul>
 * <p>
 * If you want to create a client with custom configuration, you can use following code:
 * <pre>
 * ClientConfig config = new Config.Builder()
 *     .host("https://us-east-1-1.aws.cloud2.influxdata.com")
 *     .token("my-token".toCharArray())
 *     .database("my-database")
 *     .writePrecision(WritePrecision.S)
 *     .gzipThreshold(4096)
 *     .proxy(ProxySelector.of(new InetSocketAddress("http://proxy.local", 8888)))
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
    private final String organization;
    private final String database;
    private final WritePrecision writePrecision;
    private final Integer gzipThreshold;
    private final Map<String, String> defaultTags;
    private final Duration timeout;
    private final Boolean allowHttpRedirects;
    private final Boolean disableServerCertificateValidation;
    private final ProxySelector proxy;
    private final Authenticator authenticator;
    private final Map<String, String> headers;

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
                && Objects.equals(organization, that.organization)
                && Objects.equals(database, that.database)
                && writePrecision == that.writePrecision
                && Objects.equals(gzipThreshold, that.gzipThreshold)
                && Objects.equals(defaultTags, that.defaultTags)
                && Objects.equals(timeout, that.timeout)
                && Objects.equals(allowHttpRedirects, that.allowHttpRedirects)
                && Objects.equals(disableServerCertificateValidation, that.disableServerCertificateValidation)
                && Objects.equals(proxy, that.proxy)
                && Objects.equals(authenticator, that.authenticator)
                && Objects.equals(headers, that.headers);
    }

    @Override
    public int hashCode() {
        return Objects.hash(host, Arrays.hashCode(token), organization,
          database, writePrecision, gzipThreshold,
          timeout, allowHttpRedirects, disableServerCertificateValidation,
          proxy, authenticator, headers,
          defaultTags);
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", ClientConfig.class.getSimpleName() + "ClientConfig[", "]")
                .add("host='" + host + "'")
                .add("organization='" + organization + "'")
                .add("database='" + database + "'")
                .add("writePrecision=" + writePrecision)
                .add("gzipThreshold=" + gzipThreshold)
                .add("timeout=" + timeout)
                .add("allowHttpRedirects=" + allowHttpRedirects)
                .add("disableServerCertificateValidation=" + disableServerCertificateValidation)
                .add("proxy=" + proxy)
                .add("authenticator=" + authenticator)
                .add("headers=" + headers)
                .add("defaultTags=" + defaultTags)
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
        private String organization;
        private String database;
        private WritePrecision writePrecision;
        private Integer gzipThreshold;
        private Map<String, String> defaultTags;
        private Duration timeout;
        private Boolean allowHttpRedirects;
        private Boolean disableServerCertificateValidation;
        private ProxySelector proxy;
        private Authenticator authenticator;
        private Map<String, String> headers;

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
         *
         * @param timeout default timeout to use for the API calls. Default to '10 seconds'.
         * @return this
         */
        @Nonnull
        public Builder timeout(@Nullable final Duration timeout) {

            this.timeout = timeout;
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
         */
        @Nonnull
        public Builder proxy(@Nullable final ProxySelector proxy) {

            this.proxy = proxy;
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

            return new ClientConfig(this);
        }

        /**
         * Build an instance of {@code ClientConfig} from environment variables and/or system properties.
         *
         * @param env environment variables
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

            return new ClientConfig(this);
        }

        private WritePrecision parsePrecision(@Nonnull final String precision) {
            WritePrecision writePrecision;
            switch (precision) {
                case "ns":
                    writePrecision = WritePrecision.NS;
                    break;
                case "us":
                    writePrecision = WritePrecision.US;
                    break;
                case "ms":
                    writePrecision = WritePrecision.MS;
                    break;
                case "s":
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
        organization = builder.organization;
        database = builder.database;
        writePrecision = builder.writePrecision != null ? builder.writePrecision : WritePrecision.NS;
        gzipThreshold = builder.gzipThreshold != null ? builder.gzipThreshold : 1000;
        defaultTags = builder.defaultTags;
        timeout = builder.timeout != null ? builder.timeout : Duration.ofSeconds(10);
        allowHttpRedirects = builder.allowHttpRedirects != null ? builder.allowHttpRedirects : false;
        disableServerCertificateValidation = builder.disableServerCertificateValidation != null
                ? builder.disableServerCertificateValidation : false;
        proxy = builder.proxy;
        authenticator = builder.authenticator;
        headers = builder.headers;
    }
}
