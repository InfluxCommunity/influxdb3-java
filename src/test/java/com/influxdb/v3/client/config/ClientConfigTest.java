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

import java.net.MalformedURLException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import com.influxdb.v3.client.write.WriteOptions;
import com.influxdb.v3.client.write.WritePrecision;

class ClientConfigTest {

    private final ClientConfig.Builder configBuilder = new ClientConfig.Builder()
            .host("http://localhost:9999")
            .token("my-token".toCharArray())
            .organization("my-org")
            .database("my-db")
            .writePrecision(WritePrecision.NS)
            .timeout(Duration.ofSeconds(30))
            .writeTimeout(Duration.ofSeconds(35))
            .queryTimeout(Duration.ofSeconds(120))
            .allowHttpRedirects(true)
            .disableServerCertificateValidation(true)
            .headers(Map.of("X-device", "ab-01"));

    @Test
    void equalConfig() {
        ClientConfig config = configBuilder.build();

        Assertions.assertThat(config).isEqualTo(config);
        Assertions.assertThat(config).isEqualTo(configBuilder.build());
        Assertions.assertThat(config).isNotEqualTo(configBuilder);
        Assertions.assertThat(config).isNotEqualTo(configBuilder.database("database").build());
    }

    @Test
    void hashConfig() {
        ClientConfig config = configBuilder.build();

        Map<String, String> defaultTags = Map.of("unit", "U2", "model", "M1");

        Assertions.assertThat(config.hashCode()).isEqualTo(configBuilder.build().hashCode());
        Assertions.assertThat(config.hashCode())
                .isNotEqualTo(configBuilder.database("database").build().hashCode());
        Assertions.assertThat(config.hashCode())
                .isNotEqualTo(configBuilder.defaultTags(defaultTags).build().hashCode());
    }

    @Test
    void toStringConfig() {
        String configString = configBuilder.build().toString();

        Assertions.assertThat(configString.contains("database='my-db'")).isEqualTo(true);
        Assertions.assertThat(configString.contains("gzipThreshold=1000")).isEqualTo(true);
        Assertions.assertThat(configString).contains("writeNoSync=false");
        Assertions.assertThat(configString).contains("timeout=PT30S");
        Assertions.assertThat(configString).contains("writeTimeout=PT35S");
        Assertions.assertThat(configString).contains("queryTimeout=PT2M");

    }

    @Test
    void fromConnectionString() throws MalformedURLException {
        ClientConfig cfg = new ClientConfig.Builder()
                .build("http://localhost:9999/"
                        + "?token=my-token&org=my-org&database=my-db&gzipThreshold=128&writeNoSync=true");
        Assertions.assertThat(cfg.getHost()).isEqualTo("http://localhost:9999/");
        Assertions.assertThat(cfg.getToken()).isEqualTo("my-token".toCharArray());
        Assertions.assertThat(cfg.getOrganization()).isEqualTo("my-org");
        Assertions.assertThat(cfg.getDatabase()).isEqualTo("my-db");
        Assertions.assertThat(cfg.getWritePrecision()).isEqualTo(WritePrecision.NS); // default
        Assertions.assertThat(cfg.getGzipThreshold()).isEqualTo(128);
        Assertions.assertThat(cfg.getWriteNoSync()).isEqualTo(true);

        cfg = new ClientConfig.Builder()
                .build("http://localhost:9999/"
                        + "?token=my-token&precision=us");
        Assertions.assertThat(cfg.getHost()).isEqualTo("http://localhost:9999/");
        Assertions.assertThat(cfg.getToken()).isEqualTo("my-token".toCharArray());
        Assertions.assertThat(cfg.getOrganization()).isEqualTo(null);
        Assertions.assertThat(cfg.getDatabase()).isEqualTo(null);
        Assertions.assertThat(cfg.getWritePrecision()).isEqualTo(WritePrecision.US);
        Assertions.assertThat(cfg.getGzipThreshold()).isEqualTo(1000); // default
        Assertions.assertThat(cfg.getWriteNoSync()).isEqualTo(WriteOptions.DEFAULT_NO_SYNC);

        cfg = new ClientConfig.Builder()
                .build("http://localhost:9999/"
                        + "?token=my-token&precision=ms");
        Assertions.assertThat(cfg.getHost()).isEqualTo("http://localhost:9999/");
        Assertions.assertThat(cfg.getToken()).isEqualTo("my-token".toCharArray());
        Assertions.assertThat(cfg.getOrganization()).isEqualTo(null);
        Assertions.assertThat(cfg.getDatabase()).isEqualTo(null);
        Assertions.assertThat(cfg.getWritePrecision()).isEqualTo(WritePrecision.MS);
        Assertions.assertThat(cfg.getGzipThreshold()).isEqualTo(1000); // default
        Assertions.assertThat(cfg.getWriteNoSync()).isEqualTo(WriteOptions.DEFAULT_NO_SYNC);

        cfg = new ClientConfig.Builder()
                .build("http://localhost:9999/"
                        + "?token=my-token&precision=s");
        Assertions.assertThat(cfg.getHost()).isEqualTo("http://localhost:9999/");
        Assertions.assertThat(cfg.getToken()).isEqualTo("my-token".toCharArray());
        Assertions.assertThat(cfg.getOrganization()).isEqualTo(null);
        Assertions.assertThat(cfg.getDatabase()).isEqualTo(null);
        Assertions.assertThat(cfg.getWritePrecision()).isEqualTo(WritePrecision.S);
        Assertions.assertThat(cfg.getGzipThreshold()).isEqualTo(1000); // default

        cfg = new ClientConfig.Builder()
                .build("http://localhost:9999/"
                        + "?token=my-token&authScheme=my-auth");
        Assertions.assertThat(cfg.getHost()).isEqualTo("http://localhost:9999/");
        Assertions.assertThat(cfg.getToken()).isEqualTo("my-token".toCharArray());
        Assertions.assertThat(cfg.getAuthScheme()).isEqualTo("my-auth");
    }

    @Test
    void fromConnectionStringLongPrecision() throws MalformedURLException {
        ClientConfig cfg;
        cfg = new ClientConfig.Builder().build("http://localhost:9999/?token=x&precision=nanosecond");
        Assertions.assertThat(cfg.getWritePrecision()).isEqualTo(WritePrecision.NS);
        cfg = new ClientConfig.Builder().build("http://localhost:9999/?token=x&precision=microsecond");
        Assertions.assertThat(cfg.getWritePrecision()).isEqualTo(WritePrecision.US);
        cfg = new ClientConfig.Builder().build("http://localhost:9999/?token=x&precision=millisecond");
        Assertions.assertThat(cfg.getWritePrecision()).isEqualTo(WritePrecision.MS);
        cfg = new ClientConfig.Builder().build("http://localhost:9999/?token=x&precision=second");
        Assertions.assertThat(cfg.getWritePrecision()).isEqualTo(WritePrecision.S);
    }

    @Test
    void fromEnv() {
        // minimal
        Map<String, String> env = Map.of(
                "INFLUX_HOST", "http://localhost:9999/",
                "INFLUX_TOKEN", "my-token"
        );
        ClientConfig cfg = new ClientConfig.Builder()
                .build(env, null);
        Assertions.assertThat(cfg.getHost()).isEqualTo("http://localhost:9999/");
        Assertions.assertThat(cfg.getToken()).isEqualTo("my-token".toCharArray());
        Assertions.assertThat(cfg.getOrganization()).isEqualTo(null);
        Assertions.assertThat(cfg.getDatabase()).isEqualTo(null);
        // these are defaults
        Assertions.assertThat(cfg.getWritePrecision()).isEqualTo(WritePrecision.NS);
        Assertions.assertThat(cfg.getGzipThreshold()).isEqualTo(1000);

        // basic
        env = Map.of(
                "INFLUX_HOST", "http://localhost:9999/",
                "INFLUX_TOKEN", "my-token",
                "INFLUX_ORG", "my-org",
                "INFLUX_DATABASE", "my-db"
        );
        cfg = new ClientConfig.Builder()
                .build(env, null);
        Assertions.assertThat(cfg.getHost()).isEqualTo("http://localhost:9999/");
        Assertions.assertThat(cfg.getToken()).isEqualTo("my-token".toCharArray());
        Assertions.assertThat(cfg.getOrganization()).isEqualTo("my-org");
        Assertions.assertThat(cfg.getDatabase()).isEqualTo("my-db");
        // these are defaults
        Assertions.assertThat(cfg.getWritePrecision()).isEqualTo(WritePrecision.NS);
        Assertions.assertThat(cfg.getGzipThreshold()).isEqualTo(1000);

        // with Edge authentication
        env = Map.of(
                "INFLUX_HOST", "http://localhost:9999/",
                "INFLUX_TOKEN", "my-token",
                "INFLUX_AUTH_SCHEME", "my-auth"
        );
        cfg = new ClientConfig.Builder()
                .build(env, null);
        Assertions.assertThat(cfg.getHost()).isEqualTo("http://localhost:9999/");
        Assertions.assertThat(cfg.getToken()).isEqualTo("my-token".toCharArray());
        Assertions.assertThat(cfg.getAuthScheme()).isEqualTo("my-auth");

        // with write options
        env = Map.of(
                "INFLUX_HOST", "http://localhost:9999/",
                "INFLUX_TOKEN", "my-token",
                "INFLUX_ORG", "my-org",
                "INFLUX_DATABASE", "my-db",
                "INFLUX_PRECISION", "ms",
                "INFLUX_GZIP_THRESHOLD", "64",
                "INFLUX_WRITE_NO_SYNC", "true"
        );
        cfg = new ClientConfig.Builder()
                .build(env, null);
        Assertions.assertThat(cfg.getHost()).isEqualTo("http://localhost:9999/");
        Assertions.assertThat(cfg.getToken()).isEqualTo("my-token".toCharArray());
        Assertions.assertThat(cfg.getOrganization()).isEqualTo("my-org");
        Assertions.assertThat(cfg.getDatabase()).isEqualTo("my-db");
        Assertions.assertThat(cfg.getWritePrecision()).isEqualTo(WritePrecision.MS);
        Assertions.assertThat(cfg.getGzipThreshold()).isEqualTo(64);
        Assertions.assertThat(cfg.getWriteNoSync()).isEqualTo(true);
    }

    @Test
    void fromEnvLongPrecision() {
        Map<String, String> baseEnv = Map.of(
                "INFLUX_HOST", "http://localhost:9999/",
                "INFLUX_TOKEN", "my-token"
        );
        Map<String, String> env;
        ClientConfig cfg;

        env = new HashMap<>(baseEnv);
        env.put("INFLUX_PRECISION", "nanosecond");
        cfg = new ClientConfig.Builder().build(env, null);
        Assertions.assertThat(cfg.getWritePrecision()).isEqualTo(WritePrecision.NS);

        env = new HashMap<>(baseEnv);
        env.put("INFLUX_PRECISION", "microsecond");
        cfg = new ClientConfig.Builder().build(env, null);
        Assertions.assertThat(cfg.getWritePrecision()).isEqualTo(WritePrecision.US);

        env = new HashMap<>(baseEnv);
        env.put("INFLUX_PRECISION", "millisecond");
        cfg = new ClientConfig.Builder().build(env, null);
        Assertions.assertThat(cfg.getWritePrecision()).isEqualTo(WritePrecision.MS);

        env = new HashMap<>(baseEnv);
        env.put("INFLUX_PRECISION", "second");
        cfg = new ClientConfig.Builder().build(env, null);
        Assertions.assertThat(cfg.getWritePrecision()).isEqualTo(WritePrecision.S);
    }

    @Test
    void timeoutsFromEnv() {
        Map<String, String> env = Map.of(
            "INFLUX_HOST", "http://localhost:9999/",
            "INFLUX_TOKEN", "my-token",
            "INFLUX_WRITE_TIMEOUT", "45",
            "INFLUX_QUERY_TIMEOUT", "180");

        Duration writeTimeout = Duration.ofSeconds(45);
        Duration queryTimeout = Duration.ofSeconds(180);
        ClientConfig cfg = new ClientConfig.Builder().build(env, null);
        Assertions.assertThat(cfg.getHost()).isEqualTo("http://localhost:9999/");
        Assertions.assertThat(cfg.getToken()).isEqualTo("my-token".toCharArray());
        Assertions.assertThat(cfg.getWriteTimeout()).isEqualTo(writeTimeout);
        Assertions.assertThat(cfg.getQueryTimeout()).isEqualTo(queryTimeout);

    }

    @Test
    void fromSystemProperties() {
        // minimal
        Properties properties = new Properties();
        properties.put("influx.host", "http://localhost:9999/");
        properties.put("influx.token", "my-token");
        ClientConfig cfg = new ClientConfig.Builder()
                .build(new HashMap<>(), properties);
        Assertions.assertThat(cfg.getHost()).isEqualTo("http://localhost:9999/");
        Assertions.assertThat(cfg.getToken()).isEqualTo("my-token".toCharArray());
        Assertions.assertThat(cfg.getOrganization()).isEqualTo(null);
        Assertions.assertThat(cfg.getDatabase()).isEqualTo(null);
        // these are defaults
        Assertions.assertThat(cfg.getWritePrecision()).isEqualTo(WritePrecision.NS);
        Assertions.assertThat(cfg.getGzipThreshold()).isEqualTo(1000);
        Assertions.assertThat(cfg.getWriteNoSync()).isEqualTo(WriteOptions.DEFAULT_NO_SYNC);

        // basic
        properties = new Properties();
        properties.put("influx.host", "http://localhost:9999/");
        properties.put("influx.token", "my-token");
        properties.put("influx.org", "my-org");
        properties.put("influx.database", "my-db");
        cfg = new ClientConfig.Builder()
                .build(new HashMap<>(), properties);
        Assertions.assertThat(cfg.getHost()).isEqualTo("http://localhost:9999/");
        Assertions.assertThat(cfg.getToken()).isEqualTo("my-token".toCharArray());
        Assertions.assertThat(cfg.getOrganization()).isEqualTo("my-org");
        Assertions.assertThat(cfg.getDatabase()).isEqualTo("my-db");
        // these are defaults
        Assertions.assertThat(cfg.getWritePrecision()).isEqualTo(WritePrecision.NS);
        Assertions.assertThat(cfg.getGzipThreshold()).isEqualTo(1000);

        // with custom auth scheme
        properties = new Properties();
        properties.put("influx.host", "http://localhost:9999/");
        properties.put("influx.token", "my-token");
        properties.put("influx.authScheme", "my-auth");
        cfg = new ClientConfig.Builder()
                .build(new HashMap<>(), properties);
        Assertions.assertThat(cfg.getHost()).isEqualTo("http://localhost:9999/");
        Assertions.assertThat(cfg.getToken()).isEqualTo("my-token".toCharArray());
        Assertions.assertThat(cfg.getAuthScheme()).isEqualTo("my-auth");

        // with write options
        properties = new Properties();
        properties.put("influx.host", "http://localhost:9999/");
        properties.put("influx.token", "my-token");
        properties.put("influx.org", "my-org");
        properties.put("influx.database", "my-db");
        properties.put("influx.precision", "ms");
        properties.put("influx.gzipThreshold", "64");
        properties.put("influx.writeNoSync", "true");
        cfg = new ClientConfig.Builder()
                .build(new HashMap<>(), properties);
        Assertions.assertThat(cfg.getHost()).isEqualTo("http://localhost:9999/");
        Assertions.assertThat(cfg.getToken()).isEqualTo("my-token".toCharArray());
        Assertions.assertThat(cfg.getOrganization()).isEqualTo("my-org");
        Assertions.assertThat(cfg.getDatabase()).isEqualTo("my-db");
        Assertions.assertThat(cfg.getWritePrecision()).isEqualTo(WritePrecision.MS);
        Assertions.assertThat(cfg.getGzipThreshold()).isEqualTo(64);
        Assertions.assertThat(cfg.getWriteNoSync()).isEqualTo(true);
    }

    @Test
    void fromSystemPropertiesLongPrecision() throws MalformedURLException {
        Properties baseProps = new Properties();
        baseProps.put("influx.host", "http://localhost:9999/");
        baseProps.put("influx.token", "my-token");

        Properties props;
        ClientConfig cfg;

        props = new Properties(baseProps);
        props.put("influx.precision", "nanosecond");
        cfg = new ClientConfig.Builder().build(new HashMap<>(), props);
        Assertions.assertThat(cfg.getWritePrecision()).isEqualTo(WritePrecision.NS);

        props = new Properties(baseProps);
        props.put("influx.precision", "microsecond");
        cfg = new ClientConfig.Builder().build(new HashMap<>(), props);
        Assertions.assertThat(cfg.getWritePrecision()).isEqualTo(WritePrecision.US);

        props = new Properties(baseProps);
        props.put("influx.precision", "millisecond");
        cfg = new ClientConfig.Builder().build(new HashMap<>(), props);
        Assertions.assertThat(cfg.getWritePrecision()).isEqualTo(WritePrecision.MS);

        props = new Properties(baseProps);
        props.put("influx.precision", "second");
        cfg = new ClientConfig.Builder().build(new HashMap<>(), props);
        Assertions.assertThat(cfg.getWritePrecision()).isEqualTo(WritePrecision.S);
    }

    @Test
    void fromSystemPropertiesTimeouts() {
        Properties props = new Properties();
        props.put("influx.host", "http://localhost:9999/");
        props.put("influx.token", "my-token");
        props.put("influx.writeTimeout", "20");
        props.put("influx.queryTimeout", "300");

        ClientConfig cfg = new ClientConfig.Builder().build(new HashMap<>(), props);

        Duration writeTimeout = Duration.ofSeconds(20);
        Duration queryTimeout = Duration.ofSeconds(300);

        Assertions.assertThat(cfg.getHost()).isEqualTo("http://localhost:9999/");
        Assertions.assertThat(cfg.getToken()).isEqualTo("my-token".toCharArray());
        Assertions.assertThat(cfg.getWriteTimeout()).isEqualTo(writeTimeout);
        Assertions.assertThat(cfg.getQueryTimeout()).isEqualTo(queryTimeout);

    }

    @Test
    void timeoutDefaultsTest() {
        ClientConfig cfg = new ClientConfig.Builder()
            .host("http://localhost:9999")
            .token("my-token".toCharArray())
            .organization("my-org")
            .database("my-db")
            .build();

        Duration defaultTimeout = Duration.ofSeconds(WriteOptions.DEFAULT_WRITE_TIMEOUT);
        Assertions.assertThat(cfg.getTimeout()).isEqualTo(defaultTimeout);
        Assertions.assertThat(cfg.getWriteTimeout()).isEqualTo(defaultTimeout);
        Assertions.assertThat(cfg.getQueryTimeout()).isNull();
    }

    @Test
    void standardTimeoutUsedWhenWriteTimeoutUndefinedTest() {
        int testTimeout = 7;
        ClientConfig config = new ClientConfig.Builder()
            .host("http://localhost:8086")
            .token("my-token".toCharArray())
            .timeout(Duration.ofSeconds(testTimeout))
            .build();

        Assertions.assertThat(config.getTimeout()).isEqualTo(Duration.ofSeconds(testTimeout));
        Assertions.assertThat(config.getWriteTimeout()).isEqualTo(Duration.ofSeconds(testTimeout));
    }

}
