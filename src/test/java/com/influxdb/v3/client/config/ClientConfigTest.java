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

import com.influxdb.v3.client.write.WritePrecision;

class ClientConfigTest {

    private final ClientConfig.Builder configBuilder = new ClientConfig.Builder()
            .host("http://localhost:9999")
            .token("my-token".toCharArray())
            .organization("my-org")
            .database("my-db")
            .writePrecision(WritePrecision.NS)
            .timeout(Duration.ofSeconds(30))
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
    }

    @Test
    void fromConnectionString() throws MalformedURLException {
        ClientConfig cfg = new ClientConfig.Builder()
                .build("http://localhost:9999/"
                        + "?token=my-token&org=my-org&database=my-db&gzipThreshold=128");
        Assertions.assertThat(cfg.getHost()).isEqualTo("http://localhost:9999/");
        Assertions.assertThat(cfg.getToken()).isEqualTo("my-token".toCharArray());
        Assertions.assertThat(cfg.getOrganization()).isEqualTo("my-org");
        Assertions.assertThat(cfg.getDatabase()).isEqualTo("my-db");
        Assertions.assertThat(cfg.getWritePrecision()).isEqualTo(WritePrecision.NS); // default
        Assertions.assertThat(cfg.getGzipThreshold()).isEqualTo(128);

        cfg = new ClientConfig.Builder()
                .build("http://localhost:9999/"
                        + "?token=my-token&precision=us");
        Assertions.assertThat(cfg.getHost()).isEqualTo("http://localhost:9999/");
        Assertions.assertThat(cfg.getToken()).isEqualTo("my-token".toCharArray());
        Assertions.assertThat(cfg.getOrganization()).isEqualTo(null);
        Assertions.assertThat(cfg.getDatabase()).isEqualTo(null);
        Assertions.assertThat(cfg.getWritePrecision()).isEqualTo(WritePrecision.US);
        Assertions.assertThat(cfg.getGzipThreshold()).isEqualTo(1000); // default

        cfg = new ClientConfig.Builder()
                .build("http://localhost:9999/"
                        + "?token=my-token&precision=ms");
        Assertions.assertThat(cfg.getHost()).isEqualTo("http://localhost:9999/");
        Assertions.assertThat(cfg.getToken()).isEqualTo("my-token".toCharArray());
        Assertions.assertThat(cfg.getOrganization()).isEqualTo(null);
        Assertions.assertThat(cfg.getDatabase()).isEqualTo(null);
        Assertions.assertThat(cfg.getWritePrecision()).isEqualTo(WritePrecision.MS);
        Assertions.assertThat(cfg.getGzipThreshold()).isEqualTo(1000); // default

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
                "INFLUX_GZIP_THRESHOLD", "64"
        );
        cfg = new ClientConfig.Builder()
                .build(env, null);
        Assertions.assertThat(cfg.getHost()).isEqualTo("http://localhost:9999/");
        Assertions.assertThat(cfg.getToken()).isEqualTo("my-token".toCharArray());
        Assertions.assertThat(cfg.getOrganization()).isEqualTo("my-org");
        Assertions.assertThat(cfg.getDatabase()).isEqualTo("my-db");
        Assertions.assertThat(cfg.getWritePrecision()).isEqualTo(WritePrecision.MS);
        Assertions.assertThat(cfg.getGzipThreshold()).isEqualTo(64);
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
        cfg = new ClientConfig.Builder()
                .build(new HashMap<>(), properties);
        Assertions.assertThat(cfg.getHost()).isEqualTo("http://localhost:9999/");
        Assertions.assertThat(cfg.getToken()).isEqualTo("my-token".toCharArray());
        Assertions.assertThat(cfg.getOrganization()).isEqualTo("my-org");
        Assertions.assertThat(cfg.getDatabase()).isEqualTo("my-db");
        Assertions.assertThat(cfg.getWritePrecision()).isEqualTo(WritePrecision.MS);
        Assertions.assertThat(cfg.getGzipThreshold()).isEqualTo(64);
    }
}
