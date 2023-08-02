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

import java.time.Duration;
import java.util.Map;

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

        Assertions.assertThat(config.hashCode()).isEqualTo(configBuilder.build().hashCode());
        Assertions.assertThat(config.hashCode()).isNotEqualTo(configBuilder.database("database").build().hashCode());
    }

    @Test
    void toStringConfig() {
        String configString = configBuilder.build().toString();

        Assertions.assertThat(configString.contains("database='my-db'")).isEqualTo(true);
        Assertions.assertThat(configString.contains("gzipThreshold=1000")).isEqualTo(true);
    }
}
