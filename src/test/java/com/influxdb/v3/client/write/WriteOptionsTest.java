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

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.influxdb.v3.client.config.ClientConfig;


class WriteOptionsTest {

    private ClientConfig.Builder configBuilder;

    @BeforeEach
    void before() {
        configBuilder = new ClientConfig.Builder()
                .host("http://localhost:8086")
                .token("my-token".toCharArray());
    }


    @Test
    void optionsEqualAll() {
        WriteOptions options = new WriteOptions("my-database", WritePrecision.S, 512);
        WriteOptions optionsViaBuilder = new WriteOptions.Builder()
                .database("my-database").precision(WritePrecision.S).gzipThreshold(512).build();

        Assertions.assertThat(options).isEqualTo(optionsViaBuilder);
    }

    @Test
    void optionsWithDefaultTags() {
        Map<String, String> defaultTags = Map.of("unit", "U2", "model", "M1");

        WriteOptions options = new WriteOptions("my-database", WritePrecision.S, 512, defaultTags);
        WriteOptions optionsViaBuilder = new WriteOptions.Builder()
          .database("my-database")
          .precision(WritePrecision.S)
          .gzipThreshold(512)
          .defaultTags(defaultTags)
          .build();

        Assertions.assertThat(options).isEqualTo(optionsViaBuilder);

    }

    @Test
    void optionsEmpty() {
        ClientConfig config = configBuilder
                .database("my-database")
                .organization("my-org")
                .writePrecision(WritePrecision.S)
                .gzipThreshold(512)
                .build();

        WriteOptions options = new WriteOptions.Builder().build();

        Assertions.assertThat(options.databaseSafe(config)).isEqualTo("my-database");
        Assertions.assertThat(options.precisionSafe(config)).isEqualTo(WritePrecision.S);
        Assertions.assertThat(options.gzipThresholdSafe(config)).isEqualTo(512);
    }

    @Test
    void optionsOverrideAll() {
        ClientConfig config = configBuilder
                .database("my-database")
                .organization("my-org")
                .writePrecision(WritePrecision.S)
                .gzipThreshold(512)
                .build();

        WriteOptions options = new WriteOptions("your-database", WritePrecision.US, 4096);

        Assertions.assertThat(options.databaseSafe(config)).isEqualTo("your-database");
        Assertions.assertThat(options.precisionSafe(config)).isEqualTo(WritePrecision.US);
        Assertions.assertThat(options.gzipThresholdSafe(config)).isEqualTo(4096);
    }

    @Test
    void optionsOverrideDatabase() {
        ClientConfig config = configBuilder
                .database("my-database")
                .organization("my-org")
                .writePrecision(WritePrecision.S)
                .gzipThreshold(512)
                .build();

        WriteOptions options = new WriteOptions.Builder().database("your-database").build();

        Assertions.assertThat(options.databaseSafe(config)).isEqualTo("your-database");
        Assertions.assertThat(options.precisionSafe(config)).isEqualTo(WritePrecision.S);
        Assertions.assertThat(options.gzipThresholdSafe(config)).isEqualTo(512);
    }

    @Test
    void optionsOverridePrecision() {
        ClientConfig config = configBuilder
                .database("my-database")
                .organization("my-org")
                .writePrecision(WritePrecision.US)
                .gzipThreshold(512)
                .build();

        WriteOptions options = new WriteOptions.Builder().precision(WritePrecision.US).build();

        Assertions.assertThat(options.databaseSafe(config)).isEqualTo("my-database");
        Assertions.assertThat(options.precisionSafe(config)).isEqualTo(WritePrecision.US);
        Assertions.assertThat(options.gzipThresholdSafe(config)).isEqualTo(512);
    }

    @Test
    void optionsOverrideGzipThreshold() {
        ClientConfig config = configBuilder
                .database("my-database")
                .organization("my-org")
                .writePrecision(WritePrecision.S)
                .gzipThreshold(512)
                .build();

        WriteOptions options = new WriteOptions.Builder().gzipThreshold(4096).build();

        Assertions.assertThat(options.databaseSafe(config)).isEqualTo("my-database");
        Assertions.assertThat(options.precisionSafe(config)).isEqualTo(WritePrecision.S);
        Assertions.assertThat(options.gzipThresholdSafe(config)).isEqualTo(4096);
    }

    @Test
    void optionsOverridesDefaultTags() {
        Map<String, String> defaultTagsBase = new HashMap<>() {{
            put("model", "train");
            put("scale", "HO");
        }};

        Map<String, String> defaultTagsNew = new HashMap<>() {{
            put("unit", "D1");
        }};

        ClientConfig config = configBuilder
          .database("my-database")
          .organization("my-org")
          .writePrecision(WritePrecision.S)
          .gzipThreshold(512)
          .defaultTags(defaultTagsBase)
          .build();

        Assertions.assertThat(config.getDefaultTags()).isEqualTo(defaultTagsBase);

        WriteOptions options = new WriteOptions.Builder()
          .defaultTags(defaultTagsNew)
          .build();

        Assertions.assertThat(options.databaseSafe(config)).isEqualTo("my-database");
        Assertions.assertThat(options.precisionSafe(config)).isEqualTo(WritePrecision.S);
        Assertions.assertThat(options.gzipThresholdSafe(config)).isEqualTo(512);
        Assertions.assertThat(options.defaultTagsSafe(config)).isEqualTo(defaultTagsNew);

    }
}
