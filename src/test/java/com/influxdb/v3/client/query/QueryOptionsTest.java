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
package com.influxdb.v3.client.query;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.influxdb.v3.client.config.ClientConfig;

class QueryOptionsTest {

    private ClientConfig.Builder configBuilder;

    @BeforeEach
    void before() {
        configBuilder = new ClientConfig.Builder()
                .host("http://localhost:8086")
                .token("my-token".toCharArray());
    }

    @Test
    void optionsOverrideAll() {
        ClientConfig config = configBuilder
                .database("my-database")
                .build();

        QueryOptions options = new QueryOptions("your-database", QueryType.InfluxQL);

        Assertions.assertThat(options.databaseSafe(config)).isEqualTo("your-database");
        Assertions.assertThat(options.queryTypeSafe()).isEqualTo(QueryType.InfluxQL);
    }

    @Test
    void optionsOverrideDatabase() {
        ClientConfig config = configBuilder
                .database("my-database")
                .build();

        QueryOptions options = new QueryOptions("your-database");

        Assertions.assertThat(options.databaseSafe(config)).isEqualTo("your-database");
        Assertions.assertThat(options.queryTypeSafe()).isEqualTo(QueryType.SQL);
    }

    @Test
    void optionsOverrideQueryType() {
        ClientConfig config = configBuilder
                .database("my-database")
                .build();

        QueryOptions options = new QueryOptions(QueryType.InfluxQL);

        Assertions.assertThat(options.databaseSafe(config)).isEqualTo("my-database");
        Assertions.assertThat(options.queryTypeSafe()).isEqualTo(QueryType.InfluxQL);
    }
}
