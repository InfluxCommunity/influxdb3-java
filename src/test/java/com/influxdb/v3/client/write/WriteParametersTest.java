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

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.influxdb.v3.client.config.InfluxDBClientConfigs;

class WriteParametersTest {

    private InfluxDBClientConfigs.Builder optionsBuilder;

    @BeforeEach
    void before() {
        optionsBuilder = new InfluxDBClientConfigs.Builder()
                .hostUrl("http://localhost:8086")
                .authToken("my-token");
    }

    @Test
    void check() {
        new WriteParameters("my-database", "my-org", WritePrecision.NS).check(optionsBuilder.build());

        Object[][] tests = {
                new Object[]{
                        new WriteParameters("my-database", "my-org", WritePrecision.NS), optionsBuilder.build(), null
                },
                new Object[]{
                        new WriteParameters("my-database", null, WritePrecision.NS), optionsBuilder.build(), null
                },
                new Object[]{
                        new WriteParameters("my-database", "", WritePrecision.NS), optionsBuilder.build(), null
                },
                new Object[]{
                        new WriteParameters(null, "my-org", WritePrecision.NS), optionsBuilder.build(),
                        "Expecting a non-empty string for destination database. Please specify the database as a "
                                + "method parameter or use default configuration at 'InfluxDBClientConfigs.Database'."
                },
                new Object[]{
                        new WriteParameters("", "my-org", WritePrecision.NS), optionsBuilder.build(),
                        "Expecting a non-empty string for destination database. Please specify the database as a "
                                + "method parameter or use default configuration at 'InfluxDBClientConfigs.Database'."
                },
                new Object[]{
                        new WriteParameters(null, "my-org", WritePrecision.NS),
                        optionsBuilder.database("my-database").build(), null
                },
        };

        for (Object[] test : tests) {
            WriteParameters parameters = (WriteParameters) test[0];
            InfluxDBClientConfigs options = (InfluxDBClientConfigs) test[1];
            String message = (String) test[2];

            if (message == null) {
                parameters.check(options);
            } else {
                Assertions.assertThatThrownBy(() -> parameters.check(options))
                        .isInstanceOf(IllegalArgumentException.class)
                        .hasMessage(message);
            }
        }
    }

    @Test
    void optionParameters() {
        InfluxDBClientConfigs options = optionsBuilder
                .database("my-database")
                .organization("my-org")
                .writePrecision(WritePrecision.S)
                .build();

        WriteParameters parameters = new WriteParameters(null, null, null);

        Assertions.assertThat(parameters.databaseSafe(options)).isEqualTo("my-database");
        Assertions.assertThat(parameters.organizationSafe(options)).isEqualTo("my-org");
        Assertions.assertThat(parameters.precisionSafe(options)).isEqualTo(WritePrecision.S);
    }

    @Test
    void nullableParameters() {
        InfluxDBClientConfigs options = optionsBuilder.database("my-database").organization("my-org").build();

        WriteParameters parameters = new WriteParameters(null, null, null);

        Assertions.assertThat(parameters.precisionSafe(options)).isEqualTo(WritePrecision.NS);
    }

    @Test
    void npe() {
        WriteParameters parameters = new WriteParameters(null, null, null);

        Assertions.assertThat(parameters.hashCode()).isNotNull();
        Assertions.assertThat(parameters).isEqualTo(parameters);
    }
}
