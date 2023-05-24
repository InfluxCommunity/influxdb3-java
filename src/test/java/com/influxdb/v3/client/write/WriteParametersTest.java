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
        Assertions.assertThat(parameters).isEqualTo(new WriteParameters(null, null, null));
    }
}
