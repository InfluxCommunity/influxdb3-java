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
package com.influxdb.v3.client;

import java.math.BigInteger;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class PointValuesTest {
    @Test
    void setMeasurement() {
        PointValues pointValues = PointValues.measurement("measurement");
        Assertions.assertThat("measurement").isEqualTo(pointValues.getMeasurement());

        pointValues.setMeasurement("newMeasurement");
        Assertions.assertThat("newMeasurement").isEqualTo(pointValues.getMeasurement());
    }

    @Test
    void setTimestamp() {
        PointValues pointValues = PointValues.measurement("measurement");

        Instant timestamp = Instant.parse("2023-11-08T12:00:00Z");
        pointValues.setTimestamp(timestamp);
        Assertions.assertThat(BigInteger.valueOf(timestamp.getEpochSecond())
                .multiply(BigInteger.valueOf(1_000_000_000)))
            .isEqualTo(pointValues.getTimestamp());
    }

    @Test
    void setTags() {
        PointValues pointValues = PointValues.measurement("measurement");

        Map<String, String> tags = new HashMap<>();
        tags.put("tag1", "value1");
        tags.put("tag2", "value2");

        pointValues.setTags(tags);

        Assertions.assertThat("value1").isEqualTo(pointValues.getTag("tag1"));
        Assertions.assertThat("value2").isEqualTo(pointValues.getTag("tag2"));
    }

    @Test
    void setFields() {
        PointValues pointValues = PointValues.measurement("measurement");

        pointValues.setField("field1", 42);
        pointValues.setField("field2", "value");
        pointValues.setField("field3", 3.14);

        Assertions.assertThat(42L).isEqualTo(pointValues.getField("field1"));
        Assertions.assertThat("value").isEqualTo(pointValues.getField("field2"));
        Assertions.assertThat(3.14).isEqualTo(pointValues.getField("field3"));
    }

    @Test
    void copy() {
        PointValues pointValues = PointValues.measurement("measurement")
                .setTag("tag1", "value1")
                .setField("field1", 42);

        PointValues copy = pointValues.copy();

        // Ensure the copy is not the same object
        Assertions.assertThat(pointValues).isNotSameAs(copy);
        // Ensure the values are equal
        Assertions.assertThat(pointValues.getMeasurement()).isEqualTo(copy.getMeasurement());
        Assertions.assertThat(pointValues.getTag("tag1")).isEqualTo(copy.getTag("tag1"));
        Assertions.assertThat(pointValues.getField("field1")).isEqualTo(copy.getField("field1"));
    }
}
