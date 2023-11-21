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

import com.influxdb.v3.client.write.WritePrecision;

public class PointTest {
    @Test
    void fromValues() throws Exception {
        PointValues pointValues = PointValues.measurement("measurement")
            .setField("field1", 42);
        Point point = Point.fromValues(pointValues);

        Assertions.assertThat("measurement").isEqualTo(point.getMeasurement());

        Assertions.assertThat(42L).isEqualTo(point.getField("field1"));
        point.setMeasurement("newMeasurement");
        Assertions.assertThat("newMeasurement").isEqualTo(point.getMeasurement());
        Assertions.assertThat("newMeasurement").isEqualTo(pointValues.getMeasurement());
    }

    @Test
    void setMeasurement() {
        Point point = Point.measurement("measurement");
        Assertions.assertThat("measurement").isEqualTo(point.getMeasurement());

        point.setMeasurement("newMeasurement");
        Assertions.assertThat("newMeasurement").isEqualTo(point.getMeasurement());
    }

    @Test
    void setTimestamp() {
        Point point = Point.measurement("measurement");

        Instant timestamp = Instant.parse("2023-11-08T12:00:00Z");
        point.setTimestamp(timestamp);
        Assertions.assertThat(BigInteger.valueOf(timestamp.getEpochSecond())
                .multiply(BigInteger.valueOf(1_000_000_000)))
            .isEqualTo(point.getTimestamp());
    }

    @Test
    void setTags() {
        Point point = Point.measurement("measurement");

        Map<String, String> tags = new HashMap<>();
        tags.put("tag1", "value1");
        tags.put("tag2", "value2");

        point.setTags(tags);

        Assertions.assertThat(point.getTag("tag1")).isEqualTo("value1");
        Assertions.assertThat(point.getTag("tag2")).isEqualTo("value2");
    }

    @Test
    void removeTag() {
        Point point = Point.measurement("measurement")
            .setTag("tag1", "value1")
            .setTag("tag2", "value2");

        point.removeTag("tag1");
        point.removeTag("tagNonExistent");

        Assertions.assertThat(point.getTag("tag1")).isNull();
        Assertions.assertThat(point.getTag("tag2")).isEqualTo("value2");
    }

    @Test
    void getTagNames() {
        Point point = Point.measurement("measurement")
            .setTag("tag1", "value1")
            .setTag("tag2", "value2");

        Assertions.assertThat(point.getTagNames()).isEqualTo(new String[]{"tag1", "tag2"});
    }

    @Test
    void setGetTypeField() {
        Point point = Point.measurement("measurement");

        double floatValue = 2.71;
        long integerValue = 64L;
        boolean booleanValue = true;
        String stringValue = "text";

        point.setFloatField("floatField", floatValue);
        point.setIntegerField("integerField", integerValue);
        point.setBooleanField("booleanField", booleanValue);
        point.setStringField("stringField", stringValue);

        Assertions.assertThat(point.getFloatField("floatField")).isEqualTo(floatValue);
        Assertions.assertThat(point.getIntegerField("integerField")).isEqualTo(integerValue);
        Assertions.assertThat(point.getBooleanField("booleanField")).isEqualTo(booleanValue);
        Assertions.assertThat(point.getStringField("stringField")).isEqualTo(stringValue);
    }

    @Test
    void fieldGenerics() {
        Point point = Point.measurement("measurement");

        double floatValue = 2.71;
        long integerValue = 64L;
        boolean booleanValue = true;
        String stringValue = "text";

        point.setField("floatField", floatValue);
        point.setField("integerField", integerValue);
        point.setField("booleanField", booleanValue);
        point.setField("stringField", stringValue);

        Assertions.assertThat(point.getField("floatField", Double.class)).isEqualTo(floatValue);
        Assertions.assertThat(point.getFieldType("floatField")).isEqualTo(Double.class);
        Assertions.assertThat(point.getField("integerField", Long.class)).isEqualTo(integerValue);
        Assertions.assertThat(point.getFieldType("integerField")).isEqualTo(Long.class);
        Assertions.assertThat(point.getField("booleanField", Boolean.class)).isEqualTo(booleanValue);
        Assertions.assertThat(point.getFieldType("booleanField")).isEqualTo(Boolean.class);
        Assertions.assertThat(point.getField("stringField", String.class)).isEqualTo(stringValue);
        Assertions.assertThat(point.getFieldType("stringField")).isEqualTo(String.class);
        Assertions.assertThat(point.getField("Missing", String.class)).isNull();
        Assertions.assertThat(point.getFieldType("Missing")).isNull();
    }

    @Test
    void setFields() {
        Point point = Point.measurement("measurement");

        point.setField("field1", 42);
        point.setField("field2", "value");
        point.setField("field3", 3.14);

        Assertions.assertThat(42L).isEqualTo(point.getField("field1"));
        Assertions.assertThat("value").isEqualTo(point.getField("field2"));
        Assertions.assertThat(3.14).isEqualTo(point.getField("field3"));
    }

    @Test
    void removeField() {
        Point point = Point.measurement("measurement")
            .setField("field1", 42)
            .setField("field2", "value")
            .setField("field3", 3.14);

        point.removeField("field1")
            .removeField("field2");

        Assertions.assertThat(point.getField("field1")).isNull();
        Assertions.assertThat(point.getField("field2")).isNull();
        Assertions.assertThat(3.14).isEqualTo(point.getField("field3"));
    }

    @Test
    void getFieldNames() {
        Point point = Point.measurement("measurement")
            .setField("field", 42)
            .setField("123", "value")
            .setField("some_name", 3.14);

        Assertions.assertThat(point.getFieldNames())
            .isEqualTo(new String[]{"123", "field", "some_name"});
    }

    @Test
    void toLineProtocol() {
        Point point = Point.measurement("measurement")
                .setTag("tag1", "value1")
                .setField("field1", 42);

        String lineProtocol = point.toLineProtocol(WritePrecision.NS);
        Assertions.assertThat("measurement,tag1=value1 field1=42i").isEqualTo(lineProtocol);
    }

    @Test
    void copy() {
        Point point = Point.measurement("measurement")
                .setTag("tag1", "value1")
                .setField("field1", 42);

        Point copy = point.copy();

        // Ensure the copy is not the same object
        Assertions.assertThat(point).isNotSameAs(copy);
        // Ensure the values are equal
        Assertions.assertThat(point.getMeasurement()).isEqualTo(copy.getMeasurement());
        Assertions.assertThat(point.getTag("tag1")).isEqualTo(copy.getTag("tag1"));
        Assertions.assertThat(point.getField("field1")).isEqualTo(copy.getField("field1"));
    }
}
