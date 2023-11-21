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
    void asPoint() {
        PointValues pointValues = new PointValues()
                .setTag("tag1", "value1")
                .setField("field1", 42);

        Point point = pointValues.asPoint("measurement");

        Assertions.assertThat(pointValues.getMeasurement()).isEqualTo(point.getMeasurement());
        Assertions.assertThat(pointValues.getTag("tag1")).isEqualTo(point.getTag("tag1"));
        Assertions.assertThat(pointValues.getField("field1")).isEqualTo(point.getField("field1"));
    }

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
    void removeTag() {
        PointValues pointValues = PointValues.measurement("measurement")
            .setTag("tag1", "value1")
            .setTag("tag2", "value2");

        pointValues.removeTag("tag1");
        pointValues.removeTag("tagNonExistent");

        Assertions.assertThat(pointValues.getTag("tag1")).isNull();
        Assertions.assertThat(pointValues.getTag("tag2")).isEqualTo("value2");
    }

    @Test
    void getTagNames() {
        PointValues pointValues = PointValues.measurement("measurement")
            .setTag("tag1", "value1")
            .setTag("tag2", "value2");

        Assertions.assertThat(pointValues.getTagNames()).isEqualTo(new String[]{"tag1", "tag2"});
    }

    @Test
    void setGetTypeField() {
        PointValues pointValues = PointValues.measurement("measurement");

        double floatValue = 2.71;
        long integerValue = 64L;
        boolean booleanValue = true;
        String stringValue = "text";

        pointValues.setFloatField("floatField", floatValue);
        pointValues.setIntegerField("integerField", integerValue);
        pointValues.setBooleanField("booleanField", booleanValue);
        pointValues.setStringField("stringField", stringValue);

        Assertions.assertThat(pointValues.getFloatField("floatField")).isEqualTo(floatValue);
        Assertions.assertThat(pointValues.getIntegerField("integerField")).isEqualTo(integerValue);
        Assertions.assertThat(pointValues.getBooleanField("booleanField")).isEqualTo(booleanValue);
        Assertions.assertThat(pointValues.getStringField("stringField")).isEqualTo(stringValue);
    }

    @Test
    void fieldGenerics() {
        PointValues pointValues = PointValues.measurement("measurement");

        double floatValue = 2.71;
        long integerValue = 64L;
        boolean booleanValue = true;
        String stringValue = "text";

        pointValues.setField("floatField", floatValue);
        pointValues.setField("integerField", integerValue);
        pointValues.setField("booleanField", booleanValue);
        pointValues.setField("stringField", stringValue);

        Assertions.assertThat(pointValues.getField("floatField", Double.class)).isEqualTo(floatValue);
        Assertions.assertThat(pointValues.getFieldType("floatField")).isEqualTo(Double.class);
        Assertions.assertThat(pointValues.getField("integerField", Long.class)).isEqualTo(integerValue);
        Assertions.assertThat(pointValues.getFieldType("integerField")).isEqualTo(Long.class);
        Assertions.assertThat(pointValues.getField("booleanField", Boolean.class)).isEqualTo(booleanValue);
        Assertions.assertThat(pointValues.getFieldType("booleanField")).isEqualTo(Boolean.class);
        Assertions.assertThat(pointValues.getField("stringField", String.class)).isEqualTo(stringValue);
        Assertions.assertThat(pointValues.getFieldType("stringField")).isEqualTo(String.class);
        Assertions.assertThat(pointValues.getField("Missing", String.class)).isNull();
        Assertions.assertThat(pointValues.getFieldType("Missing")).isNull();
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
    void removeField() {
        PointValues pointValues = PointValues.measurement("measurement")
            .setField("field1", 42)
            .setField("field2", "value")
            .setField("field3", 3.14);

        pointValues.removeField("field1")
            .removeField("field2");

        Assertions.assertThat(pointValues.getField("field1")).isNull();
        Assertions.assertThat(pointValues.getField("field2")).isNull();
        Assertions.assertThat(3.14).isEqualTo(pointValues.getField("field3"));
    }

    @Test
    void getFieldNames() {
        PointValues pointValues = PointValues.measurement("measurement")
            .setField("field", 42)
            .setField("123", "value")
            .setField("some_name", 3.14);

        Assertions.assertThat(pointValues.getFieldNames())
            .isEqualTo(new String[]{"123", "field", "some_name"});
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
