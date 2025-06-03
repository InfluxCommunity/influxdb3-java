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
package com.influxdb.v3.client.internal;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nonnull;

import org.apache.arrow.vector.BaseFixedWidthVector;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import com.influxdb.v3.client.PointValues;

class VectorSchemaRootConverterTest {

    @Test
    void timestampAsArrowTime() {
        try (VectorSchemaRoot root = createTimeVector(1234, new ArrowType.Time(TimeUnit.MILLISECOND, 32))) {

            PointValues pointValues = VectorSchemaRootConverter.INSTANCE.toPointValues(0, root.getFieldVectors());

            BigInteger expected = BigInteger.valueOf(1_234 * 1_000_000);
            Assertions.assertThat((BigInteger) pointValues.getTimestamp()).isEqualByComparingTo(expected);
        }
    }

    @Test
    void timestampAsArrowTimestampSecond() {
        try (VectorSchemaRoot root = createTimeVector(45_678, new ArrowType.Timestamp(TimeUnit.SECOND, "UTC"))) {

            PointValues pointValues = VectorSchemaRootConverter.INSTANCE.toPointValues(0, root.getFieldVectors());

            BigInteger expected = BigInteger.valueOf(45_678L * 1_000_000_000);
            Assertions.assertThat((BigInteger) pointValues.getTimestamp()).isEqualByComparingTo(expected);
        }
    }

    @Test
    void timestampAsArrowTimestampMillisecond() {
        try (VectorSchemaRoot root = createTimeVector(45_678, new ArrowType.Timestamp(TimeUnit.MILLISECOND, "UTC"))) {

            PointValues pointValues = VectorSchemaRootConverter.INSTANCE.toPointValues(0, root.getFieldVectors());

            BigInteger expected = BigInteger.valueOf(45_678L * 1_000_000);
            Assertions.assertThat((BigInteger) pointValues.getTimestamp()).isEqualByComparingTo(expected);
        }
    }

    @Test
    void timestampAsArrowTimestampMicrosecond() {
        try (VectorSchemaRoot root = createTimeVector(45_678, new ArrowType.Timestamp(TimeUnit.MICROSECOND, "UTC"))) {

            PointValues pointValues = VectorSchemaRootConverter.INSTANCE.toPointValues(0, root.getFieldVectors());

            BigInteger expected = BigInteger.valueOf(45_678L * 1_000);
            Assertions.assertThat((BigInteger) pointValues.getTimestamp()).isEqualByComparingTo(expected);
        }
    }

    @Test
    void timestampAsArrowTimestampNanosecond() {
        try (VectorSchemaRoot root = createTimeVector(45_678, new ArrowType.Timestamp(TimeUnit.NANOSECOND, "UTC"))) {

            PointValues pointValues = VectorSchemaRootConverter.INSTANCE.toPointValues(0, root.getFieldVectors());

            BigInteger expected = BigInteger.valueOf(45_678L);
            Assertions.assertThat((BigInteger) pointValues.getTimestamp()).isEqualByComparingTo(expected);
        }
    }

    @Test
    void timestampAsArrowTimestampNanosecondWithoutTimezone() {
        try (VectorSchemaRoot root = createTimeVector(45_978, new ArrowType.Timestamp(TimeUnit.NANOSECOND, null))) {

            PointValues pointValues = VectorSchemaRootConverter.INSTANCE.toPointValues(0, root.getFieldVectors());

            BigInteger expected = BigInteger.valueOf(45_978L);
            Assertions.assertThat((BigInteger) pointValues.getTimestamp()).isEqualByComparingTo(expected);
        }
    }

    @Test
    void timestampAsArrowInt() {
        try (VectorSchemaRoot root = createTimeVector(45_678, new ArrowType.Int(64, true))) {

            PointValues pointValues = VectorSchemaRootConverter.INSTANCE.toPointValues(0, root.getFieldVectors());

            BigInteger expected = BigInteger.valueOf(45_678L * 1_000_000);
            Assertions.assertThat((BigInteger) pointValues.getTimestamp()).isEqualByComparingTo(expected);
        }
    }

    @Test
    void getMappedValueValidMetaDataInteger() {
        Field field = VectorSchemaRootUtils.generateIntField("test");
        Long value = 1L;
        Object mappedValue = VectorSchemaRootConverter.INSTANCE.getMappedValue(field, value);
        Assertions.assertThat(mappedValue).isEqualTo(value);
        Assertions.assertThat(mappedValue.getClass()).isEqualTo(Long.class);
    }

    @Test
    void getMappedValueInvalidMetaDataInteger() {
        Field field = VectorSchemaRootUtils.generateInvalidIntField("test");
        String value = "1";
        Object mappedValue = VectorSchemaRootConverter.INSTANCE.getMappedValue(field, value);
        Assertions.assertThat(mappedValue).isEqualTo(value);
        Assertions.assertThat(mappedValue.getClass()).isEqualTo(String.class);
    }

    @Test
    void getMappedValueValidMetaDataFloat() {
        Field field = VectorSchemaRootUtils.generateFloatField("test");
        Double value = 1.2;
        Object mappedValue = VectorSchemaRootConverter.INSTANCE.getMappedValue(field, value);
        Assertions.assertThat(mappedValue).isEqualTo(value);
        Assertions.assertThat(mappedValue.getClass()).isEqualTo(Double.class);
    }

    @Test
    void getMappedValueInvalidMetaDataFloat() {
        Field field = VectorSchemaRootUtils.generateInvalidFloatField("test");
        String value = "1.2";
        Object mappedValue = VectorSchemaRootConverter.INSTANCE.getMappedValue(field, value);
        Assertions.assertThat(mappedValue).isEqualTo(value);
        Assertions.assertThat(mappedValue.getClass()).isEqualTo(String.class);
    }

    @Test
    void getMappedValueValidMetaDataString() {
        Field field = VectorSchemaRootUtils.generateStringField("test");
        String value = "string";
        Object mappedValue = VectorSchemaRootConverter.INSTANCE.getMappedValue(field, value);
        Assertions.assertThat(mappedValue).isEqualTo(value);
        Assertions.assertThat(mappedValue.getClass()).isEqualTo(String.class);
    }

    @Test
    void getMappedValueInvalidMetaDataString() {
        Field field = VectorSchemaRootUtils.generateInvalidStringField("test");
        Double value = 1.1;
        Object mappedValue = VectorSchemaRootConverter.INSTANCE.getMappedValue(field, value);
        Assertions.assertThat(mappedValue).isEqualTo(value);
        Assertions.assertThat(mappedValue.getClass()).isEqualTo(Double.class);
    }

    @Test
    void getMappedValueValidMetaDataBoolean() {
        Field field = VectorSchemaRootUtils.generateBoolField("test");
        Boolean value = true;
        Object mappedValue = VectorSchemaRootConverter.INSTANCE.getMappedValue(field, value);
        Assertions.assertThat(mappedValue).isEqualTo(value);
        Assertions.assertThat(mappedValue.getClass()).isEqualTo(Boolean.class);
    }

    @Test
    void getMappedValueInvalidMetaDataBoolean() {
        Field field = VectorSchemaRootUtils.generateInvalidBoolField("test");
        String value = "true";
        Object mappedValue = VectorSchemaRootConverter.INSTANCE.getMappedValue(field, value);
        Assertions.assertThat(mappedValue).isEqualTo(value);
        Assertions.assertThat(mappedValue.getClass()).isEqualTo(String.class);
    }

    @Test
    public void testConverterWithMetaType() {
        try (VectorSchemaRoot root = VectorSchemaRootUtils.generateVectorSchemaRoot()) {
            PointValues pointValues = VectorSchemaRootConverter.INSTANCE.toPointValues(0, root.getFieldVectors());

            String measurement = pointValues.getMeasurement();
            Assertions.assertThat(measurement).isEqualTo("host");
            Assertions.assertThat(measurement.getClass()).isEqualTo(String.class);

            BigInteger expected = BigInteger.valueOf(123_456L * 1_000_000);
            Assertions.assertThat((BigInteger) pointValues.getTimestamp()).isEqualByComparingTo(expected);

            Long memTotal = (Long) pointValues.getField("mem_total");
            Assertions.assertThat(memTotal).isEqualTo(2048);
            Assertions.assertThat(memTotal.getClass()).isEqualTo(Long.class);

            Long diskFree = (Long) pointValues.getField("disk_free");
            Assertions.assertThat(diskFree).isEqualTo(1_000_000);
            Assertions.assertThat(diskFree.getClass()).isEqualTo(Long.class);

            Double temperature = (Double) pointValues.getField("temperature");
            Assertions.assertThat(temperature).isEqualTo(100.8766);
            Assertions.assertThat(temperature.getClass()).isEqualTo(Double.class);

            String name = (String) pointValues.getField("name");
            Assertions.assertThat(name).isEqualTo("intel");
            Assertions.assertThat(name.getClass()).isEqualTo(String.class);

            Boolean isActive = (Boolean) pointValues.getField("isActive");
            Assertions.assertThat(isActive).isEqualTo(true);
            Assertions.assertThat(isActive.getClass()).isEqualTo(Boolean.class);
        }
    }

    @Test
    void testGetMapFromVectorSchemaRoot() {
        try (VectorSchemaRoot root = VectorSchemaRootUtils.generateVectorSchemaRoot()) {
            Map<String, Object> map = VectorSchemaRootConverter.INSTANCE.getMapFromVectorSchemaRoot(root, 0);

            Assertions.assertThat(map).hasSize(7);
            Assertions.assertThat(map.get("measurement")).isEqualTo("host");
            Assertions.assertThat(map.get("mem_total")).isEqualTo(2048L);
            Assertions.assertThat(map.get("temperature")).isEqualTo(100.8766);
            Assertions.assertThat(map.get("isActive")).isEqualTo(true);
            Assertions.assertThat(map.get("name")).isEqualTo("intel");
            Assertions.assertThat(map.get("time")).isEqualTo(BigInteger.valueOf(123_456L * 1_000_000));
        }
    }

    @Test
    void timestampWithoutMetadataAndFieldWithoutMetadata() {
        FieldType timeType = new FieldType(true, new ArrowType.Time(TimeUnit.MILLISECOND, 32), null);
        Field timeField = new Field("time", timeType, null);

        FieldType stringType = new FieldType(true, new ArrowType.Utf8(), null);
        Field field1 = new Field("field1", stringType, null);

        try (VectorSchemaRoot root = VectorSchemaRootUtils.initializeVectorSchemaRoot(timeField, field1)) {

            //
            // set data
            //
            TimeMilliVector timeVector = (TimeMilliVector) root.getVector("time");
            timeVector.allocateNew();
            timeVector.setSafe(0, 123_456);

            VarCharVector field1Vector = (VarCharVector) root.getVector("field1");
            field1Vector.allocateNew();
            field1Vector.setSafe(0, "field1Value".getBytes(StandardCharsets.UTF_8));

            //
            // set rows count
            //
            timeVector.setValueCount(1);
            root.setRowCount(1);

            PointValues pointValues = VectorSchemaRootConverter.INSTANCE.toPointValues(0, root.getFieldVectors());

            BigInteger expected = BigInteger.valueOf(123_456L * 1_000_000);
            Assertions.assertThat((BigInteger) pointValues.getTimestamp()).isEqualByComparingTo(expected);
            Assertions.assertThat(pointValues.getStringField("field1")).isEqualTo("field1Value");
        }
    }

    @Test
    public void measurementValue() {
        FieldType stringType = new FieldType(true, new ArrowType.Utf8(), null);
        Field measurementField = new Field("measurement", stringType, null);

        try (VectorSchemaRoot root = VectorSchemaRootUtils.initializeVectorSchemaRoot(measurementField)) {

            //
            // set data
            //
            VarCharVector measurementVector = (VarCharVector) root.getVector("measurement");
            measurementVector.allocateNew();
            measurementVector.setSafe(0, "measurementValue".getBytes(StandardCharsets.UTF_8));

            //
            // set rows count
            //
            measurementVector.setValueCount(1);
            root.setRowCount(1);

            PointValues pointValues = VectorSchemaRootConverter.INSTANCE.toPointValues(0, root.getFieldVectors());

            Assertions.assertThat(pointValues.getMeasurement()).isEqualTo("measurementValue");
        }
    }

    @Nonnull
    private VectorSchemaRoot createTimeVector(final int timeValue, final ArrowType type) {

        VectorSchemaRoot root = createVectorSchemaRoot(type);

        //
        // set data
        //
        BaseFixedWidthVector timeVector = (BaseFixedWidthVector) root.getVector("timestamp");
        timeVector.allocateNew();
        if (timeVector instanceof TimeMilliVector) {
            ((TimeMilliVector) timeVector).setSafe(0, timeValue);
        } else if (timeVector instanceof TimeStampVector) {
            ((TimeStampVector) timeVector).setSafe(0, timeValue);
        } else if (timeVector instanceof BigIntVector) {
            ((BigIntVector) timeVector).setSafe(0, timeValue);
        } else {
            throw new RuntimeException("Unexpected vector type: " + timeVector.getClass().getName());
        }

        //
        // set rows count
        //
        timeVector.setValueCount(1);
        root.setRowCount(1);

        return root;
    }

    @Nonnull
    private VectorSchemaRoot createVectorSchemaRoot(@Nonnull final ArrowType arrowType) {

        // Creating metadata
        Map<String, String> metadata = new HashMap<>();
        metadata.put("iox::column::type", "iox::type::timestamp");
        FieldType timeType = new FieldType(true, arrowType, null, metadata);
        Field timeField = new Field("timestamp", timeType, null);

        return VectorSchemaRootUtils.initializeVectorSchemaRoot(timeField);
    }
}
