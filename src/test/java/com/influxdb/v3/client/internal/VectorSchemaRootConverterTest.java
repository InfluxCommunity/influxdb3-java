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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BaseFixedWidthVector;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.assertj.core.api.Assertions;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;

import com.influxdb.v3.client.PointValues;

class VectorSchemaRootConverterTest {

    @Test
    void timestampAsArrowTime() {
        try (VectorSchemaRoot root = createTimeVector(1234, new ArrowType.Time(TimeUnit.MILLISECOND, 32))) {

            PointValues pointValues = VectorSchemaRootConverter.INSTANCE.toPointValues(0, root, root.getFieldVectors());

            BigInteger expected = BigInteger.valueOf(1_234 * 1_000_000);
            Assertions.assertThat((BigInteger) pointValues.getTimestamp()).isEqualByComparingTo(expected);
        }
    }

    @Test
    void timestampAsArrowTimestampSecond() {
        try (VectorSchemaRoot root = createTimeVector(45_678, new ArrowType.Timestamp(TimeUnit.SECOND, "UTC"))) {

            PointValues pointValues = VectorSchemaRootConverter.INSTANCE.toPointValues(0, root, root.getFieldVectors());

            BigInteger expected = BigInteger.valueOf(45_678L * 1_000_000_000);
            Assertions.assertThat((BigInteger) pointValues.getTimestamp()).isEqualByComparingTo(expected);
        }
    }

    @Test
    void timestampAsArrowTimestampMillisecond() {
        try (VectorSchemaRoot root = createTimeVector(45_678, new ArrowType.Timestamp(TimeUnit.MILLISECOND, "UTC"))) {

            PointValues pointValues = VectorSchemaRootConverter.INSTANCE.toPointValues(0, root, root.getFieldVectors());

            BigInteger expected = BigInteger.valueOf(45_678L * 1_000_000);
            Assertions.assertThat((BigInteger) pointValues.getTimestamp()).isEqualByComparingTo(expected);
        }
    }

    @Test
    void timestampAsArrowTimestampMicrosecond() {
        try (VectorSchemaRoot root = createTimeVector(45_678, new ArrowType.Timestamp(TimeUnit.MICROSECOND, "UTC"))) {

            PointValues pointValues = VectorSchemaRootConverter.INSTANCE.toPointValues(0, root, root.getFieldVectors());

            BigInteger expected = BigInteger.valueOf(45_678L * 1_000);
            Assertions.assertThat((BigInteger) pointValues.getTimestamp()).isEqualByComparingTo(expected);
        }
    }

    @Test
    void timestampAsArrowTimestampNanosecond() {
        try (VectorSchemaRoot root = createTimeVector(45_678, new ArrowType.Timestamp(TimeUnit.NANOSECOND, "UTC"))) {

            PointValues pointValues = VectorSchemaRootConverter.INSTANCE.toPointValues(0, root, root.getFieldVectors());

            BigInteger expected = BigInteger.valueOf(45_678L);
            Assertions.assertThat((BigInteger) pointValues.getTimestamp()).isEqualByComparingTo(expected);
        }
    }

    @Test
    void timestampAsArrowTimestampNanosecondWithoutTimezone() {
        try (VectorSchemaRoot root = createTimeVector(45_978, new ArrowType.Timestamp(TimeUnit.NANOSECOND, null))) {

            PointValues pointValues = VectorSchemaRootConverter.INSTANCE.toPointValues(0, root, root.getFieldVectors());

            BigInteger expected = BigInteger.valueOf(45_978L);
            Assertions.assertThat((BigInteger) pointValues.getTimestamp()).isEqualByComparingTo(expected);
        }
    }

    @Test
    void timestampAsArrowInt() {
        try (VectorSchemaRoot root = createTimeVector(45_678, new ArrowType.Int(64, true))) {

            PointValues pointValues = VectorSchemaRootConverter.INSTANCE.toPointValues(0, root, root.getFieldVectors());

            BigInteger expected = BigInteger.valueOf(45_678L * 1_000_000);
            Assertions.assertThat((BigInteger) pointValues.getTimestamp()).isEqualByComparingTo(expected);
        }
    }

    @Test
    public void testConverterWithMetaType() {
        try (VectorSchemaRoot root = generateVectorSchemaRoot()) {
            PointValues pointValues = VectorSchemaRootConverter.INSTANCE.toPointValues(0, root, root.getFieldVectors());

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
            Assertions.assertThat(temperature).isEqualTo(100.8766f);
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
    void timestampWithoutMetadataAndFieldWithoutMetadata() {
        FieldType timeType = new FieldType(true, new ArrowType.Time(TimeUnit.MILLISECOND, 32), null);
        Field timeField = new Field("time", timeType, null);

        FieldType stringType = new FieldType(true, new ArrowType.Utf8(), null);
        Field field1 = new Field("field1", stringType, null);

        try (VectorSchemaRoot root = initializeVectorSchemaRoot(timeField, field1)) {

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

            PointValues pointValues = VectorSchemaRootConverter.INSTANCE.toPointValues(0, root, root.getFieldVectors());

            BigInteger expected = BigInteger.valueOf(123_456L * 1_000_000);
            Assertions.assertThat((BigInteger) pointValues.getTimestamp()).isEqualByComparingTo(expected);
            Assertions.assertThat(pointValues.getStringField("field1")).isEqualTo("field1Value");
        }
    }

    @Test
    public void measurementValue() {
        FieldType stringType = new FieldType(true, new ArrowType.Utf8(), null);
        Field measurementField = new Field("measurement", stringType, null);

        try (VectorSchemaRoot root = initializeVectorSchemaRoot(measurementField)) {

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

            PointValues pointValues = VectorSchemaRootConverter.INSTANCE.toPointValues(0, root, root.getFieldVectors());

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

        return initializeVectorSchemaRoot(timeField);
    }

    @NotNull
    private VectorSchemaRoot initializeVectorSchemaRoot(@Nonnull final Field... fields) {

        Schema schema = new Schema(Arrays.asList(fields));

        VectorSchemaRoot root = VectorSchemaRoot.create(schema, new RootAllocator(Long.MAX_VALUE));
        root.allocateNew();

        return root;
    }

    public VectorSchemaRoot generateVectorSchemaRoot() {
        Field measurementField = generateStringField("measurement");
        Field timeField = generateTimeField();
        Field memTotalField = generateIntField("mem_total");
        Field diskFreeField = generateUnsignedIntField("disk_free");
        Field temperatureField = generateFloatField("temperature");
        Field nameField = generateStringField("name");
        Field isActiveField = generateBoolField("isActive");
        List<Field> fields = List.of(measurementField, timeField, memTotalField, diskFreeField, temperatureField, nameField, isActiveField);

        VectorSchemaRoot root = initializeVectorSchemaRoot(fields.toArray(new Field[0]));
        VarCharVector measurement = (VarCharVector) root.getVector("measurement");
        measurement.allocateNew();
        measurement.set(0, "host".getBytes());

        TimeMilliVector timeVector = (TimeMilliVector) root.getVector("time");
        timeVector.allocateNew();
        timeVector.setSafe(0, 123_456);

        BigIntVector intVector = (BigIntVector) root.getVector("mem_total");
        intVector.allocateNew();
        intVector.set(0, 2048);

        BigIntVector unsignedIntVector = (BigIntVector) root.getVector("disk_free");
        unsignedIntVector.allocateNew();
        unsignedIntVector.set(0, 1_000_000);

        Float8Vector floatVector = (Float8Vector) root.getVector("temperature");
        floatVector.allocateNew();
        floatVector.set(0, 100.8766f);

        VarCharVector stringVector = (VarCharVector) root.getVector("name");
        stringVector.allocateNew();
        stringVector.setSafe(0, "intel".getBytes());

        BitVector boolVector = (BitVector) root.getVector("isActive");
        boolVector.allocateNew();
        boolVector.setSafe(0, 1);

        return root;
    }

    private Field generateIntField(final String fieldName) {
        Map<String, String> metadata = new HashMap<>();
        metadata.put("iox::column::type", "iox::column_type::field::integer");
        FieldType intType = new FieldType(true, new ArrowType.Int(64, true), null, metadata);
        return new Field(fieldName, intType, null);
    }

    private Field generateUnsignedIntField(final String fieldName) {
        Map<String, String> metadata = new HashMap<>();
        metadata.put("iox::column::type", "iox::column_type::field::uinteger");
        FieldType intType = new FieldType(true, new ArrowType.Int(64, true), null, metadata);
        return new Field(fieldName, intType, null);
    }

    private Field generateFloatField(final String fieldName) {
        Map<String, String> metadata = new HashMap<>();
        metadata.put("iox::column::type", "iox::column_type::field::float");
        FieldType floatType = new FieldType(true, new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE), null, metadata);
        return new Field(fieldName, floatType, null);
    }

    private Field generateStringField(final String fieldName) {
        Map<String, String> metadata = new HashMap<>();
        metadata.put("iox::column::type", "iox::column_type::field::string");
        FieldType stringType = new FieldType(true, new ArrowType.Utf8(), null, metadata);
        return new Field(fieldName, stringType, null);
    }

    private Field generateBoolField(final String fieldName) {
        Map<String, String> metadata = new HashMap<>();
        metadata.put("iox::column::type", "iox::column_type::field::boolean");
        FieldType boolType = new FieldType(true, new ArrowType.Bool(), null, metadata);
        return new Field(fieldName, boolType, null);
    }

    private Field generateTimeField() {
        FieldType timeType = new FieldType(true, new ArrowType.Time(TimeUnit.MILLISECOND, 32), null);
        return new Field("time", timeType, null);
    }

}
