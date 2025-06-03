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

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

public final class VectorSchemaRootUtils {

    private VectorSchemaRootUtils() { }

    public static VectorSchemaRoot generateInvalidVectorSchemaRoot() {
        Field testField = generateInvalidIntField("test_field");
        Field testField1 = generateInvalidUnsignedIntField("test_field1");
        Field testField2 = generateInvalidFloatField("test_field2");
        Field testField3 = generateInvalidStringField("test_field3");
        Field testField4 = generateInvalidBoolField("test_field4");

        List<Field> fields = List.of(testField,
                                     testField1,
                                     testField2,
                                     testField3,
                                     testField4);

        VectorSchemaRoot root = initializeVectorSchemaRoot(fields.toArray(new Field[0]));

        VarCharVector intVector = (VarCharVector) root.getVector("test_field");
        intVector.allocateNew();
        intVector.set(0, "aaaa".getBytes());

        VarCharVector uIntVector = (VarCharVector) root.getVector("test_field1");
        uIntVector.allocateNew();
        uIntVector.set(0, "aaaa".getBytes());

        VarCharVector floatVector = (VarCharVector) root.getVector("test_field2");
        floatVector.allocateNew();
        floatVector.set(0, "aaaa".getBytes());

        Float8Vector stringVector = (Float8Vector) root.getVector("test_field3");
        stringVector.allocateNew();
        stringVector.set(0, 100.2);

        VarCharVector booleanVector = (VarCharVector) root.getVector("test_field4");
        booleanVector.allocateNew();
        booleanVector.set(0, "aaa".getBytes());

        return root;
    }

    public static VectorSchemaRoot generateVectorSchemaRoot() {
        Field measurementField = generateStringField("measurement");
        Field timeField = generateTimeField();
        Field memTotalField = generateIntField("mem_total");
        Field diskFreeField = generateUnsignedIntField("disk_free");
        Field temperatureField = generateFloatField("temperature");
        Field nameField = generateStringField("name");
        Field isActiveField = generateBoolField("isActive");
        List<Field> fields = List.of(measurementField,
                                     timeField,
                                     memTotalField,
                                     diskFreeField,
                                     temperatureField,
                                     nameField,
                                     isActiveField);

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
        floatVector.set(0, 100.8766);

        VarCharVector stringVector = (VarCharVector) root.getVector("name");
        stringVector.allocateNew();
        stringVector.setSafe(0, "intel".getBytes());

        BitVector boolVector = (BitVector) root.getVector("isActive");
        boolVector.allocateNew();
        boolVector.setSafe(0, 1);

        return root;
    }

    @Nonnull
    public static VectorSchemaRoot initializeVectorSchemaRoot(@Nonnull final Field... fields) {

        Schema schema = new Schema(Arrays.asList(fields));

        VectorSchemaRoot root = VectorSchemaRoot.create(schema, new RootAllocator(Long.MAX_VALUE));
        root.allocateNew();

        return root;
    }

    public static Field generateIntField(final String fieldName) {
        Map<String, String> metadata = new HashMap<>();
        metadata.put("iox::column::type", "iox::column_type::field::integer");
        FieldType intType = new FieldType(true,
                                          new ArrowType.Int(64, true),
                                          null,
                                          metadata);
        return new Field(fieldName, intType, null);
    }

    public static Field generateInvalidIntField(final String fieldName) {
        Map<String, String> metadata = new HashMap<>();
        metadata.put("iox::column::type", "iox::column_type::field::integer");
        FieldType intType = new FieldType(true,
                                          new ArrowType.Utf8(),
                                          null,
                                          metadata);
        return new Field(fieldName, intType, null);
    }

    public static Field generateUnsignedIntField(final String fieldName) {
        Map<String, String> metadata = new HashMap<>();
        metadata.put("iox::column::type", "iox::column_type::field::uinteger");
        FieldType intType = new FieldType(true,
                                          new ArrowType.Int(64, true),
                                          null,
                                          metadata);
        return new Field(fieldName, intType, null);
    }

    public static Field generateInvalidUnsignedIntField(final String fieldName) {
        Map<String, String> metadata = new HashMap<>();
        metadata.put("iox::column::type", "iox::column_type::field::uinteger");
        FieldType intType = new FieldType(true,
                                          new ArrowType.Utf8(),
                                          null,
                                          metadata);
        return new Field(fieldName, intType, null);
    }

    public static Field generateFloatField(final String fieldName) {
        Map<String, String> metadata = new HashMap<>();
        metadata.put("iox::column::type", "iox::column_type::field::float");
        FieldType floatType = new FieldType(true,
                                            new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE),
                                            null,
                                            metadata);
        return new Field(fieldName, floatType, null);
    }

    public static Field generateInvalidFloatField(final String fieldName) {
        Map<String, String> metadata = new HashMap<>();
        metadata.put("iox::column::type", "iox::column_type::field::float");
        FieldType floatType = new FieldType(true,
                                            new ArrowType.Utf8(),
                                            null,
                                            metadata);
        return new Field(fieldName, floatType, null);
    }

    public static Field generateStringField(final String fieldName) {
        Map<String, String> metadata = new HashMap<>();
        metadata.put("iox::column::type", "iox::column_type::field::string");
        FieldType stringType = new FieldType(true, new ArrowType.Utf8(), null, metadata);
        return new Field(fieldName, stringType, null);
    }

    public static Field generateInvalidStringField(final String fieldName) {
        Map<String, String> metadata = new HashMap<>();
        metadata.put("iox::column::type", "iox::column_type::field::string");
        FieldType stringType = new FieldType(true,
                                             new ArrowType.FloatingPoint(
                                                     FloatingPointPrecision.DOUBLE),
                                             null,
                                             metadata);
        return new Field(fieldName, stringType, null);
    }

    public static Field generateBoolField(final String fieldName) {
        Map<String, String> metadata = new HashMap<>();
        metadata.put("iox::column::type", "iox::column_type::field::boolean");
        FieldType boolType = new FieldType(true, new ArrowType.Bool(), null, metadata);
        return new Field(fieldName, boolType, null);
    }

    public static Field generateInvalidBoolField(final String fieldName) {
        Map<String, String> metadata = new HashMap<>();
        metadata.put("iox::column::type", "iox::column_type::field::boolean");
        FieldType boolType = new FieldType(true, new ArrowType.Utf8(), null, metadata);
        return new Field(fieldName, boolType, null);
    }

    public static Field generateTimeField() {
        FieldType timeType = new FieldType(true,
                                           new ArrowType.Time(TimeUnit.MILLISECOND, 32),
                                           null);
        return new Field("time", timeType, null);
    }
}
