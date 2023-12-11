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
import java.util.Map;
import javax.annotation.Nonnull;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BaseFixedWidthVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.assertj.core.api.Assertions;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;

import com.influxdb.v3.client.PointValues;

public class VectorSchemaRootConverterTest {
    @Test
    public void timestampAsArrowTime() {
        try (VectorSchemaRoot root = createTimeVector(1234, new ArrowType.Time(TimeUnit.MILLISECOND, 32))) {

            PointValues pointValues = VectorSchemaRootConverter.INSTANCE.toPointValues(0, root, root.getFieldVectors());

            BigInteger expected = BigInteger.valueOf(1_234 * 1_000_000);
            Assertions.assertThat((BigInteger) pointValues.getTimestamp()).isEqualByComparingTo(expected);
        }
    }

    @Test
    public void timestampAsArrowTimestamp() {

        try (VectorSchemaRoot root = createTimeVector(45_678, new ArrowType.Timestamp(TimeUnit.MILLISECOND, "UTC"))) {

            PointValues pointValues = VectorSchemaRootConverter.INSTANCE.toPointValues(0, root, root.getFieldVectors());

            BigInteger expected = BigInteger.valueOf(45_678L * 1_000_000);
            Assertions.assertThat((BigInteger) pointValues.getTimestamp()).isEqualByComparingTo(expected);
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
}
