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
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.logging.Logger;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.util.Text;

import com.influxdb.v3.client.PointValues;
import com.influxdb.v3.client.write.WritePrecision;


/**
 * The VectorSchemaRootConverter class is responsible for converting rows of data from a VectorSchemaRoot object
 * to PointValues. It provides a method to perform this conversion.
 * <p>
 * This class is thread-safe.
 */
@ThreadSafe
public final class VectorSchemaRootConverter {

    private static final Logger LOG = Logger.getLogger(VectorSchemaRootConverter.class.getName());

    public static final VectorSchemaRootConverter INSTANCE = new VectorSchemaRootConverter();

    /**
     * Converts a given row of data from a VectorSchemaRoot object to PointValues.
     *
     * @param rowNumber    the index of the row to be converted
     * @param fieldVectors the list of FieldVector objects representing the data columns
     * @return the converted PointValues object
     */
    @Nonnull
    PointValues toPointValues(final int rowNumber,
                              @Nonnull final List<FieldVector> fieldVectors) {
        PointValues p = new PointValues();
        for (FieldVector fieldVector : fieldVectors) {
            var field = fieldVector.getField();
            var value = fieldVector.getObject(rowNumber);
            var fieldName = field.getName();
            var metaType = field.getMetadata().get("iox::column::type");

            if (value instanceof Text) {
                value = value.toString();
            }

            if ((Objects.equals(fieldName, "measurement")
                    || Objects.equals(fieldName, "iox::measurement"))
                    && value instanceof String) {
                p.setMeasurement((String) value);
                continue;
            }

            if (metaType == null) {
                if (Objects.equals(fieldName, "time") && (value instanceof Long || value instanceof LocalDateTime)) {
                    var timeNano = NanosecondConverter.getTimestampNano(value, field);
                    p.setTimestamp(timeNano, WritePrecision.NS);
                } else {
                    // just push as field If you don't know what type is it
                    p.setField(fieldName, value);
                }

                continue;
            }

            String valueType = metaType.split("::")[2];
            Object mappedValue = getMappedValue(field, value);
            if ("field".equals(valueType)) {
                p.setField(fieldName, mappedValue);
            } else if ("tag".equals(valueType) && value instanceof String) {
                p.setTag(fieldName, (String) mappedValue);
            } else if ("timestamp".equals(valueType)) {
                p.setTimestamp((BigInteger) mappedValue, WritePrecision.NS);
            }
        }
        return p;
    }

    /**
     * Function to cast value return base on metadata from InfluxDB.
     *
     * @param field the Field object from Arrow
     * @param value the value to cast
     * @return the value with the correct type
     */
    public Object getMappedValue(@Nonnull final Field field, @Nullable final Object value) {
        if (value == null) {
            return null;
        }

        var fieldName = field.getName();
        if ("measurement".equals(fieldName) || "iox::measurement".equals(fieldName)) {
            return value.toString();
        }

        var metaType = field.getMetadata().get("iox::column::type");
        if (metaType == null) {
            if ("time".equals(fieldName) && (value instanceof Long || value instanceof LocalDateTime)) {
                return NanosecondConverter.getTimestampNano(value, field);
            } else {
                return value;
            }
        }

        String[] parts = metaType.split("::");
        String valueType = parts[2];
        if ("field".equals(valueType)) {
            switch (metaType) {
                case "iox::column_type::field::integer":
                case "iox::column_type::field::uinteger":
                    if (value instanceof Number) {
                        return TypeCasting.toLongValue(value);
                    } else {
                        LOG.warning(String.format("Value %s is not an Long", value));
                        return value;
                    }
                case "iox::column_type::field::float":
                    if (value instanceof Number) {
                        return TypeCasting.toDoubleValue(value);
                    } else {
                        LOG.warning(String.format("Value %s is not a Double", value));
                        return value;
                    }
                case "iox::column_type::field::string":
                    if (value instanceof Text || value instanceof String) {
                        return TypeCasting.toStringValue(value);
                    } else {
                        LOG.warning(String.format("Value %s is not a String", value));
                        return value;
                    }
                case "iox::column_type::field::boolean":
                    if (value instanceof Boolean) {
                        return value;
                    } else {
                        LOG.warning(String.format("Value %s is not a Boolean", value));
                        return value;
                    }
                default:
                    return value;
            }
        } else if ("timestamp".equals(valueType) || Objects.equals(fieldName, "time")) {
            return NanosecondConverter.getTimestampNano(value, field);
        } else {
            return TypeCasting.toStringValue(value);
        }
    }

    /**
     * Get array of values from VectorSchemaRoot.
     *
     * @param vector    The data return from InfluxDB.
     * @param rowNumber The row number of data
     * @return  An array of Objects represents a row of data
     */
    public Object[] getArrayObjectFromVectorSchemaRoot(@Nonnull final VectorSchemaRoot vector, final int rowNumber) {
        List<FieldVector> fieldVectors = vector.getFieldVectors();
        int columnSize = fieldVectors.size();
        var row = new Object[columnSize];
        for (int i = 0; i < columnSize; i++) {
            FieldVector fieldVector = fieldVectors.get(i);
            row[i] = getMappedValue(
                    fieldVector.getField(),
                    fieldVector.getObject(rowNumber)
            );
        }

        return row;
    }

    /**
     * Get a Map from VectorSchemaRoot.
     *
     * @param vector    The data return from InfluxDB.
     * @param rowNumber The row number of data
     * @return  A Map represents a row of data
     */
    public Map<String, Object> getMapFromVectorSchemaRoot(@Nonnull final VectorSchemaRoot vector, final int rowNumber) {
        Map<String, Object> row = new HashMap<>();
        for (FieldVector fieldVector : vector.getFieldVectors()) {
            Object mappedValue = getMappedValue(
                    fieldVector.getField(),
                    fieldVector.getObject(rowNumber)
            );
            row.put(fieldVector.getName(), mappedValue);

        }

        return row;
    }
}
