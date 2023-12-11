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

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.util.Text;

import com.influxdb.v3.client.PointValues;


/**
 * The VectorSchemaRootConverter class is responsible for converting rows of data from a VectorSchemaRoot object
 * to PointValues. It provides a method to perform this conversion.
 * <p>
 * This class is thread-safe.
 */
@ThreadSafe
final class VectorSchemaRootConverter {

    static final VectorSchemaRootConverter INSTANCE = new VectorSchemaRootConverter();

    /**
     * Converts a given row of data from a VectorSchemaRoot object to PointValues.
     *
     * @param rowNumber    the index of the row to be converted
     * @param vector       the VectorSchemaRoot object containing the data
     * @param fieldVectors the list of FieldVector objects representing the data columns
     * @return the converted PointValues object
     */
    @Nonnull
    public PointValues toPointValues(final int rowNumber,
                                     @Nonnull final VectorSchemaRoot vector,
                                     @Nonnull final List<FieldVector> fieldVectors) {
        PointValues p = new PointValues();
        for (int i = 0; i < fieldVectors.size(); i++) {
            var schema = vector.getSchema().getFields().get(i);
            var value = fieldVectors.get(i).getObject(rowNumber);
            var name = schema.getName();
            var metaType = schema.getMetadata().get("iox::column::type");

            if (value instanceof Text) {
                value = value.toString();
            }

            if ((Objects.equals(name, "measurement")
                    || Objects.equals(name, "iox::measurement"))
                    && value instanceof String) {
                p.setMeasurement((String) value);
                continue;
            }

            if (metaType == null) {
                if (Objects.equals(name, "time") && value instanceof Instant) {
                    p.setTimestamp((Instant) value);
                } else {
                    // just push as field If you don't know what type is it
                    p.setField(name, value);
                }

                continue;
            }

            String[] parts = metaType.split("::");
            String valueType = parts[2];

            if ("field".equals(valueType)) {
                p.setField(name, value);
            } else if ("tag".equals(valueType) && value instanceof String) {
                p.setTag(name, (String) value);
            } else if ("timestamp".equals(valueType)) {
                if (value instanceof Long) {
                    p.setTimestamp(Instant.ofEpochMilli((Long) value));
                } else if (value instanceof LocalDateTime) {
                    p.setTimestamp(((LocalDateTime) value).toInstant(ZoneOffset.UTC));
                }
            }
        }
        return p;
    }
}
