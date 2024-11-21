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

import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class NanosecondConverterTest {

    @Test
    void testGetTimestampNanosecond() {
        BigInteger timestampNanoSecond = null;

        // Second
        FieldType timeTypeSecond = new FieldType(true,
                                                 new ArrowType.Timestamp(TimeUnit.SECOND, "UTC"),
                                                 null);
        Field timeFieldSecond = new Field("time", timeTypeSecond, null);
        timestampNanoSecond = NanosecondConverter.getTimestampNano(123_456L, timeFieldSecond);
        Assertions.assertEquals(
                BigInteger.valueOf(123_456L)
                          .multiply(BigInteger.valueOf(1_000_000_000)), timestampNanoSecond
        );

        // MilliSecond
        FieldType timeTypeMilliSecond = new FieldType(true,
                                                      new ArrowType.Timestamp(TimeUnit.MILLISECOND, "UTC"),
                                                      null);
        Field timeFieldMilliSecond = new Field("time", timeTypeMilliSecond, null);
        timestampNanoSecond = NanosecondConverter.getTimestampNano(123_456L, timeFieldMilliSecond);
        Assertions.assertEquals(
                BigInteger.valueOf(123_456L)
                          .multiply(BigInteger.valueOf(1_000_000)), timestampNanoSecond
        );

        // MicroSecond
        FieldType timeTypeMicroSecond = new FieldType(true,
                                                      new ArrowType.Timestamp(TimeUnit.MICROSECOND, "UTC"),
                                                      null);
        Field timeFieldMicroSecond = new Field("time", timeTypeMicroSecond, null);
        timestampNanoSecond = NanosecondConverter.getTimestampNano(123_456L, timeFieldMicroSecond);
        Assertions.assertEquals(
                BigInteger.valueOf(123_456L)
                          .multiply(BigInteger.valueOf(1_000)), timestampNanoSecond
        );

        // Nano Second
        FieldType timeTypeNanoSecond = new FieldType(true,
                                                      new ArrowType.Timestamp(TimeUnit.NANOSECOND, "UTC"),
                                                      null);
        Field timeFieldNanoSecond = new Field("time", timeTypeNanoSecond, null);
        timestampNanoSecond = NanosecondConverter.getTimestampNano(123_456L, timeFieldNanoSecond);
        Assertions.assertEquals(BigInteger.valueOf(123_456L), timestampNanoSecond);

        // For ArrowType.Time type
        FieldType timeMilliSecond = new FieldType(true,
                                                      new ArrowType.Time(TimeUnit.MILLISECOND, 32),
                                                      null);
        Field fieldMilliSecond = new Field("time", timeMilliSecond, null);
        timestampNanoSecond = NanosecondConverter.getTimestampNano(123_456L, fieldMilliSecond);
        Assertions.assertEquals(
                BigInteger.valueOf(123_456L)
                          .multiply(BigInteger.valueOf(1_000_000)), timestampNanoSecond
        );
    }
}
