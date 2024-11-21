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
        Assertions.assertEquals(BigInteger.valueOf(123_456L), timestampNanoSecond
        );
    }
}
