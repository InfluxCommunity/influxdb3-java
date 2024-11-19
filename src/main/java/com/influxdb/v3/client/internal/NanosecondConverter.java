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

import com.influxdb.v3.client.write.WritePrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static java.util.function.Function.identity;

/**
 * Nanosecond converter.
 * <p>
 * Utility class converting epoch nanoseconds to epoch with a given precision.
 */
public final class NanosecondConverter {

    private static final BigInteger NANOS_PER_SECOND = BigInteger.valueOf(1000_000_000L);
    private static final BigInteger MICRO_PER_NANOS = BigInteger.valueOf(1000L);
    private static final BigInteger MILLIS_PER_NANOS = BigInteger.valueOf(1000_000L);
    private static final BigInteger SECONDS_PER_NANOS = BigInteger.valueOf(1000_000_000L);

    private NanosecondConverter() {
    }

    /**
     * Timestamp calculation functions to add timestamp to records.
     */
    private static final Map<WritePrecision, Function<BigInteger, BigInteger>> FROM_NANOS = new HashMap<>();

    static {
        FROM_NANOS.put(WritePrecision.S, (timestamp) -> timestamp.divide(SECONDS_PER_NANOS));
        FROM_NANOS.put(WritePrecision.MS, (timestamp) -> timestamp.divide(MILLIS_PER_NANOS));
        FROM_NANOS.put(WritePrecision.US, (timestamp) -> timestamp.divide(MICRO_PER_NANOS));
        FROM_NANOS.put(WritePrecision.NS, identity());
    }

    /**
     * Timestamp calculation functions from specified precision to nanos.
     */
    private static final Map<WritePrecision, Function<BigInteger, BigInteger>> TO_NANOS = new HashMap<>();

    static {
        TO_NANOS.put(WritePrecision.S, (timestamp) -> timestamp.multiply(SECONDS_PER_NANOS));
        TO_NANOS.put(WritePrecision.MS, (timestamp) -> timestamp.multiply(MILLIS_PER_NANOS));
        TO_NANOS.put(WritePrecision.US, (timestamp) -> timestamp.multiply(MICRO_PER_NANOS));
        TO_NANOS.put(WritePrecision.NS, identity());
    }

    /**
     * Convert timestamp in a given precision to nanoseconds.
     *
     * @param timestamp epoch timestamp
     * @param precision precision
     * @return epoch timestamp in precision, can be null
     */
    @Nullable
    public static BigInteger convertToNanos(@Nullable final Number timestamp, final WritePrecision precision) {
        if (timestamp == null) {
            return null;
        }

        BigInteger t;
        if (timestamp instanceof BigDecimal) {
            t = ((BigDecimal) timestamp).toBigInteger();
        } else if (timestamp instanceof BigInteger) {
            t = (BigInteger) timestamp;
        } else {
            t = BigInteger.valueOf(timestamp.longValue());
        }

        return TO_NANOS.get(precision).apply(t);
    }

    /**
     * Convert {@link Instant} timestamp to a given precision.
     *
     * @param instant   Instant timestamp
     * @param precision precision
     * @return epoch timestamp in precision
     */
    public static BigInteger convert(final Instant instant, final WritePrecision precision) {
        BigInteger nanos = BigInteger.valueOf(instant.getEpochSecond())
                .multiply(NANOS_PER_SECOND)
                .add(BigInteger.valueOf(instant.getNano()));

        return FROM_NANOS.get(precision).apply(nanos);
    }

    public static BigInteger getTimestamp(@Nonnull final Object value, @Nonnull final Field schema) {
        BigInteger result = null;

        if (value instanceof Long) {
            if (schema.getFieldType().getType() instanceof ArrowType.Timestamp) {
                ArrowType.Timestamp type = (ArrowType.Timestamp) schema.getFieldType().getType();
                TimeUnit timeUnit;
                switch (type.getUnit()) {
                    case SECOND:
                        timeUnit = TimeUnit.SECONDS;
                        break;
                    case MILLISECOND:
                        timeUnit = TimeUnit.MILLISECONDS;
                        break;
                    case MICROSECOND:
                        timeUnit = TimeUnit.MICROSECONDS;
                        break;
                    default:
                    case NANOSECOND:
                        timeUnit = TimeUnit.NANOSECONDS;
                        break;
                }
                long nanoseconds = TimeUnit.NANOSECONDS.convert((Long) value, timeUnit);
                Instant instant = Instant.ofEpochSecond(0, nanoseconds);
                result = convertInstantToNano(instant, WritePrecision.NS);
            } else {
                Instant instant = Instant.ofEpochMilli((Long) value);
                result = convertInstantToNano(instant, WritePrecision.NS);
            }
        } else if (value instanceof LocalDateTime) {
            Instant instant = ((LocalDateTime) value).toInstant(ZoneOffset.UTC);
            result = convertInstantToNano(instant, WritePrecision.NS);
        }
        return result;
    }

    private static BigInteger convertInstantToNano(final Instant instant, final WritePrecision precision) {
        var writePrecision = WritePrecision.NS;
        if (precision != null) {
            writePrecision = precision;
        }
        BigInteger convertedTime = NanosecondConverter.convert(instant, writePrecision);
        return NanosecondConverter.convertToNanos(convertedTime, writePrecision);
    }
}
