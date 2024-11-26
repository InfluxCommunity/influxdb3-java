package com.influxdb.v3.client.internal;

import javax.annotation.Nonnull;

import org.apache.arrow.vector.util.Text;

/**
 * Functions for safe type casting.
 */
public final class TypeCasting {

    private TypeCasting() { }

    /**
     * Safe casting to long value.
     *
     * @param value object to cast
     * @return long value
     */
    public static long toLongValue(@Nonnull final Object value) {

        if (long.class.isAssignableFrom(value.getClass())
                || Long.class.isAssignableFrom(value.getClass())) {
            return (long) value;
        }

        return ((Number) value).longValue();
    }

    /**
     * Safe casting to double value.
     *
     * @param value object to cast
     * @return double value
     */
    public static double toDoubleValue(@Nonnull final Object value) {

        if (double.class.isAssignableFrom(value.getClass())
                || Double.class.isAssignableFrom(value.getClass())) {
            return (double) value;
        }

        return ((Number) value).doubleValue();
    }

    /**
     * Safe casting to string value.
     *
     * @param value object to cast
     * @return string value
     */
    public static String toStringValue(@Nonnull final Object value) {

        if (Text.class.isAssignableFrom(value.getClass())) {
            return ((Text) value).toString();
        }

        return (String) value;
    }
}
