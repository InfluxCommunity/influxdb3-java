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
            return value.toString();
        }

        return (String) value;
    }
}
