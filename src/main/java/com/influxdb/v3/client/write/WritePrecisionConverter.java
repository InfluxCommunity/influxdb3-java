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
package com.influxdb.v3.client.write;

public final class WritePrecisionConverter {

    private WritePrecisionConverter() {
    }

    public static String toV2ApiString(final WritePrecision precision) {
        switch (precision) {
            case NS:
                return "ns";
            case US:
                return "us";
            case MS:
                return "ms";
            case S:
                return "s";
            default:
                throw new IllegalArgumentException("Unsupported precision '" + precision + "'");
        }
    }

    public static String toV3ApiString(final WritePrecision precision) {
        switch (precision) {
            case NS:
                return "nanosecond";
            case US:
                return "microsecond";
            case MS:
                return "millisecond";
            case S:
                return "second";
            default:
                throw new IllegalArgumentException("Unsupported precision '" + precision + "'");
        }
    }
}