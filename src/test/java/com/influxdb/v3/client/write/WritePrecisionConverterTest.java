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

import java.util.Map;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class WritePrecisionConverterTest {
    @Test
    void toV2ApiString() {
        Map<WritePrecision, String> testCases = Map.of(
                WritePrecision.NS, "ns",
                WritePrecision.US, "us",
                WritePrecision.MS, "ms",
                WritePrecision.S, "s"
        );

        for (Map.Entry<WritePrecision, String> e : testCases.entrySet()) {
            WritePrecision precision = e.getKey();
            String expectedString = e.getValue();
            String result = WritePrecisionConverter.toV2ApiString(precision);
            assertEquals(expectedString, result, "Failed for precision: " + precision);
        }
    }

    @Test
    void toV3ApiString() {
        Map<WritePrecision, String> tc = Map.of(
                WritePrecision.NS, "nanosecond",
                WritePrecision.US, "microsecond",
                WritePrecision.MS, "millisecond",
                WritePrecision.S, "second"
        );

        for (Map.Entry<WritePrecision, String> e : tc.entrySet()) {
            WritePrecision precision = e.getKey();
            String expectedString = e.getValue();
            String result = WritePrecisionConverter.toV3ApiString(precision);
            assertEquals(expectedString, result, "Failed for precision: " + precision);
        }
    }
}
