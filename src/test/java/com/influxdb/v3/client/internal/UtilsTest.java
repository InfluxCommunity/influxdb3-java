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

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

class UtilsTest {

    @ParameterizedTest
    @ValueSource(strings = {
            "0",
            "1",
            "123",
            "+123",
            "-1",
            "-123",
            "2147483647",
            "-2147483648"
    })
    void isIntegerValid(final String value) {
        Assertions.assertThat(Utils.isInteger(value)).isTrue();
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = {
            " ",
            "   ",
            "\t",
            "\n",
            "abc",
            "12a",
            "a12",
            "1 2",
            "1.0",
            "-1.5",
            "1e5",
            "2147483648",
            "-2147483649",
            "99999999999999999999"
    })
    void isIntegerInvalid(final String value) {
        Assertions.assertThat(Utils.isInteger(value)).isFalse();
    }

    @Test
    void testEmpty() {
        Assertions.assertThat(Utils.isInteger("")).isFalse();
    }
}
