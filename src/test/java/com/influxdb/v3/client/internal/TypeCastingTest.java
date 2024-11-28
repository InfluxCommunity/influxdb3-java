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

import org.apache.arrow.vector.util.Text;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TypeCastingTest {

    @Test
    void testToLongValue() {
        Assertions.assertEquals(1, TypeCasting.toLongValue(1));
        Assertions.assertEquals(1.0, TypeCasting.toLongValue(1.23));

        Assertions.assertThrows(ClassCastException.class,
                                () -> TypeCasting.toLongValue("1"));
    }

    @Test
    void testToDoubleValue() {
        Assertions.assertEquals(1.23, TypeCasting.toDoubleValue(1.23));
        Assertions.assertEquals(1.0, TypeCasting.toDoubleValue(1));

        Assertions.assertThrows(ClassCastException.class,
                                () -> TypeCasting.toDoubleValue("1.2"));
    }

    @Test
    void testToStringValue() {
        Assertions.assertEquals("test", TypeCasting.toStringValue("test"));
        Assertions.assertEquals("test",
                                TypeCasting.toStringValue(new Text("test")));

        Assertions.assertThrows(ClassCastException.class,
                                () -> TypeCasting.toStringValue(1));
    }
}
