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

import io.grpc.Deadline;
import io.grpc.stub.AbstractStub;
import org.apache.arrow.flight.CallOption;
import org.apache.arrow.flight.CallOptions;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class GrpcCallOptionsTest {

    @Test
    void testNotSetMaxInboundMessageSize() {
        GrpcCallOptions grpcCallOptions = new GrpcCallOptions.Builder().build();
        assertNotNull(grpcCallOptions);
        assertEquals(Integer.MAX_VALUE, grpcCallOptions.getMaxInboundMessageSize());
    }

    @Test
    void testSetMaxInboundMessageSize() {
        GrpcCallOptions grpcCallOptions = new GrpcCallOptions.Builder()
                .withMaxInboundMessageSize(2000)
                .build();
        assertNotNull(grpcCallOptions);
        assertEquals(2000, grpcCallOptions.getMaxInboundMessageSize());
    }

    @Test
    void testMergeCallOptionsWithBothNonNullArrays() {
        CallOption option1 = callOption();
        CallOption option2 = callOption();
        CallOption[] baseCallOptions = {option1};
        CallOption[] additionalCallOptions = {option2};

        CallOption[] result = GrpcCallOptions.mergeCallOptions(baseCallOptions, additionalCallOptions);

        assertNotNull(result);
        assertEquals(2, result.length);
        assertEquals(option1, result[0]);
        assertEquals(option2, result[1]);
    }

    @Test
    void testMergeCallOptionsWithBaseCallOptionsNull() {
        CallOption option1 = callOption();
        CallOption option2 = callOption();
        CallOption[] baseCallOptions = null;
        CallOption[] additionalCallOptions = {option1, option2};

        CallOption[] result = GrpcCallOptions.mergeCallOptions(baseCallOptions, additionalCallOptions);

        assertNotNull(result);
        assertEquals(2, result.length);
        assertEquals(option1, result[0]);
        assertEquals(option2, result[1]);
    }

    @Test
    void testMergeCallOptionsWithAdditionalCallOptionsNull() {
        CallOption option1 = callOption();
        CallOption[] baseCallOptions = {option1};
        CallOption[] additionalCallOptions = null;

        CallOption[] result = GrpcCallOptions.mergeCallOptions(baseCallOptions, additionalCallOptions);

        assertNotNull(result);
        assertEquals(1, result.length);
        assertEquals(option1, result[0]);
    }

    @Test
    void testMergeCallOptionsWithBothArraysNull() {
        CallOption[] result = GrpcCallOptions.mergeCallOptions(null, null);

        assertNotNull(result);
        assertEquals(0, result.length);
    }

    @Test
    void testMergeCallOptionsWithEmptyArrays() {
        CallOption[] baseCallOptions = {};
        CallOption[] additionalCallOptions = {};

        CallOption[] result = GrpcCallOptions.mergeCallOptions(baseCallOptions, additionalCallOptions);

        assertNotNull(result);
        assertEquals(0, result.length);
    }

    @Test
    void grpcOptionsCloneTest(){
        GrpcCallOptions origOptions = new GrpcCallOptions.Builder()
            .withMaxInboundMessageSize(2000)
            .withMaxOutboundMessageSize(1000)
            .withWaitForReady()
            .build();

        GrpcCallOptions clonedOptions = new GrpcCallOptions.Builder()
            .fromGrpcCallOptions(origOptions)
            .build();

        Assertions.assertEquals(origOptions, clonedOptions);
        Assertions.assertNotSame(origOptions, clonedOptions);
    }

    @Test
    void grpcOptionsFromCloneWithUpdateTest(){
        GrpcCallOptions origOptions = new GrpcCallOptions.Builder()
            .withMaxInboundMessageSize(2000)
            .withMaxOutboundMessageSize(1000)
            .build();

        GrpcCallOptions copyOptions = new GrpcCallOptions.Builder()
            .fromGrpcCallOptions(origOptions)
            .withDeadline(Deadline.after(30, TimeUnit.SECONDS))
            .build();

        Assertions.assertNotNull(copyOptions.getCallOptions());
        assertEquals(3, copyOptions.getCallOptions().length);
        assertEquals(origOptions.getMaxInboundMessageSize(), copyOptions.getMaxInboundMessageSize());
        assertEquals(origOptions.getMaxOutboundMessageSize(), copyOptions.getMaxOutboundMessageSize());
        assertNotNull(copyOptions.getDeadline());
        Assertions.assertTrue(copyOptions.getDeadline().timeRemaining(TimeUnit.SECONDS) > 27);
        Assertions.assertNull(copyOptions.getExecutor());
        Assertions.assertNull(copyOptions.getWaitForReady());
        Assertions.assertNull(copyOptions.getCompressorName());
    }

    private CallOption callOption() {
        return new CallOptions.GrpcCallOption() {
            @Override
            public <T extends AbstractStub<T>> T wrapStub(final T stub) {
                return stub.withMaxInboundMessageSize(Integer.MAX_VALUE);
            }
        };
    }

    @Test
    void testEqualsWithEqualObjects() {
        GrpcCallOptions options1 = new GrpcCallOptions.Builder()
                .withMaxInboundMessageSize(2000)
                .withCompressorName("gzip")
                .build();
        GrpcCallOptions options2 = new GrpcCallOptions.Builder()
                .withMaxInboundMessageSize(2000)
                .withCompressorName("gzip")
                .build();

        assertEquals(options1, options2);
    }

    @Test
    void testEqualsWithDifferentObjects() {
        GrpcCallOptions options1 = new GrpcCallOptions.Builder()
                .withMaxInboundMessageSize(2000)
                .withCompressorName("gzip")
                .build();
        GrpcCallOptions options2 = new GrpcCallOptions.Builder()
                .withMaxInboundMessageSize(1000)
                .withCompressorName("deflate")
                .build();

        assertNotEquals(options1, options2);
    }

    @Test
    void testEqualsWithNullAndDifferentClass() {
        GrpcCallOptions options = new GrpcCallOptions.Builder()
                .withMaxInboundMessageSize(2000)
                .build();

        assertNotEquals(null, options);
        assertNotEquals(1, options);
    }

    @Test
    void testHashCodeWithEqualObjects() {
        GrpcCallOptions options1 = new GrpcCallOptions.Builder()
                .withMaxInboundMessageSize(2000)
                .withCompressorName("gzip")
                .build();
        GrpcCallOptions options2 = new GrpcCallOptions.Builder()
                .withMaxInboundMessageSize(2000)
                .withCompressorName("gzip")
                .build();

        assertEquals(options1.hashCode(), options2.hashCode());
    }

    @Test
    void testHashCodeWithDifferentObjects() {
        GrpcCallOptions options1 = new GrpcCallOptions.Builder()
                .withMaxInboundMessageSize(2000)
                .withCompressorName("gzip")
                .build();
        GrpcCallOptions options2 = new GrpcCallOptions.Builder()
                .withMaxInboundMessageSize(1000)
                .withCompressorName("deflate")
                .build();

        assertNotEquals(options1.hashCode(), options2.hashCode());
    }

    @Test
    void testToString() {
        GrpcCallOptions options = new GrpcCallOptions.Builder()
                .withMaxInboundMessageSize(2000)
                .withMaxOutboundMessageSize(5000)
                .withCompressorName("gzip")
                .build();

        String expected = "GrpcCallOptions{deadline=null, "
                + "executor=null, "
                + "compressorName='gzip', "
                + "waitForReady=null, "
                + "maxInboundMessageSize=2000, "
                + "maxOutboundMessageSize=5000}";
        assertEquals(expected, options.toString());
    }
}
