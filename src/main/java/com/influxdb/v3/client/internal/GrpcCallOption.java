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

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Executor;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import io.grpc.CompressorRegistry;
import io.grpc.Deadline;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.AbstractStub;
import org.apache.arrow.flight.CallOption;

/**
 * The collection of runtime options for a new RPC call.
 */
public final class GrpcCallOption {

    private final Deadline deadline;
    private final Executor executor;
    private final String compressorName;
    private final Boolean waitForReady;
    private final Integer maxInboundMessageSize;
    private final Integer maxOutboundMessageSize;
    private final CallOption[] callOptionCallback;

    private GrpcCallOption(@Nonnull final Builder builder) {
        this.deadline = builder.deadline;
        this.executor = builder.executor;
        this.compressorName = builder.compressorName;
        this.waitForReady = builder.waitForReady;
        this.maxInboundMessageSize = builder.maxInboundMessageSize;
        this.maxOutboundMessageSize = builder.maxOutboundMessageSize;
        this.callOptionCallback = builder.callOptions.toArray(new CallOption[0]);
    }

    /**
     * Returns the absolute deadline for a call.
     *
     * @return the Deadline object
     */
    @Nullable
    public Deadline getDeadline() {
        return deadline;
    }

    /**
     * Returns the Executor to be used instead of default.
     *
     * @return the Executor
     */
    @Nullable
    public Executor getExecutor() {
        return executor;
    }

    /**
     * Returns the compressor's name.
     *
     * @return the compressor's name
     */
    @Nullable
    public String getCompressorName() {
        return compressorName;
    }

    /**
     * Returns the wait for ready flag.
     *
     * @return the wait for ready flag
     */
    @Nullable
    public Boolean getWaitForReady() {
        return waitForReady;
    }

    /**
     * Returns the maximum allowed message size acceptable from the remote peer.
     *
     * @return the maximum message size receive allowed
     */
    @Nullable
    public Integer getMaxInboundMessageSize() {
        return maxInboundMessageSize;
    }

    /**
     * Returns the maximum allowed message size acceptable to send the remote peer.
     *
     * @return the maximum message size send allowed
     */
    @Nullable
    public Integer getMaxOutboundMessageSize() {
        return maxOutboundMessageSize;
    }

    /**
     * Get the CallOption callback list which is use when setting
     * the grpc CallOption.
     *
     * @return the CallOption list
     */
    @Nonnull
    public CallOption[] getCallOptionCallback() {
        return callOptionCallback;
    }

    @Override
    public boolean equals(final Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        GrpcCallOption that = (GrpcCallOption) o;
        return Objects.equals(deadline, that.deadline)
                && Objects.equals(executor, that.executor)
                && Objects.equals(compressorName, that.compressorName)
                && Objects.equals(waitForReady, that.waitForReady)
                && Objects.equals(maxInboundMessageSize, that.maxInboundMessageSize)
                && Objects.equals(maxOutboundMessageSize, that.maxOutboundMessageSize);
    }

    @Override
    public int hashCode() {
        return Objects.hash(deadline,
                executor,
                compressorName,
                waitForReady,
                maxInboundMessageSize,
                maxOutboundMessageSize
        );
    }

    @Override
    public String toString() {
        return "GrpcCallOption{"
                + "deadline=" + deadline
                + ", executor=" + executor
                + ", compressorName='" + compressorName
                + '\''
                + ", waitForReady=" + waitForReady
                + ", maxInboundMessageSize=" + maxInboundMessageSize
                + ", maxOutboundMessageSize=" + maxOutboundMessageSize
                + '}';
    }

    public static final class Builder {
        private Deadline deadline;
        private Executor executor;
        private String compressorName;
        private Boolean waitForReady;
        private Integer maxInboundMessageSize;
        private Integer maxOutboundMessageSize;
        private final List<CallOption> callOptions = new ArrayList<>();

        /**
         * Sets the absolute deadline for a rpc call.
         * @param deadline The deadline
         * @return this
         */
        public Builder withDeadline(final @Nonnull Deadline deadline) {
            var callOption = new org.apache.arrow.flight.CallOptions.GrpcCallOption() {
                @Override
                public <T extends AbstractStub<T>> T wrapStub(final T stub) {
                    return stub.withDeadline(deadline);
                }
            };
            this.deadline = deadline;
            callOptions.add(callOption);
            return this;
        }

        /**
         * Sets an {@code executor} to be used instead of the default
         * executor specified with {@link ManagedChannelBuilder#executor}.
         * @param executor The executor
         * @return this
         */
        public Builder withExecutor(@Nonnull final Executor executor) {
            var callOption = new org.apache.arrow.flight.CallOptions.GrpcCallOption() {
                @Override
                public <T extends AbstractStub<T>> T wrapStub(final T stub) {
                    return stub.withExecutor(executor);
                }
            };
            this.executor = executor;
            callOptions.add(callOption);
            return this;
        }

        /**
         * Sets the compression to use for the call.  The compressor must be a valid name known in the
         * {@link CompressorRegistry}.  By default, the "gzip" compressor will be available.
         *
         * <p>It is only safe to call this if the server supports the compression format chosen. There is
         * no negotiation performed; if the server does not support the compression chosen, the call will
         * fail.
         * @param compressorName The compressor name
         * @return this
         */
        public Builder withCompressorName(@Nonnull final String compressorName) {
            var callOption = new org.apache.arrow.flight.CallOptions.GrpcCallOption() {
                @Override
                public <T extends AbstractStub<T>> T wrapStub(final T stub) {
                    return stub.withCompression(compressorName);
                }
            };
            this.compressorName = compressorName;
            callOptions.add(callOption);
            return this;
        }

        /**
         * Enables <a href="https://github.com/grpc/grpc/blob/master/doc/wait-for-ready.md">
         * 'wait for ready'</a> for the call. Wait-for-ready queues the RPC until a connection is
         * available. This may dramatically increase the latency of the RPC, but avoids failing
         * "unnecessarily." The default queues the RPC until an attempt to connect has completed, but
         * fails RPCs without sending them if unable to connect.
         * @return this
         */
        public Builder withWaitForReady() {
            var callOption = new org.apache.arrow.flight.CallOptions.GrpcCallOption() {
                @Override
                public <T extends AbstractStub<T>> T wrapStub(final T stub) {
                    return stub.withWaitForReady();
                }
            };
            this.waitForReady = true;
            callOptions.add(callOption);
            return this;
        }

        /**
         * Sets the maximum allowed message size acceptable from the remote peer.  If unset, this will
         * default to the value set on the {@link ManagedChannelBuilder#maxInboundMessageSize(int)}.
         * @param maxInboundMessageSize The max receive message size
         * @return this
         */
        public Builder withMaxInboundMessageSize(@Nonnull final Integer maxInboundMessageSize) {
            var callOption = new org.apache.arrow.flight.CallOptions.GrpcCallOption() {
                @Override
                public <T extends AbstractStub<T>> T wrapStub(final T stub) {
                    return stub.withMaxInboundMessageSize(maxInboundMessageSize);
                }
            };
            this.maxInboundMessageSize = maxInboundMessageSize;
            callOptions.add(callOption);
            return this;
        }

        /**
         * Sets the maximum allowed message size acceptable sent to the remote peer.
         * @param maxOutboundMessageSize The maximum message send size
         * @return this
         */
        public Builder withMaxOutboundMessageSize(@Nonnull final Integer maxOutboundMessageSize) {
            var callOption = new org.apache.arrow.flight.CallOptions.GrpcCallOption() {
                @Override
                public <T extends AbstractStub<T>> T wrapStub(final T stub) {
                    return stub.withMaxOutboundMessageSize(maxOutboundMessageSize);
                }
            };
            this.maxOutboundMessageSize = maxOutboundMessageSize;
            callOptions.add(callOption);
            return this;
        }

        /**
         * Build an instance of GrpcCallOption.
         *
         * @return the GrpcCallOption instance
         */
        public GrpcCallOption build() {
            return new GrpcCallOption(this);
        }
    }
}
