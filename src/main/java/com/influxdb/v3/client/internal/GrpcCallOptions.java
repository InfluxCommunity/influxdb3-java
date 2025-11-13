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

import java.time.Duration;
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
public final class GrpcCallOptions {

    private final Deadline deadline;
    private final Executor executor;
    private final String compressorName;
    private final Boolean waitForReady;
    private final Integer maxInboundMessageSize;
    private final Integer maxOutboundMessageSize;
    private final CallOption[] callOptions;

    private GrpcCallOptions(@Nonnull final Builder builder) {
        this.deadline = builder.deadline;
        this.executor = builder.executor;
        this.compressorName = builder.compressorName;
        this.waitForReady = builder.waitForReady;
        this.maxInboundMessageSize = builder.maxInboundMessageSize;
        this.maxOutboundMessageSize = builder.maxOutboundMessageSize;
        this.callOptions = builder.callOptions.toArray(new CallOption[0]);
    }


    /**
     * Creates a default instance of {@link GrpcCallOptions} with predefined settings.
     * <p>
     * The default configuration includes:
     * <ul>
     *   <li>Maximum inbound message size set to {@link Integer#MAX_VALUE}.</li>
     * </ul>
     * Other options can be customized using the {@link GrpcCallOptions.Builder}.
     *
     * @return the default configuration of {@link GrpcCallOptions}.
     */
    @Nonnull
    public static GrpcCallOptions getDefaultOptions() {
        GrpcCallOptions.Builder builder = new GrpcCallOptions.Builder();
        return builder.build();
    }

    /**
     * Merges two arrays of {@link CallOption} into a single array. The method combines the elements
     * from the baseCallOptions array and the additional callOptions array. If either of the input
     * arrays is null, it will be treated as an empty array.
     *
     * @param baseCallOptions the base array of {@link CallOption} instances, may be null
     * @param callOptions additional {@link CallOption} instances to be added, may also be null
     * @return a combined array containing all {@link CallOption} instances from both input arrays
     */
    public static CallOption[] mergeCallOptions(@Nullable final CallOption[] baseCallOptions,
                                                final CallOption... callOptions) {
        CallOption[] base = baseCallOptions != null ? baseCallOptions : new CallOption[0];
        CallOption[] additional = callOptions != null ? callOptions : new CallOption[0];
        CallOption[] merged = new CallOption[base.length + additional.length];
        System.arraycopy(base, 0, merged, 0, base.length);
        System.arraycopy(additional, 0, merged, base.length, additional.length);
        return merged;
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
    public CallOption[] getCallOptions() {
        return callOptions;
    }

    @Override
    public boolean equals(final Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        GrpcCallOptions that = (GrpcCallOptions) o;
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
        return "GrpcCallOptions{"
                + "deadline=" + deadline
                + ", executor=" + executor
                + ", compressorName='" + compressorName
                + '\''
                + ", waitForReady=" + waitForReady
                + ", maxInboundMessageSize=" + maxInboundMessageSize
                + ", maxOutboundMessageSize=" + maxOutboundMessageSize
                + '}';
    }

    /**
     * Builder for GrpcCallOption.
     */
    public static final class Builder {
        private Deadline deadline;
        private Executor executor;
        private String compressorName;
        private Boolean waitForReady;
        private Integer maxInboundMessageSize;
        private Integer maxOutboundMessageSize;
        private final List<CallOption> callOptions = new ArrayList<>();

        /**
         * Constructs a new instance of the Builder with default values.
         * By default, the maximum inbound message size is set to the largest possible value.
         */
        public Builder() {
            this.maxInboundMessageSize = Integer.MAX_VALUE;
        }

        /**
         * Sets the absolute deadline for a rpc call.
         *
         * <p><i>Please note</i> the preferred approach is to set a <code>queryTimeout</code>
         * Duration value globally in the ClientConfig
         * ({@link com.influxdb.v3.client.config.ClientConfig.Builder#queryTimeout(Duration)}).
         * This value will then be used to calculate a new Deadline with each call.</p>
         *
         * @param deadline The deadline
         * @return this
         */
        public Builder withDeadline(final @Nonnull Deadline deadline) {
            this.deadline = deadline;
            return this;
        }

        /**
         * Unsets absolute deadline.  Note deadline may have been set
         * via {@link #fromGrpcCallOptions(GrpcCallOptions)} method.
         *
         * @return this
         */
        public Builder withoutDeadline() {
            this.deadline = null;
            return this;
        }

        /**
         * Sets an {@code executor} to be used instead of the default
         * executor specified with {@link ManagedChannelBuilder#executor}.
         *
         * @param executor The executor
         * @return this
         */
        public Builder withExecutor(@Nonnull final Executor executor) {
            this.executor = executor;
            return this;
        }

        /**
         * Sets the compression to use for the call.  The compressor must be a valid name known in the
         * {@link CompressorRegistry}.  By default, the "gzip" compressor will be available.
         *
         * <p>It is only safe to call this if the server supports the compression format chosen. There is
         * no negotiation performed; if the server does not support the compression chosen, the call will
         * fail.
         *
         * @param compressorName The compressor name
         * @return this
         */
        public Builder withCompressorName(@Nonnull final String compressorName) {
            this.compressorName = compressorName;
            return this;
        }

        /**
         * Enables <a href="https://github.com/grpc/grpc/blob/master/doc/wait-for-ready.md">
         * 'wait for ready'</a> for the call. Wait-for-ready queues the RPC until a connection is
         * available. This may dramatically increase the latency of the RPC, but avoids failing
         * "unnecessarily." The default queues the RPC until an attempt to connect has completed, but
         * fails RPCs without sending them if unable to connect.
         *
         * @return this
         */
        public Builder withWaitForReady() {
            this.waitForReady = true;
            return this;
        }

        /**
         * Sets the maximum allowed message size acceptable from the remote peer.  If unset, this will
         * default to the value set on the {@link ManagedChannelBuilder#maxInboundMessageSize(int)}.
         *
         * @param maxInboundMessageSize The max receive message size
         * @return this
         */
        public Builder withMaxInboundMessageSize(@Nonnull final Integer maxInboundMessageSize) {
            this.maxInboundMessageSize = maxInboundMessageSize;
            return this;
        }

        /**
         * Helper method to clone already existing gRPC options.
         *
         * @param grpcCallOptions = options to copy
         * @return this
         */
        public Builder fromGrpcCallOptions(@Nonnull final GrpcCallOptions grpcCallOptions) {
            if (grpcCallOptions.getDeadline() != null) {
                this.deadline = grpcCallOptions.getDeadline();
            }
            if (grpcCallOptions.getExecutor() != null) {
                this.executor = grpcCallOptions.getExecutor();
            }
            if (grpcCallOptions.getCompressorName() != null) {
                this.compressorName = grpcCallOptions.getCompressorName();
            }
            if (grpcCallOptions.getWaitForReady() != null) {
                this.waitForReady = grpcCallOptions.getWaitForReady();
            }
            if (grpcCallOptions.getMaxInboundMessageSize() != null) {
                this.maxInboundMessageSize = grpcCallOptions.getMaxInboundMessageSize();
            }
            if (grpcCallOptions.getMaxOutboundMessageSize() != null) {
                this.maxOutboundMessageSize = grpcCallOptions.getMaxOutboundMessageSize();
            }
            return this;
        }

        /**
         * Sets the maximum allowed message size acceptable sent to the remote peer.
         * <p>
         * Note: this property leads to grpc-java issue 12109 and can lead to the connection hanging indefinitely.
         * See (<a href="https://github.com/grpc/grpc-java/issues/12109">grpc-java 12109</a>)
         *
         * @param maxOutboundMessageSize The maximum message send size
         * @return this
         */
        public Builder withMaxOutboundMessageSize(@Nonnull final Integer maxOutboundMessageSize) {
            // TODO remove warning about issue 12109 in javadoc above,
            //  once 12109 is resolved and dependencies are updated.
            this.maxOutboundMessageSize = maxOutboundMessageSize;
            return this;
        }

        private org.apache.arrow.flight.CallOptions.GrpcCallOption createDeadlineCallOption(
                final Deadline deadline) {
            return new org.apache.arrow.flight.CallOptions.GrpcCallOption() {
                @Override
                public <T extends AbstractStub<T>> T wrapStub(final T stub) {
                    return stub.withDeadline(deadline);
                }
            };
        }

        private org.apache.arrow.flight.CallOptions.GrpcCallOption createExecutorCallOption(
                final Executor executor) {
            return new org.apache.arrow.flight.CallOptions.GrpcCallOption() {
                @Override
                public <T extends AbstractStub<T>> T wrapStub(final T stub) {
                    return stub.withExecutor(executor);
                }
            };
        }

        private org.apache.arrow.flight.CallOptions.GrpcCallOption createCompressionCallOption(
                final String compressorName) {
            return new org.apache.arrow.flight.CallOptions.GrpcCallOption() {
                @Override
                public <T extends AbstractStub<T>> T wrapStub(final T stub) {
                    return stub.withCompression(compressorName);
                }
            };
        }

        private org.apache.arrow.flight.CallOptions.GrpcCallOption createWaitForReadyCallOption() {
            return new org.apache.arrow.flight.CallOptions.GrpcCallOption() {
                @Override
                public <T extends AbstractStub<T>> T wrapStub(final T stub) {
                    return stub.withWaitForReady();
                }
            };
        }

        private org.apache.arrow.flight.CallOptions.GrpcCallOption createMaxInboundMessageSizeCallOption(
                final Integer maxInboundMessageSize) {
            return new org.apache.arrow.flight.CallOptions.GrpcCallOption() {
                @Override
                public <T extends AbstractStub<T>> T wrapStub(final T stub) {
                    return stub.withMaxInboundMessageSize(maxInboundMessageSize);
                }
            };
        }

        private org.apache.arrow.flight.CallOptions.GrpcCallOption createMaxOutboundMessageSizeCallOption(
                final Integer maxOutboundMessageSize) {
            return new org.apache.arrow.flight.CallOptions.GrpcCallOption() {
                @Override
                public <T extends AbstractStub<T>> T wrapStub(final T stub) {
                    return stub.withMaxOutboundMessageSize(maxOutboundMessageSize);
                }
            };
        }

        /**
         * Build an instance of GrpcCallOptions.
         *
         * @return the GrpcCallOptions instance
         */
        public GrpcCallOptions build() {
            if (deadline != null) {
                var callOption = createDeadlineCallOption(deadline);
                callOptions.add(callOption);
            }

            if (executor != null) {
                var callOption = createExecutorCallOption(executor);
                callOptions.add(callOption);
            }

            if (compressorName != null) {
                var callOption = createCompressionCallOption(compressorName);
                callOptions.add(callOption);
            }

            if (waitForReady != null) {
                var callOption = createWaitForReadyCallOption();
                callOptions.add(callOption);
            }

            if (maxInboundMessageSize != null) {
                var callOption = createMaxInboundMessageSizeCallOption(maxInboundMessageSize);
                callOptions.add(callOption);
            }

            if (maxOutboundMessageSize != null) {
                var callOption = createMaxOutboundMessageSizeCallOption(maxOutboundMessageSize);
                callOptions.add(callOption);
            }

            return new GrpcCallOptions(this);
        }
    }
}
