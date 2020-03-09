package io.rsocket;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.util.IllegalReferenceCountException;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.collection.IntObjectMap;
import io.rsocket.frame.FrameLengthFlyweight;
import io.rsocket.frame.FrameType;
import io.rsocket.frame.RequestFireAndForgetFrameFlyweight;
import io.rsocket.internal.UnboundedProcessor;
import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;
import reactor.core.Scannable;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Operators;
import reactor.util.annotation.NonNull;
import reactor.util.annotation.Nullable;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

final class UnicastFireAndForgetMono extends Mono<Void> implements Scannable {


    volatile int once;
    @SuppressWarnings("rawtypes")
    static final AtomicIntegerFieldUpdater<UnicastFireAndForgetMono> ONCE =
            AtomicIntegerFieldUpdater.newUpdater(UnicastFireAndForgetMono.class, "once");

    final ByteBufAllocator allocator;
    final Payload payload;
    final StateAware parent;
    final StreamIdSupplier streamIdSupplier;
    final IntObjectMap<?> activeStreams;
    final UnboundedProcessor<ByteBuf> sendProcessor;

    UnicastFireAndForgetMono(@NonNull ByteBufAllocator allocator, @NonNull Payload payload, @NonNull StateAware parent, @NonNull StreamIdSupplier streamIdSupplier, @NonNull IntObjectMap<?> activeStreams, @NonNull UnboundedProcessor<ByteBuf> sendProcessor) {
        this.allocator = allocator;
        this.payload = payload;
        this.parent = parent;
        this.streamIdSupplier = streamIdSupplier;
        this.activeStreams = activeStreams;
        this.sendProcessor = sendProcessor;
    }

    @Override
    public void subscribe(CoreSubscriber<? super Void> actual) {
        Throwable throwable = parent.checkAvailable();

        if (throwable == null) {
            if (payload.refCnt() > 0) {
                if (once == 0 && ONCE.compareAndSet(this, 0, 1)) {
                    try {
                        final boolean hasMetadata = payload.hasMetadata();
                        final ByteBuf data = payload.data();
                        final ByteBuf metadata = hasMetadata ? payload.metadata() : null;
                        if (data.readableBytes() + (hasMetadata ? metadata.readableBytes() : 0) > FrameLengthFlyweight.FRAME_LENGTH_SIZE) {
                            Operators.error(actual, new IllegalArgumentException("Too Big Payload size"));
                        } else {
                            final ByteBuf slicedData = data.retainedSlice();
                            final ByteBuf slicedMetadata = hasMetadata ? payload.metadata().retainedSlice() : null;
                            int streamId = streamIdSupplier.nextStreamId(activeStreams);

                            ByteBuf requestFrame =
                                    RequestFireAndForgetFrameFlyweight.encode(
                                            allocator,
                                            streamId,
                                            false,
                                            metadata,
                                            data);

                            sendProcessor.onNext(requestFrame);
                            Operators.complete(actual);
                        }
                    } catch (IllegalReferenceCountException e) {
                        Operators.error(actual, e);
                    }
                }
                else {
                    Operators.error(
                            actual, new IllegalStateException("UnicastFireAndForgetMono allows only a single Subscriber"));
                }
            } else {
                Operators.error(actual, new IllegalReferenceCountException(0));
                return;
            }
        } else {
            Operators.error(actual, throwable);
        }

        ReferenceCountUtil.safeRelease(payload);
    }

    @Override
    @Nullable
    public Void block(Duration m) {
        return block();
    }

    @Override
    @Nullable
    public Void block() {
        Throwable throwable = parent.checkAvailable();

        if (throwable == null) {
            if (payload.refCnt() > 0) {
                try {
                    ByteBuf data = payload.data().retainedSlice();
                    ByteBuf metadata = payload.hasMetadata() ? payload.metadata().retainedSlice() : null;

                    int streamId = streamIdSupplier.nextStreamId(activeStreams);

                    ByteBuf requestFrame =
                            RequestFireAndForgetFrameFlyweight.encode(
                                    allocator,
                                    streamId,
                                    false,
                                    metadata,
                                    data);

                    sendProcessor.onNext(requestFrame);
                    return null;
                } finally {
                    ReferenceCountUtil.safeRelease(payload);
                }
            } else {
                return null;
            }
        } else {
            ReferenceCountUtil.safeRelease(payload);
            throw Exceptions.propagate(throwable);
        }
    }

    @Override
    public Object scanUnsafe(Scannable.Attr key) {
        return null; // no particular key to be represented, still useful in hooks
    }

    @Override
    @NonNull
    public String stepName() {
        return "source(FireAndForgetMono)";
    }
}
