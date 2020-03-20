package io.rsocket;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.util.IllegalReferenceCountException;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.collection.IntObjectMap;
import io.rsocket.fragmentation.FragmentationUtils;
import io.rsocket.frame.FrameLengthFlyweight;
import io.rsocket.frame.FrameType;
import io.rsocket.frame.RequestFireAndForgetFrameFlyweight;
import io.rsocket.internal.UnboundedProcessor;
import org.reactivestreams.Publisher;
import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;
import reactor.core.Scannable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Operators;
import reactor.util.annotation.NonNull;
import reactor.util.annotation.Nullable;

import java.time.Duration;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.function.Function;

final class UnicastFireAndForgetMono extends Mono<Void> implements Scannable {

    volatile int once;
    @SuppressWarnings("rawtypes")
    static final AtomicIntegerFieldUpdater<UnicastFireAndForgetMono> ONCE =
            AtomicIntegerFieldUpdater.newUpdater(UnicastFireAndForgetMono.class, "once");

    final ByteBufAllocator allocator;
    final Payload payload;
    final int mtu;
    final StateAware parent;
    final StreamIdSupplier streamIdSupplier;
    final IntObjectMap<?> activeStreams;
    final UnboundedProcessor<ByteBuf> sendProcessor;

    UnicastFireAndForgetMono(@NonNull ByteBufAllocator allocator, @NonNull Payload payload, int mtu, @NonNull StateAware parent, @NonNull StreamIdSupplier streamIdSupplier, @NonNull IntObjectMap<?> activeStreams, @NonNull UnboundedProcessor<ByteBuf> sendProcessor) {
        this.allocator = allocator;
        this.payload = payload;
        this.mtu = mtu;
        this.parent = parent;
        this.streamIdSupplier = streamIdSupplier;
        this.activeStreams = activeStreams;
        this.sendProcessor = sendProcessor;
    }

    @Override
    public void subscribe(CoreSubscriber<? super Void> actual) {
        final Throwable throwable = parent.checkAvailable();
        final Payload p = this.payload;

        if (throwable == null) {
            if (p.refCnt() > 0) {
                if (once == 0 && ONCE.compareAndSet(this, 0, 1)) {
                    try {
                        final boolean hasMetadata = p.hasMetadata();
                        final ByteBuf data = p.data();
                        final ByteBuf metadata = hasMetadata ? p.metadata() : null;
                        final int mtu = this.mtu;

                        if (mtu == 0 && data.readableBytes() + (hasMetadata ? metadata.readableBytes() : 0) > FrameLengthFlyweight.FRAME_LENGTH_SIZE) {
                            Operators.error(actual, new IllegalArgumentException("Too Big Payload size"));
                        } else {
                            final int streamId = this.streamIdSupplier.nextStreamId(this.activeStreams);
                            final UnboundedProcessor<ByteBuf> sender = this.sendProcessor;
                            final ByteBufAllocator allocator = this.allocator;
                            final ByteBuf slicedData = data.retainedSlice();

                            if (mtu > 0) {
                                final ByteBuf slicedMetadata = hasMetadata ? metadata.retainedSlice() : Unpooled.EMPTY_BUFFER;

                                final ByteBuf first = FragmentationUtils.encodeFirstFragment(allocator, mtu, FrameType.REQUEST_FNF, streamId, slicedMetadata, slicedData);
                                sender.onNext(first);

                                while (slicedData.isReadable() || slicedMetadata.isReadable()) {
                                    ByteBuf following = FragmentationUtils.encodeFollowsFragment(allocator, mtu, streamId, slicedMetadata, slicedData);
                                    sender.onNext(following);
                                }
                            } else {
                                final ByteBuf slicedMetadata = hasMetadata ? metadata.retainedSlice() : null;

                                final ByteBuf requestFrame =
                                        RequestFireAndForgetFrameFlyweight.encode(
                                                allocator,
                                                streamId,
                                                false,
                                                slicedMetadata,
                                                slicedData);
                                sender.onNext(requestFrame);
                            }

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

        ReferenceCountUtil.safeRelease(p);
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
            if (this.payload.refCnt() > 0) {
                try {
                    ByteBuf data = this.payload.data().retainedSlice();
                    ByteBuf metadata = this.payload.hasMetadata() ? this.payload.metadata().retainedSlice() : null;

                    int streamId = this.streamIdSupplier.nextStreamId(this.activeStreams);

                    ByteBuf requestFrame =
                            RequestFireAndForgetFrameFlyweight.encode(
                                    this.allocator,
                                    streamId,
                                    false,
                                    metadata,
                                    data);

                    this.sendProcessor.onNext(requestFrame);
                    return null;
                } finally {
                    ReferenceCountUtil.safeRelease(this.payload);
                }
            } else {
                return null;
            }
        } else {
            ReferenceCountUtil.safeRelease(this.payload);
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
        return "source(UnicastFireAndForgetMono)";
    }
}
