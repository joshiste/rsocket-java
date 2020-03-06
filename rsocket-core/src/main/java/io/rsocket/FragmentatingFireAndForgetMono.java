package io.rsocket;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.util.IllegalReferenceCountException;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.collection.IntObjectMap;
import io.rsocket.fragmentation.FragmentationUtils;
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

class FragmentatingFireAndForgetMono extends Mono<Void> implements Scannable {

    final ByteBufAllocator allocator;
    final Payload payload;
    private final int mtu;
    final StateAware parent;
    final StreamIdSupplier streamIdSupplier;
    final IntObjectMap<?> activeStreams;
    final UnboundedProcessor<ByteBuf> sendProcessor;

    FragmentatingFireAndForgetMono(
            @NonNull ByteBufAllocator allocator,
            @NonNull Payload payload,
            int mtu,
            @NonNull StateAware parent,
            @NonNull StreamIdSupplier streamIdSupplier,
            @NonNull IntObjectMap<?> activeStreams,
            @NonNull UnboundedProcessor<ByteBuf> sendProcessor) {
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
        Throwable throwable = parent.checkAvailable();

        if (throwable == null) {
            if (payload.refCnt() > 0) {
                try {
                    ByteBuf data = payload.data().retainedSlice();
                    ByteBuf metadata = payload.hasMetadata() ? payload.metadata().retainedSlice() : Unpooled.EMPTY_BUFFER;

                    int streamId = streamIdSupplier.nextStreamId(activeStreams);

                    ByteBuf first = FragmentationUtils.encodeFirstFragment(allocator, mtu, FrameType.REQUEST_FNF, streamId, metadata, data);
                    sendProcessor.onNext(first);

                    while (data.isReadable() || metadata.isReadable()) {
                        ByteBuf following = FragmentationUtils.encodeFollowsFragment(allocator, mtu, streamId, metadata, data);
                        sendProcessor.onNext(following);
                    }

                    Operators.complete(actual);
                } catch (IllegalReferenceCountException e) {
                    Operators.error(actual, e);
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

                    ByteBuf first = FragmentationUtils.encodeFirstFragment(allocator, mtu, FrameType.REQUEST_FNF, streamId, metadata, data);
                    sendProcessor.onNext(first);

                    while (data.isReadable() || metadata.isReadable()) {
                        ByteBuf following = FragmentationUtils.encodeFollowsFragment(allocator, mtu, streamId, metadata, data);
                        sendProcessor.onNext(following);
                    }
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
    public Object scanUnsafe(Attr key) {
        return null; // no particular key to be represented, still useful in hooks
    }

    @Override
    @NonNull
    public String stepName() {
        return "source(FireAndForgetMono)";
    }
}
