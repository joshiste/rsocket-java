package io.rsocket;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.util.IllegalReferenceCountException;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.collection.IntObjectMap;
import io.rsocket.fragmentation.FragmentationUtils;
import io.rsocket.frame.CancelFrameFlyweight;
import io.rsocket.frame.FrameType;
import io.rsocket.frame.RequestNFrameFlyweight;
import io.rsocket.frame.RequestResponseFrameFlyweight;
import io.rsocket.frame.RequestStreamFrameFlyweight;
import io.rsocket.internal.UnboundedProcessor;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;
import reactor.core.Scannable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Operators;
import reactor.util.annotation.NonNull;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

final class UnicastRequestStreamFlux extends Flux<Payload> implements CoreSubscriber<Payload>, Subscription, Scannable {

    final ByteBufAllocator allocator;
    final Payload payload;
    final int mtu;
    final StateAware parent;
    final StreamIdSupplier streamIdSupplier;
    final IntObjectMap<Subscriber<? super Payload>> activeStreams;
    final UnboundedProcessor<ByteBuf> sendProcessor;

    static final long STATE_UNSUBSCRIBED = -2;
    static final long STATE_SUBSCRIBED = -1;
    static final long STATE_WAS_REQUESTED_MAX = -3;
    static final long STATE_TERMINATED = Long.MIN_VALUE;

    volatile long requested;
    static final AtomicLongFieldUpdater<UnicastRequestStreamFlux> REQUESTED =
            AtomicLongFieldUpdater.newUpdater(UnicastRequestStreamFlux.class, "requested");

    int streamId;
    CoreSubscriber<? super Payload> actual;

    UnicastRequestStreamFlux(ByteBufAllocator allocator, Payload payload, int mtu, StateAware parent, StreamIdSupplier streamIdSupplier, IntObjectMap<Subscriber<? super Payload>> activeStreams, UnboundedProcessor<ByteBuf> sendProcessor) {
        this.allocator = allocator;
        this.payload = payload;
        this.mtu = mtu;
        this.parent = parent;
        this.streamIdSupplier = streamIdSupplier;
        this.activeStreams = activeStreams;
        this.sendProcessor = sendProcessor;
    }

    @Override
    @NonNull
    public Context currentContext() {
        long state = this.requested;

        if (state == STATE_UNSUBSCRIBED) {
            return Context.empty();
        }

        return this.actual.currentContext();
    }

    @Override
    public final void onSubscribe(Subscription subscription) {
        subscription.cancel();
        // TODO: Add logging
    }

    @Override
    public final void onComplete() {
        if (this.requested == STATE_TERMINATED) {
            return;
        }

        this.activeStreams.remove(this.streamId, this);

        this.actual.onComplete();
    }

    @Override
    public final void onError(Throwable cause) {
        Objects.requireNonNull(cause, "onError cannot be null");

        if (this.requested == STATE_TERMINATED) {
            Operators.onErrorDropped(cause, currentContext());
            return;
        }

        this.activeStreams.remove(this.streamId, this);

        this.actual.onError(cause);
    }

    @Override
    public final void onNext(Payload value) {
        if (this.requested == STATE_TERMINATED) {
            ReferenceCountUtil.safeRelease(value);
            return;
        }

        this.actual.onNext(value);
    }

    @Override
    public void subscribe(CoreSubscriber<? super Payload> actual) {
        Objects.requireNonNull(actual, "subscribe");

        if (this.requested == STATE_UNSUBSCRIBED && REQUESTED.compareAndSet(this, STATE_UNSUBSCRIBED, STATE_SUBSCRIBED)) {
            this.actual = actual;
            // call onSubscribe if has value in the result or no result delivered so far
            actual.onSubscribe(this);
        } else {
            Operators.error(
                    actual,
                    new IllegalStateException("UnicastRequestStreamFlux allows only a single Subscriber"));
        }
    }

    @Override
    public final void request(long n) {
        if (!Operators.validate(n)) {
            return;
        }

        long currentRequested;
        long nextRequested;
        for (;;) {
            currentRequested = this.requested;

            if (currentRequested == STATE_WAS_REQUESTED_MAX) {
                return;
            }

            if (currentRequested == Long.MAX_VALUE) {
                return;
            }

            if (currentRequested == STATE_TERMINATED) {
                return;
            }

            if (currentRequested == STATE_SUBSCRIBED) {
                nextRequested = n;
            } else {
                nextRequested = Operators.addCap(currentRequested, n);
            }

            if (REQUESTED.compareAndSet(this, currentRequested, nextRequested)) {
                break;
            }
        }

        if (currentRequested > 0) {
            return;
        }

        final UnboundedProcessor<ByteBuf> sender = this.sendProcessor;
        final ByteBufAllocator allocator = this.allocator;

        int streamId = this.streamId;

        for (;;) {
            if (currentRequested == STATE_SUBSCRIBED) {
                final IntObjectMap<Subscriber<? super Payload>> as = this.activeStreams;
                final Payload p = this.payload;
                final int mtu = this.mtu;

                try {
                    final boolean hasMetadata = p.hasMetadata();
                    final ByteBuf slicedData = p.data().retainedSlice();

                    streamId = this.streamIdSupplier.nextStreamId(as);
                    this.streamId = streamId;

                    if (mtu > 0) {
                        final ByteBuf slicedMetadata = hasMetadata ? p.metadata().retainedSlice() : Unpooled.EMPTY_BUFFER;

                        final ByteBuf first = FragmentationUtils.encodeFirstFragment(allocator, mtu, (int) (nextRequested & Integer.MAX_VALUE), FrameType.REQUEST_STREAM, streamId, slicedMetadata, slicedData);

                        as.put(streamId, this);
                        sender.onNext(first);

                        while (slicedData.isReadable() || slicedMetadata.isReadable()) {
                            final ByteBuf following = FragmentationUtils.encodeFollowsFragment(allocator, mtu, streamId, slicedMetadata, slicedData);
                            sender.onNext(following);
                        }
                    } else {
                        final ByteBuf slicedMetadata = hasMetadata ? p.metadata().retainedSlice() : null;

                        final ByteBuf requestFrame =
                                RequestStreamFrameFlyweight.encode(
                                        allocator,
                                        streamId,
                                        false,
                                        n,
                                        slicedMetadata,
                                        slicedData);

                        as.put(streamId, this);
                        sender.onNext(requestFrame);
                    }

                    ReferenceCountUtil.safeRelease(p);
                } catch (Throwable e) {
                    ReferenceCountUtil.safeRelease(p);

                    Exceptions.throwIfFatal(e);

                    if (as.remove(streamId, this)) {
                        final ByteBuf cancelFrame = CancelFrameFlyweight.encode(allocator, streamId);
                        sender.onNext(cancelFrame);
                    }

                    this.actual.onError(e);

                    return;
                }
            } else {
                final ByteBuf requestNFrame = RequestNFrameFlyweight.encode(allocator, streamId, nextRequested);
                sender.onNext(requestNFrame);
            }

            for (;;) {
                long toUpdate;
                // now currentRequested is newer than nextRequested
                currentRequested = this.requested;

                if (currentRequested == STATE_TERMINATED) {
                    // we should not override terminal state
                    nextRequested = STATE_TERMINATED;
                    break;
                }

                if (nextRequested == Long.MAX_VALUE) {
                    // we should state that it max value has already been requested, so no need to loop anymore
                    toUpdate = STATE_WAS_REQUESTED_MAX;
                } else {
                    // subtract the requestN from the latest requested state
                    toUpdate = currentRequested - nextRequested;
                }

                if (REQUESTED.compareAndSet(this, nextRequested, toUpdate)) {
                    nextRequested = toUpdate;
                    break;
                }
            }

            if (nextRequested == STATE_WAS_REQUESTED_MAX) {
                return;
            }

            if (nextRequested == STATE_TERMINATED) {
                final ByteBuf cancelFrame = CancelFrameFlyweight.encode(allocator, streamId);
                sender.onNext(cancelFrame);

                return;
            }

            if (nextRequested == 0) {
                return;
            }
        }
    }

    @Override
    public final void cancel() {
        final long state = REQUESTED.getAndSet(this, STATE_TERMINATED);
        if (state != STATE_TERMINATED && state <= 0) {
            if (state == STATE_SUBSCRIBED) {
                // no need to send anything, since the first request has not happened
                ReferenceCountUtil.safeRelease(this.payload);
            } else {
                final int streamId = this.streamId;
                final ByteBuf cancelFrame = CancelFrameFlyweight.encode(this.allocator, streamId);

                this.activeStreams.remove(streamId, this);
                this.sendProcessor.onNext(cancelFrame);
            }
        }
    }

    @Override
    @Nullable
    public Object scanUnsafe(Attr key) {
        // touch guard
        long state = this.requested;

        if (key == Attr.TERMINATED) return state == STATE_TERMINATED;
        if (key == Attr.REQUESTED_FROM_DOWNSTREAM) return state;

        return null;
    }

    @Override
    @NonNull
    public String stepName() {
        return "source(UnicastRequestStreamFlux)";
    }
}
