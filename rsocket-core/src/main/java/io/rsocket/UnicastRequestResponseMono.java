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
import io.rsocket.frame.RequestResponseFrameFlyweight;
import io.rsocket.internal.UnboundedProcessor;
import io.rsocket.internal.UnicastMonoProcessor;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Disposable;
import reactor.core.Exceptions;
import reactor.core.Scannable;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Operators;
import reactor.util.annotation.NonNull;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

final class UnicastRequestResponseMono extends Mono<Payload>, CoreSubscriber<Payload>, Disposable, Subscription, Scannable {

    final ByteBufAllocator allocator;
    final Payload payload;
    final int mtu;
    final StateAware parent;
    final StreamIdSupplier streamIdSupplier;
    final IntObjectMap<Subscriber<? super Payload>> activeStreams;
    final UnboundedProcessor<ByteBuf> sendProcessor;

    static final int STATE_UNSUBSCRIBED = 0;
    static final int STATE_SUBSCRIBED   = 1;
    static final int STATE_REQUESTED    = 2;
    static final int STATE_TERMINATED   = 3;

    volatile int state;
    static final AtomicIntegerFieldUpdater<UnicastRequestResponseMono> STATE =
            AtomicIntegerFieldUpdater.newUpdater(UnicastRequestResponseMono.class, "state");

    int streamId = -1;
    CoreSubscriber<? super Payload> actual;

    UnicastRequestResponseMono(ByteBufAllocator allocator, Payload payload, int mtu, StateAware parent, StreamIdSupplier streamIdSupplier, IntObjectMap<Subscriber<? super Payload>> activeStreams, UnboundedProcessor<ByteBuf> sendProcessor) {
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
        int state = this.state;

        if (state == STATE_SUBSCRIBED) {
            return this.actual.currentContext();
        }

        return Context.empty();
    }

    @Override
    public final void onSubscribe(Subscription subscription) {
        subscription.request(Long.MAX_VALUE);
    }

    @Override
    public final void onComplete() {
        onNext(null);
    }

    @Override
    public final void onError(Throwable cause) {
        Objects.requireNonNull(cause, "onError cannot be null");

        if (state == STATE_TERMINATED || STATE.getAndSet(this, STATE_TERMINATED) == STATE_TERMINATED) {
            Operators.onErrorDropped(cause, currentContext());
            return;
        }


        this.actual.onError(cause);
    }

    @Override
    public final void onNext(@Nullable Payload value) {
        if (state == STATE_TERMINATED || STATE.getAndSet(this, STATE_TERMINATED) == STATE_TERMINATED) {
            ReferenceCountUtil.safeRelease(value);
            return;
        }

        final int streamId = this.streamId;
        final CoreSubscriber<? super Payload> a = this.actual;

        this.activeStreams.remove(streamId);

        if (value != null) {
            a.onNext(value);
        }
        a.onComplete();
    }

    @Override
    public void subscribe(CoreSubscriber<? super Payload> actual) {
        Objects.requireNonNull(actual, "subscribe");

        if (state == STATE_UNSUBSCRIBED && STATE.compareAndSet(this, STATE_UNSUBSCRIBED, STATE_SUBSCRIBED)) {
            this.actual = actual;
            // call onSubscribe if has value in the result or no result delivered so far
            actual.onSubscribe(this);
        } else {
            Operators.error(
                    actual,
                    new IllegalStateException("UnicastMonoProcessor allows only a single Subscriber"));
        }
    }

    @Override
    public final void request(long n) {
        if (Operators.validate(n) && state == STATE_SUBSCRIBED && STATE.compareAndSet(this, STATE_SUBSCRIBED, STATE_REQUESTED)) {
            final IntObjectMap<Subscriber<? super Payload>> as = this.activeStreams;
            final Payload p = this.payload;
            final int mtu = this.mtu;
            final UnboundedProcessor<ByteBuf> sender = this.sendProcessor;
            final ByteBufAllocator allocator = this.allocator;

            try {
                final boolean hasMetadata = p.hasMetadata();
                final ByteBuf slicedData = p.data().retainedSlice();
                final int streamId = this.streamIdSupplier.nextStreamId(as);
                this.streamId = streamId;
                as.put(streamId, this);
                if (mtu > 0) {
                    final ByteBuf slicedMetadata = hasMetadata ? p.metadata().retainedSlice() : Unpooled.EMPTY_BUFFER;

                    final ByteBuf first = FragmentationUtils.encodeFirstFragment(allocator, mtu, FrameType.REQUEST_FNF, streamId, slicedMetadata, slicedData);
                    sender.onNext(first);

                    while (slicedData.isReadable() || slicedMetadata.isReadable()) {
                        ByteBuf following = FragmentationUtils.encodeFollowsFragment(allocator, mtu, streamId, slicedMetadata, slicedData);
                        sender.onNext(following);
                    }
                } else {
                    final ByteBuf slicedMetadata = hasMetadata ? p.metadata().retainedSlice() : null;

                    final ByteBuf requestFrame =
                            RequestResponseFrameFlyweight.encode(
                                    allocator,
                                    streamId,
                                    false,
                                    slicedMetadata,
                                    slicedData);
                    sender.onNext(requestFrame);
                }
            } catch (IllegalReferenceCountException e) {
                final int streamId = this.streamId;
                if (streamId != -1) {
                    as.put(streamId, this);
                }
                this.onError(e);
            }
        }
    }

    @Override
    public final void cancel() {
        int state = this.state;
        if (state != STATE_TERMINATED && STATE.getAndSet(this, STATE_TERMINATED) != STATE_TERMINATED) {
            if (state == STATE_REQUESTED) {
                this.activeStreams.remove(this.streamId, this);
            }
        }
    }

    @Override
    public void dispose() {
        final Subscription s = UPSTREAM.getAndSet(this, Operators.cancelledSubscription());
        if (s == Operators.cancelledSubscription()) {
            return;
        }

        if (s != null) {
            s.cancel();
        }

        complete(new CancellationException("Disposed"));
    }

    /**
     * Returns the value that completed this {@link UnicastMonoProcessor}. Returns {@code null} if the
     * {@link UnicastMonoProcessor} has not been completed. If the {@link UnicastMonoProcessor} is
     * completed with an error a RuntimeException that wraps the error is thrown.
     *
     * @return the value that completed the {@link UnicastMonoProcessor}, or {@code null} if it has
     *     not been completed
     * @throws RuntimeException if the {@link UnicastMonoProcessor} was completed with an error
     */
    @Nullable
    public O peek() {
        if (isCancelled()) {
            return null;
        }

        if (value != null) {
            return value;
        }

        if (error != null) {
            RuntimeException re = Exceptions.propagate(error);
            re = Exceptions.addSuppressed(re, new Exception("Mono#peek terminated with an error"));
            throw re;
        }

        return null;
    }

    /**
     * Set the error internally, without impacting request tracking state.
     *
     * @param throwable the error.
     * @see #complete(Object)
     */
    private void setError(Throwable throwable) {
        this.error = throwable;
    }

    /**
     * Return the produced {@link Throwable} error if any or null
     *
     * @return the produced {@link Throwable} error if any or null
     */
    @Nullable
    public final Throwable getError() {
        return isDisposed() ? error : null;
    }

    /**
     * Indicates whether this {@code UnicastMonoProcessor} has been completed with an error.
     *
     * @return {@code true} if this {@code UnicastMonoProcessor} was completed with an error, {@code
     *     false} otherwise.
     */
    public final boolean isError() {
        return getError() != null;
    }

    /**
     * Indicates whether this {@code UnicastMonoProcessor} has been interrupted via cancellation.
     *
     * @return {@code true} if this {@code UnicastMonoProcessor} is cancelled, {@code false}
     *     otherwise.
     */
    public boolean isCancelled() {
        return state == CANCELLED;
    }

    public final boolean isTerminated() {
        int state = this.state;
        return (state < CANCELLED && state % 2 == 1);
    }

    @Override
    public boolean isDisposed() {
        int state = this.state;
        return state == CANCELLED || (state < CANCELLED && state % 2 == 1);
    }

    @Override
    @Nullable
    public Object scanUnsafe(Attr key) {
        // touch guard
        int state = this.state;

        if (key == Attr.TERMINATED) {
            return (state < CANCELLED && state % 2 == 1);
        }
        if (key == Attr.PARENT) {
            return subscription;
        }
        if (key == Attr.ERROR) {
            return error;
        }
        if (key == Attr.PREFETCH) {
            return Integer.MAX_VALUE;
        }
        if (key == Attr.CANCELLED) {
            return state == CANCELLED;
        }
        return null;
    }

    /**
     * Return true if any {@link Subscriber} is actively subscribed
     *
     * @return true if any {@link Subscriber} is actively subscribed
     */
    public final boolean hasDownstream() {
        return state > NO_SUBSCRIBER_HAS_RESULT && actual != null;
    }
}
