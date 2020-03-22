package io.rsocket;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.util.IllegalReferenceCountException;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.collection.IntObjectMap;
import io.rsocket.fragmentation.FragmentationUtils;
import io.rsocket.frame.CancelFrameFlyweight;
import io.rsocket.frame.ErrorFrameFlyweight;
import io.rsocket.frame.FrameLengthFlyweight;
import io.rsocket.frame.FrameType;
import io.rsocket.frame.PayloadFrameFlyweight;
import io.rsocket.frame.RequestChannelFrameFlyweight;
import io.rsocket.frame.RequestNFrameFlyweight;
import io.rsocket.internal.UnboundedProcessor;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;
import reactor.core.Scannable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Operators;
import reactor.util.annotation.NonNull;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

final class UnicastRequestChannelFlux extends Flux<Payload>
    implements CoreSubscriber<Payload>, Subscription, Scannable {

  final ByteBufAllocator allocator;
  final int mtu;
  final Flux<Payload> payloadPublisher;
  final StateAware parent;
  final StreamIdSupplier streamIdSupplier;
  final IntObjectMap<CoreSubscriber<? super Payload>> activeStreams;
  final IntObjectMap<Subscription> activeSubscriptions;
  final UnboundedProcessor<ByteBuf> sendProcessor;

  static final long STATE_UNSUBSCRIBED = -2;
  static final long STATE_SUBSCRIBED = -1;
  static final long STATE_WIRED = -3;
  static final long STATE_TERMINATED = Long.MIN_VALUE;

  volatile long requested;
  static final AtomicLongFieldUpdater<UnicastRequestChannelFlux> REQUESTED =
      AtomicLongFieldUpdater.newUpdater(UnicastRequestChannelFlux.class, "requested");

  int streamId;
  InnerConnectorSubscriber connector;

  boolean first = true;

  UnicastRequestChannelFlux(
      ByteBufAllocator allocator,
      Flux<Payload> payloadPublisher,
      int mtu,
      StateAware parent,
      StreamIdSupplier streamIdSupplier,
      IntObjectMap<CoreSubscriber<? super Payload>> activeSubscribers,
      IntObjectMap<Subscription> activeSubscriptions,
      UnboundedProcessor<ByteBuf> sendProcessor) {
    this.allocator = allocator;
    this.payloadPublisher = payloadPublisher;
    this.mtu = mtu;
    this.parent = parent;
    this.streamIdSupplier = streamIdSupplier;
    this.activeStreams = activeSubscribers;
    this.activeSubscriptions = activeSubscriptions;
    this.sendProcessor = sendProcessor;

    REQUESTED.lazySet(this, STATE_UNSUBSCRIBED);
  }

  @Override
  @NonNull
  public Context currentContext() {
    long state = this.requested;

    if (state == STATE_UNSUBSCRIBED) {
      return Context.empty();
    }

    return this.connector.currentContext();
  }

  @Override
  public void onSubscribe(Subscription s) {
    this.connector.onSubscribe(s);
  }

  @Override
  public void onNext(Payload p) {
    if (this.isTerminated()) {
      ReferenceCountUtil.safeRelease(p);
      return;
    }
    final UnboundedProcessor<ByteBuf> sender = this.sendProcessor;
    final ByteBufAllocator allocator = this.allocator;
    final IntObjectMap<CoreSubscriber<? super Payload>> as = this.activeStreams;
    final IntObjectMap<Subscription> ass = this.activeSubscriptions;
    final int mtu = this.mtu;

    int streamId = this.streamId;
    if (this.first) {
      this.first = false;

      if (p.refCnt() <= 0) {
        final Throwable t = new IllegalReferenceCountException(0);
        this.requested = STATE_TERMINATED;
        this.connector.s.cancel();
        this.connector.actual.onError(t);
        return;
      }

      boolean firstLoop = true;
      long requested;

      // to prevent racing between requests and onNext and to make sure the RequestChannelFrame goes
      // first
      // in case of racing may deliver work from requestN
      for (; ; ) {
        requested = this.requested;

        if (requested == STATE_TERMINATED) {
          if (firstLoop) {
            ReferenceCountUtil.safeRelease(p);
          } else {
            if (as.remove(streamId, this.connector)) {
              ass.remove(streamId, this.connector);
              final ByteBuf cancelFrame = CancelFrameFlyweight.encode(allocator, streamId);
              sender.onNext(cancelFrame);
            }
          }
          return;
        }

        if (firstLoop) {
          final Throwable throwable = parent.checkAvailable();
          if (throwable != null) {
            this.requested = STATE_TERMINATED;
            this.connector.s.cancel();
            this.connector.actual.onError(throwable);
            return;
          }
          try {
            final boolean hasMetadata = p.hasMetadata();

            streamId = this.streamIdSupplier.nextStreamId(as);

            this.streamId = streamId;

            if (mtu > 0) {
              final ByteBuf slicedData = p.data().retainedSlice();
              final ByteBuf slicedMetadata =
                  hasMetadata ? p.metadata().retainedSlice() : Unpooled.EMPTY_BUFFER;

              final ByteBuf first =
                  FragmentationUtils.encodeFirstFragment(
                      allocator,
                      mtu,
                      (int) (requested & Integer.MAX_VALUE),
                      FrameType.REQUEST_CHANNEL,
                      streamId,
                      slicedMetadata,
                      slicedData);

              as.put(streamId, this.connector);
              ass.put(streamId, this.connector);
              sender.onNext(first);

              while (slicedData.isReadable() || slicedMetadata.isReadable()) {
                final ByteBuf following =
                    FragmentationUtils.encodeFollowsFragment(
                        allocator, mtu, streamId, slicedMetadata, slicedData);
                sender.onNext(following);
              }
            } else {
              final ByteBuf data = p.data();
              final ByteBuf metadata = p.metadata();

              if (((data.readableBytes() + (hasMetadata ? metadata.readableBytes() : 0))
                      & ~FrameLengthFlyweight.FRAME_LENGTH_MASK)
                  != 0) {
                final Throwable t = new IllegalArgumentException("Too Big Payload size");
                this.requested = STATE_TERMINATED;
                this.connector.s.cancel();
                this.connector.actual.onError(t);
                ReferenceCountUtil.safeRelease(p);
                return;
              }

              final ByteBuf slicedData = data.retainedSlice();
              final ByteBuf slicedMetadata = hasMetadata ? metadata.retainedSlice() : null;

              final ByteBuf requestFrame =
                  RequestChannelFrameFlyweight.encode(
                      allocator,
                      streamId,
                      false,
                      false,
                      (int) (requested & Integer.MAX_VALUE),
                      slicedMetadata,
                      slicedData);

              as.put(streamId, this.connector);
              ass.put(streamId, this.connector);
              sender.onNext(requestFrame);
            }

            ReferenceCountUtil.safeRelease(p);
          } catch (Throwable e) {
            this.requested = STATE_TERMINATED;

            ReferenceCountUtil.safeRelease(p);
            Exceptions.throwIfFatal(e);

            this.connector.s.cancel();

            if (as.remove(streamId, this.connector)) {
              ass.remove(streamId, this.connector);
              final ByteBuf cancelFrame = CancelFrameFlyweight.encode(allocator, streamId);
              sender.onNext(cancelFrame);
            }

            this.connector.actual.onError(e);

            return;
          }
        } else {
          final ByteBuf requestNFrame =
              RequestNFrameFlyweight.encode(allocator, streamId, requested);
          sender.onNext(requestNFrame);
        }

        if (REQUESTED.compareAndSet(this, requested, STATE_WIRED)) {
          break;
        }
        firstLoop = false;
      }
    } else {
      if (p.refCnt() <= 0) {
        final Throwable t = new IllegalReferenceCountException(0);
        final ByteBuf cancelFrame = CancelFrameFlyweight.encode(allocator, streamId);
        sender.onNext(cancelFrame);
        this.connector.onError(t);
        return;
      }

      try {
        final boolean hasMetadata = p.hasMetadata();

        if (mtu > 0) {
          final ByteBuf slicedData = p.data().retainedSlice();
          final ByteBuf slicedMetadata =
              hasMetadata ? p.metadata().retainedSlice() : Unpooled.EMPTY_BUFFER;

          final ByteBuf first =
              FragmentationUtils.encodeFirstFragment(
                  allocator, mtu, FrameType.NEXT, streamId, slicedMetadata, slicedData);
          sender.onNext(first);

          while (slicedData.isReadable() || slicedMetadata.isReadable()) {
            final ByteBuf following =
                FragmentationUtils.encodeFollowsFragment(
                    allocator, mtu, streamId, slicedMetadata, slicedData);
            sender.onNext(following);
          }
        } else {
          final ByteBuf data = p.data();
          final ByteBuf metadata = p.metadata();

          if (((data.readableBytes() + (hasMetadata ? metadata.readableBytes() : 0))
                  & ~FrameLengthFlyweight.FRAME_LENGTH_MASK)
              != 0) {
            final Throwable t = new IllegalArgumentException("Too Big Payload size");
            final ByteBuf cancelFrame = CancelFrameFlyweight.encode(allocator, streamId);
            sender.onNext(cancelFrame);
            this.connector.onError(t);
            ReferenceCountUtil.safeRelease(p);
            return;
          }

          final ByteBuf slicedData = data.retainedSlice();
          final ByteBuf slicedMetadata = hasMetadata ? metadata.retainedSlice() : null;

          final ByteBuf nextFrame =
              PayloadFrameFlyweight.encode(
                  allocator, streamId, false, false, true, slicedMetadata, slicedData);

          sender.onNext(nextFrame);
        }

        ReferenceCountUtil.safeRelease(p);
      } catch (Throwable e) {
        ReferenceCountUtil.safeRelease(p);

        Exceptions.throwIfFatal(e);

        final ByteBuf cancelFrame = CancelFrameFlyweight.encode(allocator, streamId);
        sender.onNext(cancelFrame);

        this.connector.onError(e);
      }
    }
  }

  @Override
  public void onError(Throwable t) {
    long state = REQUESTED.getAndSet(this, STATE_TERMINATED);
    if (state == STATE_TERMINATED) {
      Operators.onErrorDropped(t, currentContext());
      return;
    }

    if (state == STATE_WIRED) {
      final int streamId = this.streamId;
      this.activeSubscriptions.remove(streamId, this.connector);
      this.activeStreams.remove(streamId, this.connector);

      final ByteBuf errorFrame = ErrorFrameFlyweight.encode(this.allocator, streamId, t);
      this.sendProcessor.onNext(errorFrame);
    }
    this.connector.actual.onError(t);
  }

  @Override
  public void onComplete() {
    long state = this.requested;
    if (state == STATE_TERMINATED) {
      return;
    }

    if (state == STATE_WIRED) {
      final int streamId = this.streamId;
      this.activeSubscriptions.remove(streamId, this.connector);

      final ByteBuf completeFrame = PayloadFrameFlyweight.encodeComplete(this.allocator, streamId);
      this.sendProcessor.onNext(completeFrame);
    } else {
      this.connector.actual.onError(new CancellationException("Empty Source"));
    }
  }

  @Override
  public void subscribe(CoreSubscriber<? super Payload> actual) {
    Objects.requireNonNull(actual, "subscribe");

    if (this.requested == STATE_UNSUBSCRIBED
        && REQUESTED.compareAndSet(this, STATE_UNSUBSCRIBED, STATE_SUBSCRIBED)) {
      this.connector =
          new InnerConnectorSubscriber(this, actual, activeStreams, activeSubscriptions);
      this.payloadPublisher.subscribe(this);
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
    for (; ; ) {
      currentRequested = this.requested;

      if (currentRequested == STATE_WIRED) {
        final ByteBuf requestNFrame =
            RequestNFrameFlyweight.encode(this.allocator, this.streamId, n);
        this.sendProcessor.onNext(requestNFrame);
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
        if (currentRequested == STATE_SUBSCRIBED) {
          // do first request
          this.connector.request(1);
        }
        break;
      }
    }
  }

  @Override
  public final void cancel() {
    final long state = REQUESTED.getAndSet(this, STATE_TERMINATED);
    if (state != STATE_TERMINATED) {
      final InnerConnectorSubscriber connector = this.connector;
      connector.cancel();

      if (state != STATE_WIRED) {
        // no need to send anything, since we have not started a stream yet (no logical wire)
        return;
      }

      final int streamId = this.streamId;
      this.activeStreams.remove(streamId, connector);

      final ByteBuf cancelFrame = CancelFrameFlyweight.encode(this.allocator, streamId);
      this.sendProcessor.onNext(cancelFrame);
    }
  }

  boolean isTerminated() {
    return this.requested == STATE_TERMINATED;
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
    return "source(UnicastRequestChannelFlux)";
  }

  // this should be stored to listen for requestN frames from the Responder side
  // this should accommodate frame sending logic, since it receives frames from the requester side
  // and should deliver them to the responder one
  static final class InnerConnectorSubscriber implements CoreSubscriber<Payload>, Subscription {

    final UnicastRequestChannelFlux parent;
    final CoreSubscriber<? super Payload> actual;
    final IntObjectMap<CoreSubscriber<? super Payload>> activeStreams;
    final IntObjectMap<Subscription> activeSubscriptions;

    Subscription s;

    InnerConnectorSubscriber(
        UnicastRequestChannelFlux parent,
        CoreSubscriber<? super Payload> actual,
        IntObjectMap<CoreSubscriber<? super Payload>> activeStreams,
        IntObjectMap<Subscription> activeSubscriptions) {
      this.parent = parent;
      this.actual = actual;
      this.activeStreams = activeStreams;
      this.activeSubscriptions = activeSubscriptions;
    }

    @Override
    public final void onSubscribe(Subscription outerSubscription) {
      if (Operators.validate(this.s, outerSubscription)) {
        this.s = outerSubscription;
        this.actual.onSubscribe(this.parent);
      }
    }

    @Override
    public final void onComplete() {
      if (this.parent.isTerminated()) {
        return;
      }

      this.activeStreams.remove(this.parent.streamId, this);

      this.actual.onComplete();
    }

    @Override
    public final void onError(Throwable cause) {
      Objects.requireNonNull(cause, "onError cannot be null");

      if (REQUESTED.getAndSet(this.parent, STATE_TERMINATED) == STATE_TERMINATED) {
        Operators.onErrorDropped(cause, currentContext());
        return;
      }

      final int streamId = this.parent.streamId;

      this.activeSubscriptions.remove(streamId, this);
      this.activeStreams.remove(streamId, this);

      this.s.cancel();
      this.actual.onError(cause);
    }

    @Override
    public final void onNext(Payload value) {
      if (this.parent.isTerminated()) {
        ReferenceCountUtil.safeRelease(value);
        return;
      }

      this.actual.onNext(value);
    }

    @Override
    public void request(long n) {
      this.s.request(n);
    }

    @Override
    public void cancel() {
      this.activeSubscriptions.remove(this.parent.streamId, this);
      this.s.cancel();
    }
  }
}
