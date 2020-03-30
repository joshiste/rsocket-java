package io.rsocket;

import static io.rsocket.fragmentation.FragmentationUtils.isFragmentable;
import static io.rsocket.fragmentation.FragmentationUtils.isValid;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.IllegalReferenceCountException;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.collection.IntObjectMap;
import io.rsocket.fragmentation.FragmentationUtils;
import io.rsocket.fragmentation.ReassemblyUtils;
import io.rsocket.frame.CancelFrameFlyweight;
import io.rsocket.frame.ErrorFrameFlyweight;
import io.rsocket.frame.FrameType;
import io.rsocket.frame.PayloadFrameFlyweight;
import io.rsocket.frame.RequestChannelFrameFlyweight;
import io.rsocket.frame.RequestNFrameFlyweight;
import io.rsocket.frame.decoder.PayloadDecoder;
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

final class RequestChannelFlux extends Flux<Payload>
    implements CoreSubscriber<Payload>, Subscription, Scannable {

  final ByteBufAllocator allocator;
  final int mtu;
  final Flux<Payload> payloadPublisher;
  final StateAware parent;
  final StreamIdSupplier streamIdSupplier;
  final IntObjectMap<Reassemble<?>> activeStreams;
  final UnboundedProcessor<ByteBuf> sendProcessor;
  final PayloadDecoder payloadDecoder;

  static final long STATE_UNSUBSCRIBED = -2;
  static final long STATE_SUBSCRIBED = -1;
  static final long STATE_WIRED = -3;
  static final long STATE_HALF_CLOSED = Long.MIN_VALUE + 1;
  static final long STATE_TERMINATED = Long.MIN_VALUE;

  volatile long requested;
  static final AtomicLongFieldUpdater<RequestChannelFlux> REQUESTED =
      AtomicLongFieldUpdater.newUpdater(RequestChannelFlux.class, "requested");

  int streamId;
  InnerConnectorSubscriber connector;

  boolean first = true;
  boolean done;

  RequestChannelFlux(
      ByteBufAllocator allocator,
      Flux<Payload> payloadPublisher,
      int mtu,
      StateAware parent,
      StreamIdSupplier streamIdSupplier,
      IntObjectMap<Reassemble<?>> activeSubscribers,
      UnboundedProcessor<ByteBuf> sendProcessor,
      PayloadDecoder payloadDecoder) {
    this.allocator = allocator;
    this.payloadPublisher = payloadPublisher;
    this.mtu = mtu;
    this.parent = parent;
    this.streamIdSupplier = streamIdSupplier;
    this.activeStreams = activeSubscribers;
    this.sendProcessor = sendProcessor;
    this.payloadDecoder = payloadDecoder;

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
    if (this.isTerminated() || this.done) {
      ReferenceCountUtil.safeRelease(p);
      return;
    }
    final UnboundedProcessor<ByteBuf> sender = this.sendProcessor;
    final ByteBufAllocator allocator = this.allocator;
    final IntObjectMap<Reassemble<?>> as = this.activeStreams;
    final int mtu = this.mtu;
    final InnerConnectorSubscriber connector = this.connector;

    int streamId = this.streamId;
    if (this.first) {
      this.first = false;

      if (p.refCnt() <= 0) {
        final Throwable t = new IllegalReferenceCountException(0);
        this.requested = STATE_TERMINATED;
        connector.s.cancel();
        connector.actual.onError(t);
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
            p.release();
          } else {
            if (as.remove(streamId, connector)) {
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
            connector.s.cancel();
            connector.actual.onError(throwable);
            return;
          }
          try {
            final boolean hasMetadata = p.hasMetadata();
            final ByteBuf data = p.data();
            final ByteBuf metadata = p.metadata();

            if (hasMetadata ? !isValid(mtu, data, metadata) : !isValid(mtu, data)) {
              final Throwable t = new IllegalArgumentException("Too Big Payload size");

              p.release();
              this.requested = STATE_TERMINATED;
              connector.s.cancel();
              connector.actual.onError(t);

              return;
            }

            streamId = this.streamIdSupplier.nextStreamId(as);
            this.streamId = streamId;

            if (hasMetadata ? isFragmentable(mtu, data, metadata) : isFragmentable(mtu, data)) {
              final ByteBuf slicedData = data.slice();
              final ByteBuf slicedMetadata = hasMetadata ? metadata.slice() : Unpooled.EMPTY_BUFFER;

              final ByteBuf first =
                  FragmentationUtils.encodeFirstFragment(
                      allocator,
                      mtu,
                      (int) (requested & Integer.MAX_VALUE),
                      FrameType.REQUEST_CHANNEL,
                      streamId,
                      slicedMetadata,
                      slicedData);

              as.put(streamId, connector);
              sender.onNext(first);

              while (slicedData.isReadable() || slicedMetadata.isReadable()) {
                final ByteBuf following =
                    FragmentationUtils.encodeFollowsFragment(
                        allocator,
                        mtu,
                        streamId,
                        false, // TODO: Should be a different flag in case of the scalar source or
                        // we know in advance upstream is mono
                        slicedMetadata,
                        slicedData);
                sender.onNext(following);
              }
            } else {
              final ByteBuf retainedSlicedData = data.retainedSlice();
              final ByteBuf retainedSlicedMetadata = hasMetadata ? metadata.retainedSlice() : null;

              final ByteBuf requestFrame =
                  RequestChannelFrameFlyweight.encode(
                      allocator,
                      streamId,
                      false,
                      false, // TODO: Should be a different flag in case of the scalar source or
                      // sync source of a single element
                      (int) (requested & Integer.MAX_VALUE),
                      retainedSlicedMetadata,
                      retainedSlicedData);

              as.put(streamId, connector);
              sender.onNext(requestFrame);
            }

            p.release();
          } catch (Throwable e) {
            this.requested = STATE_TERMINATED;

            ReferenceCountUtil.safeRelease(p);
            Exceptions.throwIfFatal(e);

            connector.s.cancel();

            if (as.remove(streamId, connector)) {
              final ByteBuf cancelFrame = CancelFrameFlyweight.encode(allocator, streamId);
              sender.onNext(cancelFrame);
            }

            connector.actual.onError(e);

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
        connector.onError(t);
        return;
      }

      try {
        final boolean hasMetadata = p.hasMetadata();
        final ByteBuf data = p.data();
        final ByteBuf metadata = p.metadata();

        if (hasMetadata ? !isValid(mtu, data, metadata) : !isValid(mtu, data)) {
          final Throwable t = new IllegalArgumentException("Too Big Payload size");
          final ByteBuf cancelFrame = CancelFrameFlyweight.encode(allocator, streamId);

          p.release();

          sender.onNext(cancelFrame);
          connector.onError(t);
          return;
        }

        if (hasMetadata ? isFragmentable(mtu, data, metadata) : isFragmentable(mtu, data)) {
          final ByteBuf slicedData = p.data().slice();
          final ByteBuf slicedMetadata = hasMetadata ? p.metadata().slice() : Unpooled.EMPTY_BUFFER;

          final ByteBuf first =
              FragmentationUtils.encodeFirstFragment(
                  allocator, mtu, FrameType.NEXT, streamId, slicedMetadata, slicedData);
          sender.onNext(first);

          while (slicedData.isReadable() || slicedMetadata.isReadable()) {
            final ByteBuf following =
                FragmentationUtils.encodeFollowsFragment(
                    allocator, mtu, streamId, false, slicedMetadata, slicedData);
            sender.onNext(following);
          }
        } else {
          final ByteBuf retainedSlicedData = data.retainedSlice();
          final ByteBuf retainedSlicedMetadata = hasMetadata ? metadata.retainedSlice() : null;

          final ByteBuf nextFrame =
              PayloadFrameFlyweight.encode(
                  allocator,
                  streamId,
                  false,
                  false,
                  true,
                  retainedSlicedMetadata,
                  retainedSlicedData);

          sender.onNext(nextFrame);
        }

        p.release();
      } catch (Throwable e) {
        ReferenceCountUtil.safeRelease(p);

        Exceptions.throwIfFatal(e);

        final ByteBuf cancelFrame = CancelFrameFlyweight.encode(allocator, streamId);
        sender.onNext(cancelFrame);

        connector.onError(e);
      }
    }
  }

  @Override
  public void onError(Throwable t) {
    if (this.isTerminated() || this.done) {
      Operators.onErrorDropped(t, currentContext());
      return;
    }

    this.done = true;

    long state = REQUESTED.getAndSet(this, STATE_TERMINATED);
    if (state == STATE_TERMINATED) {
      Operators.onErrorDropped(t, currentContext());
      return;
    }

    if (state <= STATE_WIRED) {
      final int streamId = this.streamId;
      this.activeStreams.remove(streamId, this.connector);

      // propagates error to remote responder
      final ByteBuf errorFrame = ErrorFrameFlyweight.encode(this.allocator, streamId, t);
      this.sendProcessor.onNext(errorFrame);
    }

    // propagates error to actual subscriber as well
    this.connector.actual.onError(t);
  }

  @Override
  public void onComplete() {
    long state = this.requested;
    if (state == STATE_TERMINATED || this.done) {
      return;
    }

    this.done = true;

    boolean terminal;
    for (; ; ) {
      terminal = state > STATE_WIRED || state == STATE_HALF_CLOSED;

      if (REQUESTED.compareAndSet(this, state, terminal ? STATE_TERMINATED : STATE_HALF_CLOSED)) {
        break;
      }

      state = this.requested;

      if (state == STATE_TERMINATED) {
        return;
      }
    }

    if (state > STATE_WIRED) {
      this.connector.actual.onError(new CancellationException("Empty Source"));
      return;
    }

    final int streamId = this.streamId;

    if (terminal) {
      this.activeStreams.remove(streamId, this.connector);
    }

    final ByteBuf completeFrame = PayloadFrameFlyweight.encodeComplete(this.allocator, streamId);
    this.sendProcessor.onNext(completeFrame);
  }

  @Override
  public void subscribe(CoreSubscriber<? super Payload> actual) {
    Objects.requireNonNull(actual, "subscribe");

    if (this.requested == STATE_UNSUBSCRIBED
        && REQUESTED.compareAndSet(this, STATE_UNSUBSCRIBED, STATE_SUBSCRIBED)) {
      this.connector =
          new InnerConnectorSubscriber(
              this, actual, this.activeStreams, this.payloadDecoder, this.allocator);
      this.payloadPublisher.subscribe(this);
    } else {
      Operators.error(
          actual, new IllegalStateException("RequestChannelFlux allows only a single Subscriber"));
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

      if (currentRequested == STATE_WIRED || currentRequested == STATE_HALF_CLOSED) {
        if (this.connector.done) {
          return;
        }

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
    long state = this.requested;
    if (state == STATE_TERMINATED) {
      return;
    }

    final InnerConnectorSubscriber connector = this.connector;
    final CompositeByteBuf frames = connector.frames;
    connector.frames = null;

    state = REQUESTED.getAndSet(this, STATE_TERMINATED);
    if (state == STATE_TERMINATED) {
      return;
    }

    connector.s.cancel();

    if (state != STATE_WIRED) {
      // no need to send anything, since we have not started a stream yet (no logical wire)
      return;
    }

    if (frames != null && frames.refCnt() > 0) {
      ReferenceCountUtil.safeRelease(frames);
    }

    final int streamId = this.streamId;
    this.activeStreams.remove(streamId, connector);

    final ByteBuf cancelFrame = CancelFrameFlyweight.encode(this.allocator, streamId);
    this.sendProcessor.onNext(cancelFrame);
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
    return "source(RequestChannelFlux)";
  }

  // this should be stored to listen for requestN frames from the Responder side
  // this should accommodate frame sending logic, since it receives frames from the requester side
  // and should deliver them to the responder one
  static final class InnerConnectorSubscriber implements Reassemble<Payload> {

    final RequestChannelFlux parent;
    final CoreSubscriber<? super Payload> actual;
    final IntObjectMap<Reassemble<?>> activeStreams;
    final PayloadDecoder payloadDecoder;
    final ByteBufAllocator allocator;

    Subscription s;
    CompositeByteBuf frames;
    boolean done;

    InnerConnectorSubscriber(
        RequestChannelFlux parent,
        CoreSubscriber<? super Payload> actual,
        IntObjectMap<Reassemble<?>> activeStreams,
        PayloadDecoder payloadDecoder,
        ByteBufAllocator allocator) {
      this.parent = parent;
      this.actual = actual;
      this.activeStreams = activeStreams;
      this.payloadDecoder = payloadDecoder;
      this.allocator = allocator;
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
      final RequestChannelFlux parent = this.parent;
      long state = parent.requested;
      if (state == STATE_TERMINATED || this.done) {
        return;
      }

      this.done = true;

      boolean terminal;
      for (; ; ) {
        terminal = state > STATE_WIRED || parent.done;

        if (REQUESTED.compareAndSet(
            parent, state, terminal ? STATE_TERMINATED : STATE_HALF_CLOSED)) {
          break;
        }

        state = parent.requested;

        if (state == STATE_TERMINATED) {
          return;
        }
      }

      if (terminal) {
        this.activeStreams.remove(parent.streamId, this);
      }

      this.actual.onComplete();
    }

    @Override
    public final void onError(Throwable cause) {
      final RequestChannelFlux parent = this.parent;
      if (parent.isTerminated() || this.done) {
        Operators.onErrorDropped(cause, currentContext());
        return;
      }

      final CompositeByteBuf frames = this.frames;
      this.done = true;
      this.frames = null;

      if (REQUESTED.getAndSet(parent, STATE_TERMINATED) == STATE_TERMINATED) {
        Operators.onErrorDropped(cause, currentContext());
        return;
      }

      if (frames != null && frames.refCnt() > 0) {
        frames.release();
      }

      final int streamId = parent.streamId;

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
      final RequestChannelFlux parent = this.parent;

      long state = parent.requested;
      if (state == STATE_TERMINATED) {
        return;
      }

      parent.done = true;

      boolean terminal;
      for (; ; ) {
        terminal = state > STATE_WIRED || this.done;

        if (REQUESTED.compareAndSet(
            parent, state, terminal ? STATE_TERMINATED : STATE_HALF_CLOSED)) {
          break;
        }

        state = parent.requested;

        if (state == STATE_TERMINATED) {
          return;
        }
      }

      if (terminal) {
        this.activeStreams.remove(parent.streamId, parent);
      }

      this.s.cancel();
    }

    @Override
    public boolean isReassemblingNow() {
      return this.frames != null;
    }

    @Override
    public void reassemble(ByteBuf followingFrame, boolean hasFollows, boolean terminal) {
      final RequestChannelFlux parent = this.parent;
      if (parent.isTerminated() || this.done) {
        return;
      }

      CompositeByteBuf frames = this.frames;

      if (frames == null) {
        frames =
            ReassemblyUtils.addFollowingFrame(this.allocator.compositeBuffer(), followingFrame);
        this.frames = frames;

        if (parent.isTerminated()) {
          this.frames = null;
          if (frames.refCnt() > 0) {
            ReferenceCountUtil.safeRelease(frames);
          }
        }

        return;
      } else {
        frames = ReassemblyUtils.addFollowingFrame(frames, followingFrame);
      }

      if (!hasFollows) {
        this.frames = null;
        try {
          this.onNext(this.payloadDecoder.apply(frames));
          frames.release();

          if (terminal) {
            this.onComplete();
          }
        } catch (Throwable t) {
          ReferenceCountUtil.safeRelease(frames);
          Exceptions.throwIfFatal(t);

          if (!parent.isTerminated()) {
            this.actual.onError(t);
          }
          parent.cancel();
        }
      }
    }
  }
}
