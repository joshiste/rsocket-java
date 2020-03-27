package io.rsocket;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.IllegalReferenceCountException;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.collection.IntObjectMap;
import io.rsocket.fragmentation.FragmentationUtils;
import io.rsocket.frame.ErrorFrameFlyweight;
import io.rsocket.frame.FrameLengthFlyweight;
import io.rsocket.frame.FrameType;
import io.rsocket.frame.PayloadFrameFlyweight;
import io.rsocket.frame.decoder.PayloadDecoder;
import io.rsocket.internal.UnboundedProcessor;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.function.Consumer;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Operators;
import reactor.util.context.Context;

public class RequestChannelSubscriber extends Flux<Payload>
    implements CoreSubscriber<Payload>, Reassemble {

  final int streamId;
  final ByteBufAllocator allocator;
  final Consumer<? super Throwable> errorConsumer;
  final PayloadDecoder payloadDecoder;
  final int mtu;
  final IntObjectMap<? super Subscription> activeStreams;
  final UnboundedProcessor<ByteBuf> sendProcessor;

  final ResponderRSocket handler;

  volatile int wip;
  static final AtomicIntegerFieldUpdater<RequestChannelSubscriber> WIP =
      AtomicIntegerFieldUpdater.newUpdater(RequestChannelSubscriber.class, "wip");

  volatile long requested;
  static final AtomicLongFieldUpdater<RequestChannelSubscriber> REQUESTED =
      AtomicLongFieldUpdater.newUpdater(RequestChannelSubscriber.class, "requested");
  // |.....INITIAL STATE.....|...RESULT STATE.....|    |..INITIAL BITS...|.RESULT BITS..|
  // | ANY                 ->| TERMINATED         |    | _XX_XXXX...X  ->| _10_0000...0 |
  // | UNSUB + UNREQ       ->| SUB   + UNREQ      |    | _00_XXXX...X  ->| _01_XXXX...X |
  // | UNSUB + REQ         ->| SUB   + REQ        |    | _00_0000...0  ->| _01_0000...0 |
  // | UNSUB + UNREQ       ->| UNSUB + REQ        |    | _00_XXXX...X  ->| _00_0000...0 |
  //  BY THIS POINT FIRST REQUEST TO UPSTREAM IS ALWAYS 0
  // | SUB   + UNREQ + NF  ->| SUB   + REQ   + NF |    | _01_XXXX...X  ->| _01_0000...0 |
  // | SUB   + UNREQ + NF  ->| SUB   + UNREQ + F  |    | _01_XXXX...X  ->| _11_XXXX...X |
  // | SUB   + UNREQ + F   ->| SUB   + REQ   + F  |    | _11_XXXX...X  ->| _11_0000...0 |
  // | SUB   + REQ   + NF  ->| SUB   + REQ   + F  |    | _01_0000...0  ->| _11_0000...0 |
  //
  //
  //                                          2_BITS_STATE⌍                          ⌌ 62_BITS_REQUEST_VALUE
  //                                                     |..|..62...|      |..|..62...|
  //                                                     |....64....|      |....64....|
  //                                                     3  bits for STATE
  //                                                     61 bits for REQUESTED
  static final long REQUEST_MAX_VALUE =
      Long.MAX_VALUE / 2; // can do that since max requestN is Integer.MAX_VALUE

  static final long FLAG_UNSUBSCRIBED = 0;
  static final long FLAG_SUBSCRIBED = Long.MAX_VALUE / 2 + 1;
  static final long FLAG_FIRST = Long.MIN_VALUE;

  static final long MASK_FLAGS = FLAG_SUBSCRIBED | FLAG_FIRST;
  static final long MASK_REQUESTED = ~MASK_FLAGS;

  static final long STATE_REQUESTED = 0;
  static final long STATE_TERMINATED = Long.MIN_VALUE;

  OuterSenderSubscriber senderSubscriber;
  Subscription s;
  CoreSubscriber<? super Payload> actual;
  CompositeByteBuf frames;
  boolean done;
  Throwable t;

  public RequestChannelSubscriber(
      int streamId,
      long firstRequest,
      ByteBufAllocator allocator,
      PayloadDecoder payloadDecoder,
      ByteBuf firstFrame,
      int mtu,
      Consumer<? super Throwable> errorConsumer,
      IntObjectMap<? super Subscription> activeStreams,
      UnboundedProcessor<ByteBuf> sendProcessor,
      ResponderRSocket handler) {
    this.streamId = streamId;
    this.allocator = allocator;
    this.mtu = mtu;
    this.errorConsumer = errorConsumer;
    this.activeStreams = activeStreams;
    this.sendProcessor = sendProcessor;
    this.payloadDecoder = payloadDecoder;
    this.handler = handler;
    this.frames = allocator.compositeBuffer().addComponent(true, firstFrame);

    REQUESTED.lazySet(this, firstRequest);
  }

  public RequestChannelSubscriber(
      int streamId,
      long firstRequest,
      ByteBufAllocator allocator,
      PayloadDecoder payloadDecoder,
      Payload firstPayload,
      Flux<Payload> payloads,
      int mtu,
      Consumer<? super Throwable> errorConsumer,
      IntObjectMap<? super Subscription> activeStreams,
      UnboundedProcessor<ByteBuf> sendProcessor) {
    this.streamId = streamId;
    this.allocator = allocator;
    this.mtu = mtu;
    this.errorConsumer = errorConsumer;
    this.activeStreams = activeStreams;
    this.sendProcessor = sendProcessor;
    this.payloadDecoder = payloadDecoder;

    final OuterSenderSubscriber senderSubscriber = new OuterSenderSubscriber(this);
    this.senderSubscriber = senderSubscriber;
    this.handler = null;
    this.frames = null;

    REQUESTED.lazySet(this, firstRequest);

    payloads.subscribe(senderSubscriber);
  }

  @Override
  // subscriber from the requestChannel method
  public void subscribe(CoreSubscriber<? super Payload> actual) {

    for (; ; ) {
      long state = this.requested;

      if (state == STATE_TERMINATED) {
        Operators.error(
            actual,
            new CancellationException("RequestChannelSubscriber has already been terminated"));
        return;
      }

      if ((state & FLAG_SUBSCRIBED) != FLAG_UNSUBSCRIBED) {
        Operators.error(
            actual,
            new IllegalStateException("RequestChannelSubscriber allows only one Subscriber"));
        return;
      }

      if (REQUESTED.compareAndSet(this, state, state | FLAG_SUBSCRIBED)) {
        break;
      }
    }

    this.actual = actual;
    // sends sender as a subscription since every request|cancel signal should be encoded to
    // requestNFrame|cancelFrame
    actual.onSubscribe(this.senderSubscriber);
  }

  @Override
  // subscription from the requestChannel method
  public void onSubscribe(Subscription subscription) {
    long state;
    for (; ; ) {
      state = this.requested;
      long requested;
      // terminated or has already received subscription
      if (state == STATE_TERMINATED || (requested = (state & MASK_REQUESTED)) == STATE_REQUESTED) {
        subscription.cancel();
        return;
      }

      this.s = subscription;
      if (REQUESTED.compareAndSet(this, state, (state & MASK_FLAGS))) {
        subscription.request(requested == REQUEST_MAX_VALUE ? Long.MAX_VALUE : requested);
        return;
      }
    }
  }

  @Override
  public void request(long n) {
    for (; ; ) {
      long state = this.requested;

      // terminated or has already received subscription
      if (state == STATE_TERMINATED) {
        return;
      }

      long requested;
      if ((requested = (state & MASK_REQUESTED)) == STATE_REQUESTED) {
        this.s.request(n);
        return;
      }

      if (requested == REQUEST_MAX_VALUE) {
        return;
      }

      long next = Math.min(Operators.addCap(requested, n), REQUEST_MAX_VALUE);
      if (REQUESTED.compareAndSet(this, state, next | (state & MASK_FLAGS))) {
        return;
      }
    }
  }

  @Override
  public void cancel() {
    long state = REQUESTED.getAndSet(this, STATE_TERMINATED);
    if (state == STATE_TERMINATED) {
      return;
    }

    this.activeStreams.remove(this.streamId, this);

    final CompositeByteBuf frames = this.frames;
    if (frames != null && frames.refCnt() > 0) {
      ReferenceCountUtil.safeRelease(frames);
    }

    final OuterSenderSubscriber senderSubscriber = this.senderSubscriber;
    if (senderSubscriber != null) {
      final Payload firstPayload = senderSubscriber.firstPayload;
      if (firstPayload != null && firstPayload.refCnt() > 0) {
        ReferenceCountUtil.safeRelease(firstPayload);
      }
    }

    if ((state & MASK_REQUESTED) == STATE_REQUESTED) {
      this.s.cancel();
    }
  }

  @Override
  public void onNext(Payload p) {
    if (this.requested == STATE_TERMINATED || this.done) {
      ReferenceCountUtil.safeRelease(p);
      return;
    }

    this.actual.onNext(p);
  }

  @Override
  public void onError(Throwable t) {
    if (this.done) {
      Operators.onErrorDropped(t, Context.empty());
      return;
    }

    this.done = true;
    this.t = t;

    long state = REQUESTED.getAndSet(this, STATE_TERMINATED);
    if (state == STATE_TERMINATED) {
      Operators.onErrorDropped(t, Context.empty());
      return;
    }

    this.activeStreams.remove(this.streamId, this);

    if ((state & MASK_FLAGS) != FLAG_UNSUBSCRIBED)  {
      this.actual.onError(t);
    }

    if ((state & FLAG_FIRST) != FLAG_FIRST) {
      ReferenceCountUtil.safeRelease(this.senderSubscriber.firstPayload);
    }

    this.s.cancel();
  }

  @Override
  public void onComplete() {
    if (this.done) {
      return;
    }

    this.done = true;

    long state;
    for (;;) {
      state = this.requested;

      if (state == STATE_TERMINATED) {
        return;
      }

      if ((state & FLAG_FIRST) == FLAG_FIRST) {
        this.actual.onComplete();
        this.tryTerminate();
        return;
      }

      if (REQUESTED.compareAndSet(this, state, state | FLAG_FIRST)) {
        return;
      }
    }
  }

  void tryTerminate() {
    if (this.requested == STATE_TERMINATED) {
      return;
    }

    if (WIP.getAndIncrement(this) != 0) {
      return;
    }

    final OuterSenderSubscriber senderSubscriber = this.senderSubscriber;

    int m = 1;
    boolean upstreamDone = this.done;
    boolean downstreamDone = senderSubscriber.done;

    for (;;) {

      if (upstreamDone && downstreamDone) {
        this.requested = STATE_TERMINATED;

        this.activeStreams.remove(this.streamId, this);
      }


      m = WIP.addAndGet(this, -m);
      if (m == 0) {
        return;
      }
      upstreamDone = this.done;
      downstreamDone = senderSubscriber.done;
    }
  }

  @Override
  public boolean isReassemblingNow() {
    return this.frames != null;
  }

  @Override
  public void reassemble(ByteBuf dataAndMetadata, boolean hasFollows) {
    final CompositeByteBuf frames = this.frames.addComponent(true, dataAndMetadata);

    if (!hasFollows) {
      if (this.senderSubscriber == null) {
        try {
          final OuterSenderSubscriber senderSubscriber = new OuterSenderSubscriber();
          this.senderSubscriber = senderSubscriber;
          Payload firstPayload = this.payloadDecoder.apply(frames);
          ReferenceCountUtil.safeRelease(frames);
          this.frames = null;
          Flux<Payload> source = this.handler.requestChannel(firstPayload, this);
          source.subscribe(senderSubscriber);
        } catch (Throwable t) {
          ReferenceCountUtil.safeRelease(frames);
          Exceptions.throwIfFatal(t);
          this.cancel(t);
        }
      } else {
        try {
          Payload firstPayload = this.payloadDecoder.apply(frames);
          ReferenceCountUtil.safeRelease(frames);
          Flux<Payload> source = this.handler.requestChannel(firstPayload, this);
          source.subscribe(senderSubscriber);
        } catch (Throwable t) {
          ReferenceCountUtil.safeRelease(frames);
          Exceptions.throwIfFatal(t);
          this.onError(t);
        }
      }
    }
  }

  static final class OuterSenderSubscriber implements CoreSubscriber<Payload>, Subscription {

    final RequestChannelSubscriber parent;
    final PayloadDecoder payloadDecoder;
    final UnboundedProcessor<ByteBuf> sendProcessor;
    final ByteBufAllocator allocator;
    final RSocket handler;
    final int mtu;

    Payload firstPayload;

    boolean done;
    Throwable t;

    OuterSenderSubscriber(RequestChannelSubscriber parent, Payload firstPayload) {
      this.parent = parent;

      this.firstPayload = firstPayload;
    }

    @Override
    public void onSubscribe(Subscription subscription) {
      this.parent.onSubscribe(subscription);
    }

    @Override
    public void onNext(Payload p) {
      final RequestChannelSubscriber parent = this.parent;
      if (parent.requested == STATE_TERMINATED) {
        ReferenceCountUtil.safeRelease(parent);
        return;
      }

      final int streamId = parent.streamId;
      final UnboundedProcessor<ByteBuf> sender = this.sendProcessor;
      final ByteBufAllocator allocator = this.allocator;

      if (p.refCnt() <= 0) {
        parent.cancel();

        final IllegalReferenceCountException t = new IllegalReferenceCountException(0);
        final ByteBuf errorFrame = ErrorFrameFlyweight.encode(allocator, streamId, t);
        this.errorConsumer.accept(t);
        sender.onNext(errorFrame);
      }

      try {
        final int mtu = this.mtu;
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
                    allocator, mtu, streamId, false, slicedMetadata, slicedData);
            sender.onNext(following);
          }

        } else {
          final ByteBuf data = p.data();
          final ByteBuf metadata = p.metadata();

          if (((data.readableBytes() + (hasMetadata ? metadata.readableBytes() : 0))
                  & ~FrameLengthFlyweight.FRAME_LENGTH_MASK)
              != 0) {
            parent.cancel();

            final Throwable t = new IllegalArgumentException("Too Big Payload size");
            final ByteBuf errorFrame = ErrorFrameFlyweight.encode(allocator, streamId, t);
            this.errorConsumer.accept(t);
            sender.onNext(errorFrame);

          } else {
            final ByteBuf slicedData = data.retainedSlice();
            final ByteBuf slicedMetadata = hasMetadata ? metadata.retainedSlice() : null;

            final ByteBuf nextFrame =
                PayloadFrameFlyweight.encode(
                    allocator, streamId, false, true, true, slicedMetadata, slicedData);
            sender.onNext(nextFrame);
          }
        }

        ReferenceCountUtil.safeRelease(p);
      } catch (Throwable t) {
        parent.cancel();
        ReferenceCountUtil.safeRelease(p);
        Exceptions.throwIfFatal(t);

        this.errorConsumer.accept(t);

        final ByteBuf errorFrame = ErrorFrameFlyweight.encode(allocator, streamId, t);
        sender.onNext(errorFrame);
      }
    }

    @Override
    public void onError(Throwable t) {
      this.errorConsumer.accept(t);

      if (REQUESTED.getAndSet(this.parent, STATE_TERMINATED) == STATE_TERMINATED) {
        Operators.onErrorDropped(t, Context.empty());
        return;
      }

      final int streamId = this.parent.streamId;

      this.activeStreams.remove(streamId, this);

      final ByteBuf errorFrame = ErrorFrameFlyweight.encode(this.allocator, streamId, t);
      this.sendProcessor.onNext(errorFrame);
    }

    @Override
    public void onComplete() {
      final RequestChannelSubscriber parent = this.parent;
      if (REQUESTED.getAndSet(parent, STATE_TERMINATED) == STATE_TERMINATED) {
        return;
      }


      final ByteBuf completeFrame = PayloadFrameFlyweight.encodeComplete(this.allocator, parent.streamId);
      this.sendProcessor.onNext(completeFrame);

      parent.tryTerminate();
    }

    @Override
    public void request(long n) {
      long current;
      long next;
      for (; ; ) {
        current = this.requested;

        if (current <= STATE_SUBSCRIBED_RECEIVED_MAX) {
          return;
        }

        if (current == FLAG_SUBSCRIBED) {
          this.s.request(n);
          return;
        }

        next = Operators.addCap(current, n);

        if (REQUESTED.compareAndSet(this, current, next)) {
          return;
        }
      }
    }

    @Override
    public void cancel() {
      long state = REQUESTED.getAndSet(this, STATE_TERMINATED);
      if (state == STATE_TERMINATED) {
        return;
      }

      final CompositeByteBuf frames = this.frames;
      if (frames != null && frames.refCnt() > 0) {
        ReferenceCountUtil.safeRelease(frames);
      }

      if (state <= FLAG_SUBSCRIBED) {
        this.activeStreams.remove(this.streamId, this);
        this.s.cancel();
      }
    }
  }
}
