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
import io.rsocket.frame.RequestNFrameFlyweight;
import io.rsocket.frame.decoder.PayloadDecoder;
import io.rsocket.internal.UnboundedProcessor;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.function.Consumer;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Operators;
import reactor.util.context.Context;

public class RequestChannelSubscriber extends Flux<Payload> implements Reassemble<Payload> {

  final int streamId;
  final ByteBufAllocator allocator;
  final Consumer<? super Throwable> errorConsumer;
  final PayloadDecoder payloadDecoder;
  final int mtu;
  final IntObjectMap<Reassemble<?>> activeStreams;
  final UnboundedProcessor<ByteBuf> sendProcessor;

  final ResponderRSocket handler;

  volatile long requested;
  static final AtomicLongFieldUpdater<RequestChannelSubscriber> REQUESTED =
      AtomicLongFieldUpdater.newUpdater(RequestChannelSubscriber.class, "requested");
  // |.....INITIAL STATE.....|...RESULT STATE.....|    |...INITIAL BITS...|..RESULT BITS..|
  // | UNSUB + UNREQ       ->| SUB   + UNREQ      |    | _000_XXXX...X  ->| _001_XXXX...X |
  // | UNSUB + REQ         ->| SUB   + REQ        |    | _000_0000...0  ->| _001_0000...0 |
  // | UNSUB + UNREQ       ->| UNSUB + REQ        |    | _000_XXXX...X  ->| _000_0000...0 |
  //  BY THIS POINT FIRST REQUEST TO UPSTREAM IS ALWAYS 0
  // | SUB   + UNREQ + NF  ->| SUB   + REQ   + NF |    | _001_XXXX...X  ->| _001_0000...0 |
  // | SUB   + UNREQ + NF  ->| SUB   + UNREQ + F  |    | _001_XXXX...X  ->| _011_XXXX...X |
  // | SUB   + UNREQ + F   ->| SUB   + REQ   + F  |    | _011_XXXX...X  ->| _011_0000...0 |
  // | SUB   + REQ   + NF  ->| SUB   + REQ   + F  |    | _001_0000...0  ->| _011_0000...0 |
  //  TERMINAL STATES
  // | ANY                 ->| TERMINATED         |    | _XXX_XXXX...X  ->| _100_0000...0 |
  // | ANY                 ->| HALF_CLOSED        |    | _XXX_XXXX...X  ->| _1XX_XXXX...X |
  //
  //
  //                                          3_BITS_STATE⌍                          ⌌
  // 61_BITS_REQUEST_VALUE
  //                                                     |.3.|...61...|     |.3.|...61...|
  //                                                     |.....64.....|     |.....64.....|
  //                                                     3  bits for STATE
  //                                                     61 bits for REQUESTED

  // can do that since max requestN is Integer.MAX_VALUE so having 61 bits is more than enough
  static final long REQUEST_MAX_VALUE = Long.MAX_VALUE / 4;

  static final long FLAG_UNSUBSCRIBED = 0;
  static final long FLAG_SUBSCRIBED = Long.MAX_VALUE / 4 + 1;
  static final long FLAG_FIRST = Long.MAX_VALUE / 2 + 1;
  static final long FLAG_HALF_CLOSED = Long.MIN_VALUE;

  static final long MASK_FLAGS = FLAG_SUBSCRIBED | FLAG_FIRST | FLAG_HALF_CLOSED;
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
      IntObjectMap<Reassemble<?>> activeStreams,
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
    this.frames = ReassemblyUtils.addFollowingFrame(allocator.compositeBuffer(), firstFrame);

    REQUESTED.lazySet(this, Math.min(firstRequest, REQUEST_MAX_VALUE));
  }

  public RequestChannelSubscriber(
      int streamId,
      long firstRequest,
      ByteBufAllocator allocator,
      PayloadDecoder payloadDecoder,
      Payload firstPayload,
      int mtu,
      Consumer<? super Throwable> errorConsumer,
      IntObjectMap<Reassemble<?>> activeStreams,
      UnboundedProcessor<ByteBuf> sendProcessor) {
    this.streamId = streamId;
    this.allocator = allocator;
    this.mtu = mtu;
    this.errorConsumer = errorConsumer;
    this.activeStreams = activeStreams;
    this.sendProcessor = sendProcessor;
    this.payloadDecoder = payloadDecoder;

    this.senderSubscriber =
        new OuterSenderSubscriber(
            this,
            firstPayload,
            payloadDecoder,
            mtu,
            sendProcessor,
            activeStreams,
            allocator,
            errorConsumer);
    this.handler = null;
    this.frames = null;

    REQUESTED.lazySet(this, Math.min(firstRequest, REQUEST_MAX_VALUE));
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
  public Context currentContext() {

    if ((this.requested & FLAG_SUBSCRIBED) != FLAG_UNSUBSCRIBED) {
      return Context.empty();
    }

    return this.actual.currentContext();
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
      if (REQUESTED.compareAndSet(this, state, state & MASK_FLAGS)) {
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
    if (this.requested == STATE_TERMINATED) {
      return;
    }

    final CompositeByteBuf frames = this.frames;
    this.frames = null;

    long state = REQUESTED.getAndSet(this, STATE_TERMINATED);
    if (state == STATE_TERMINATED) {
      return;
    }

    this.activeStreams.remove(this.streamId, this);

    if ((state & MASK_REQUESTED) == STATE_REQUESTED) {
      this.s.cancel();
    }

    if (frames != null && frames.refCnt() > 0) {
      frames.release();
    }

    final OuterSenderSubscriber senderSubscriber = this.senderSubscriber;
    // null is only possible if the cancel signal appears in the middle of the reassembling
    if (senderSubscriber != null) {
      final Payload firstPayload = senderSubscriber.firstPayload;
      if (firstPayload != null && firstPayload.refCnt() > 0) {
        firstPayload.release();
      }
    }
  }

  @Override
  public void onNext(Payload p) {
    if (this.requested == STATE_TERMINATED || this.done) {
      // payload from network so it has refCnt > 0
      p.release();
      return;
    }

    this.actual.onNext(p);
  }

  @Override
  public void onError(Throwable t) {
    if (this.done) {
      Operators.onErrorDropped(t, currentContext());
      return;
    }

    this.done = true;
    this.t = t;

    final CompositeByteBuf frames = this.frames;
    this.frames = null;

    long state = REQUESTED.getAndSet(this, STATE_TERMINATED);
    if (state == STATE_TERMINATED) {
      Operators.onErrorDropped(t, currentContext());
      return;
    }

    this.activeStreams.remove(this.streamId, this);

    if (frames != null && frames.refCnt() > 0) {
      frames.release();
    }

    if ((state & FLAG_FIRST) != FLAG_FIRST) {
      final Payload firstPayload = this.senderSubscriber.firstPayload;
      if (firstPayload != null && firstPayload.refCnt() > 0) {
        firstPayload.release();
      }
    }

    if ((state & MASK_FLAGS) != FLAG_UNSUBSCRIBED) {
      this.actual.onError(t);
    }

    // this is downstream subscription so need to cancel it just in case error signal has not
    // reached it
    // needs for disconnected upstream and downstream case
    this.s.cancel();
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
      state = this.requested;

      if (state == STATE_TERMINATED) {
        return;
      }

      terminal = (state & FLAG_HALF_CLOSED) == FLAG_HALF_CLOSED && this.senderSubscriber.done;

      if (REQUESTED.compareAndSet(
          this, state, terminal ? STATE_TERMINATED : (state | FLAG_HALF_CLOSED))) {
        break;
      }
    }

    if (terminal) {
      this.activeStreams.remove(this.streamId, this);
    }

    if ((state & FLAG_FIRST) == FLAG_FIRST) {
      this.actual.onComplete();
    }
  }

  @Override
  public boolean isReassemblingNow() {
    return this.frames != null;
  }

  @Override
  public void reassemble(ByteBuf followingFrame, boolean hasFollows, boolean terminal) {
    if (this.requested == STATE_TERMINATED || this.done) {
      return;
    }

    if (this.frames == null) {
      final CompositeByteBuf frames =
          ReassemblyUtils.addFollowingFrame(this.allocator.compositeBuffer(), followingFrame);
      this.frames = frames;

      if (this.requested == STATE_TERMINATED || this.done) {
        this.frames = null;
        ReferenceCountUtil.safeRelease(frames);
      }
    } else {
      final CompositeByteBuf frames =
          ReassemblyUtils.addFollowingFrame(this.frames, followingFrame);

      if (!hasFollows) {
        Payload payload;
        try {
          payload = this.payloadDecoder.apply(frames);
          frames.release();
          this.frames = null;
        } catch (Throwable t) {
          ReferenceCountUtil.safeRelease(frames);
          Exceptions.throwIfFatal(t);
          this.cancel();
          // send error to terminate interaction
          final ByteBuf errorFrame = ErrorFrameFlyweight.encode(this.allocator, this.streamId, t);
          this.sendProcessor.onNext(errorFrame);
          return;
        }

        if (this.senderSubscriber == null) {
          final OuterSenderSubscriber senderSubscriber =
              new OuterSenderSubscriber(
                  this,
                  payload,
                  this.payloadDecoder,
                  this.mtu,
                  this.sendProcessor,
                  this.activeStreams,
                  this.allocator,
                  this.errorConsumer);
          this.senderSubscriber = senderSubscriber;

          Flux<Payload> source = this.handler.requestChannel(payload, this);
          source.subscribe(senderSubscriber);
        } else {
          this.onNext(payload);
        }

        if (terminal) {
          this.onComplete();
        }
      }
    }
  }

  static final class OuterSenderSubscriber implements CoreSubscriber<Payload>, Subscription {

    final RequestChannelSubscriber parent;
    final PayloadDecoder payloadDecoder;
    final UnboundedProcessor<ByteBuf> sendProcessor;
    final IntObjectMap<Reassemble<?>> activeStreams;
    final ByteBufAllocator allocator;
    final Consumer<? super Throwable> errorConsumer;
    final int mtu;

    Payload firstPayload;

    boolean done;

    OuterSenderSubscriber(
        RequestChannelSubscriber parent,
        Payload firstPayload,
        PayloadDecoder payloadDecoder,
        int mtu,
        UnboundedProcessor<ByteBuf> sendProcessor,
        IntObjectMap<Reassemble<?>> activeStreams,
        ByteBufAllocator allocator,
        Consumer<? super Throwable> errorConsumer) {
      this.parent = parent;
      this.payloadDecoder = payloadDecoder;
      this.mtu = mtu;
      this.sendProcessor = sendProcessor;
      this.activeStreams = activeStreams;
      this.allocator = allocator;
      this.errorConsumer = errorConsumer;

      this.firstPayload = firstPayload;
    }

    @Override
    public void onSubscribe(Subscription subscription) {
      this.parent.onSubscribe(subscription);
    }

    @Override
    public void onNext(Payload p) {
      final RequestChannelSubscriber parent = this.parent;
      if (parent.requested == STATE_TERMINATED || this.done) {
        ReferenceCountUtil.safeRelease(parent);
        return;
      }

      final int streamId = parent.streamId;
      final UnboundedProcessor<ByteBuf> sender = this.sendProcessor;
      final ByteBufAllocator allocator = this.allocator;

      if (p.refCnt() <= 0) {
        parent.cancel();

        final IllegalReferenceCountException t = new IllegalReferenceCountException(0);
        this.errorConsumer.accept(t);
        final ByteBuf errorFrame = ErrorFrameFlyweight.encode(allocator, streamId, t);
        sender.onNext(errorFrame);

        return;
      }

      try {
        final int mtu = this.mtu;
        final boolean hasMetadata = p.hasMetadata();
        final ByteBuf data = p.data();
        final ByteBuf metadata = p.metadata();

        if (hasMetadata ? !isValid(mtu, data, metadata) : !isValid(mtu, data)) {
          parent.cancel();
          p.release();

          final Throwable t = new IllegalArgumentException("Too Big Payload size");
          this.errorConsumer.accept(t);

          // upstream will be terminated due to spec
          final ByteBuf errorFrame = ErrorFrameFlyweight.encode(allocator, streamId, t);
          sender.onNext(errorFrame);

          return;
        }

        if (hasMetadata
            ? isFragmentable(mtu, data, metadata)
            : isFragmentable(mtu, data, metadata)) {
          final ByteBuf slicedData = data.slice();
          final ByteBuf slicedMetadata = hasMetadata ? metadata.slice() : Unpooled.EMPTY_BUFFER;

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
          final ByteBuf slicedData = data.retainedSlice();
          final ByteBuf slicedMetadata = hasMetadata ? metadata.retainedSlice() : null;

          final ByteBuf nextFrame =
              PayloadFrameFlyweight.encode(
                  allocator, streamId, false, false, true, slicedMetadata, slicedData);
          sender.onNext(nextFrame);
        }

        p.release();
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

      final RequestChannelSubscriber parent = this.parent;

      if (parent.requested == STATE_TERMINATED || this.done) {
        Operators.onErrorDropped(t, Context.empty());
        return;
      }

      final CompositeByteBuf frames = parent.frames;

      this.done = true;
      parent.frames = null;

      long state = REQUESTED.getAndSet(parent, STATE_TERMINATED);
      if (state == STATE_TERMINATED) {
        Operators.onErrorDropped(t, Context.empty());
        return;
      }

      if (frames != null && frames.refCnt() > 0) {
        frames.release();
      }

      if ((state & FLAG_FIRST) != FLAG_FIRST) {
        final Payload firstPayload = this.firstPayload;
        this.firstPayload = null;
        if (firstPayload != null && firstPayload.refCnt() > 0) {
          firstPayload.release();
        }
      }

      final int streamId = parent.streamId;
      this.activeStreams.remove(streamId, this.parent);

      final ByteBuf errorFrame = ErrorFrameFlyweight.encode(this.allocator, streamId, t);
      this.sendProcessor.onNext(errorFrame);
    }

    @Override
    public void onComplete() {
      final RequestChannelSubscriber parent = this.parent;

      long state = parent.requested;

      if (state == STATE_TERMINATED || this.done) {
        return;
      }

      this.done = true;

      boolean terminal;
      for (; ; ) {

        terminal = (state & FLAG_HALF_CLOSED) == FLAG_HALF_CLOSED && parent.done;

        if (REQUESTED.compareAndSet(
            parent, state, terminal ? STATE_TERMINATED : state | FLAG_HALF_CLOSED)) {
          break;
        }

        state = parent.requested;

        if (state == STATE_TERMINATED) {
          return;
        }
      }

      final int streamId = parent.streamId;

      if (terminal) {
        parent.activeStreams.remove(streamId, parent);
      }

      final ByteBuf completeFrame =
          PayloadFrameFlyweight.encodeComplete(this.allocator, parent.streamId);
      this.sendProcessor.onNext(completeFrame);
    }

    @Override
    public void request(long n) {
      final RequestChannelSubscriber parent = this.parent;
      long state = parent.requested;

      if (state == STATE_TERMINATED) {
        return;
      }

      final Payload firstPayload = this.firstPayload;

      boolean done = parent.done;
      boolean hasSentFirst;
      for (; ; ) {
        if (hasSentFirst = (state & FLAG_FIRST) == FLAG_FIRST) {
          // no need to send any frames anymore since the upstream is already done
          if (done) {
            return;
          }

          break;
        }

        this.firstPayload = null;

        if (REQUESTED.compareAndSet(parent, state, state | FLAG_FIRST)) {
          break;
        }

        state = parent.requested;
        done = parent.done;

        if (state == STATE_TERMINATED) {
          // to ensure that we have not missed anything in racing
          if (firstPayload != null && firstPayload.refCnt() > 0) {
            firstPayload.release();
          }
          return;
        }
      }

      if (!hasSentFirst) {
        // decrement first requested item immediately
        n--;
        final CoreSubscriber<? super Payload> actual = parent.actual;

        actual.onNext(firstPayload);

        if (done) {
          Throwable t = parent.t;
          if (t != null) {
            actual.onError(t);
          } else {
            actual.onComplete();
          }
        }
      }

      if (n > 0) {
        final ByteBuf requestNFrame =
            RequestNFrameFlyweight.encode(this.allocator, parent.streamId, n);
        this.sendProcessor.onNext(requestNFrame);
      }
    }

    @Override
    // upstream cancellation
    public void cancel() {
      final RequestChannelSubscriber parent = this.parent;

      long state = parent.requested;

      if (state == STATE_TERMINATED || parent.done) {
        return;
      }

      parent.done = true;

      final Payload firstPayload = this.firstPayload;

      boolean terminal;
      for (; ; ) {
        this.firstPayload = null;

        terminal = (state & FLAG_HALF_CLOSED) == FLAG_HALF_CLOSED && this.done;

        if (REQUESTED.compareAndSet(
            parent, state, terminal ? STATE_TERMINATED : state | FLAG_FIRST | FLAG_HALF_CLOSED)) {
          break;
        }

        state = parent.requested;

        if (state == STATE_TERMINATED) {
          return;
        }
      }

      if ((state & FLAG_FIRST) != FLAG_FIRST) {
        if (firstPayload != null && firstPayload.refCnt() > 0) {
          firstPayload.release();
        }
      }

      final int streamId = parent.streamId;

      if (terminal) {
        parent.activeStreams.remove(streamId, parent);
      }

      final ByteBuf completeFrame = CancelFrameFlyweight.encode(this.allocator, streamId);
      this.sendProcessor.onNext(completeFrame);
    }
  }
}
