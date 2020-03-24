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
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Operators;
import reactor.util.context.Context;

import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.function.Consumer;

public class RequestChannelSubscriber implements CoreSubscriber<Payload>, Reassemble {

  final int streamId;
  final long firstRequest;
  final ByteBufAllocator allocator;
  final Consumer<? super Throwable> errorConsumer;
  final PayloadDecoder payloadDecoder;
  final int mtu;
  final IntObjectMap<? super Subscription> activeStreams;
  final UnboundedProcessor<ByteBuf> sendProcessor;

  final RSocket handler;
  final CompositeByteBuf frames;

  volatile long requested;
  static final AtomicLongFieldUpdater<RequestChannelSubscriber> REQUESTED =
      AtomicLongFieldUpdater.newUpdater(RequestChannelSubscriber.class, "requested");

  static final long STATE_TERMINATED = Long.MIN_VALUE;
  static final long STATE_SUBSCRIBED_RECEIVED_MAX = -2;
  static final long STATE_SUBSCRIBED = -1;

  Subscription s;

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
          RSocket handler) {
    this.streamId = streamId;
    this.firstRequest = firstRequest;
    this.allocator = allocator;
    this.mtu = mtu;
    this.errorConsumer = errorConsumer;
    this.activeStreams = activeStreams;
    this.sendProcessor = sendProcessor;
    this.payloadDecoder = payloadDecoder;
    this.handler = handler;
    this.frames = allocator.compositeBuffer().addComponent(true, firstFrame);
  }

  public RequestChannelSubscriber(
      int streamId,
      long firstRequest,
      Payload firstPayload,
      ByteBufAllocator allocator,
      int mtu,
      Consumer<? super Throwable> errorConsumer,
      IntObjectMap<? super Subscription> activeStreams,
      UnboundedProcessor<ByteBuf> sendProcessor) {
    this.streamId = streamId;
    this.firstRequest = firstRequest;
    this.allocator = allocator;
    this.mtu = mtu;
    this.errorConsumer = errorConsumer;
    this.activeStreams = activeStreams;
    this.sendProcessor = sendProcessor;

    this.payloadDecoder = null;
    this.handler = null;
    this.frames = null;
  }

  @Override
  public void onSubscribe(Subscription subscription) {
    final long firstRequest = this.firstRequest;

    long requested;
    long next;
    for (; ; ) {
      requested = this.requested;

      if (requested <= STATE_SUBSCRIBED) {
        subscription.cancel();
        return;
      }

      next = Operators.addCap(firstRequest, requested);

      this.s = subscription;
      if (REQUESTED.compareAndSet(
          this,
          requested,
          next == Long.MAX_VALUE ? STATE_SUBSCRIBED_RECEIVED_MAX : STATE_SUBSCRIBED)) {
        subscription.request(next);
        return;
      }
    }
  }

  @Override
  public void onNext(Payload p) {
    if (this.requested == STATE_TERMINATED) {
      ReferenceCountUtil.safeRelease(p);
      return;
    }

    final int streamId = this.streamId;
    final UnboundedProcessor<ByteBuf> sender = this.sendProcessor;
    final ByteBufAllocator allocator = this.allocator;

    if (p.refCnt() <= 0) {
      this.cancel();

      final IllegalReferenceCountException t = new IllegalReferenceCountException(0);
      final ByteBuf errorFrame =
          ErrorFrameFlyweight.encode(allocator, streamId, t);
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
          this.cancel();

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
      this.cancel();
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

    if (REQUESTED.getAndSet(this, STATE_TERMINATED) == STATE_TERMINATED) {
      Operators.onErrorDropped(t, Context.empty());
      return;
    }

    final int streamId = this.streamId;

    this.activeStreams.remove(streamId, this);

    final ByteBuf errorFrame = ErrorFrameFlyweight.encode(this.allocator, streamId, t);
    this.sendProcessor.onNext(errorFrame);
  }

  @Override
  public void onComplete() {
    if (REQUESTED.getAndSet(this, STATE_TERMINATED) == STATE_TERMINATED) {
      return;
    }

    final int streamId = this.streamId;

    this.activeStreams.remove(streamId, this);

    final ByteBuf completeFrame = PayloadFrameFlyweight.encodeComplete(this.allocator, streamId);
    this.sendProcessor.onNext(completeFrame);
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

      if (current == STATE_SUBSCRIBED) {
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

    if (state <= STATE_SUBSCRIBED) {
      this.activeStreams.remove(this.streamId, this);
      this.s.cancel();
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
      try {
        Flux<Payload> source = this.handler.requestStream(this.payloadDecoder.apply(frames));
        ReferenceCountUtil.safeRelease(frames);
        source.subscribe(this);
      } catch (Throwable t) {
        ReferenceCountUtil.safeRelease(frames);
        Exceptions.throwIfFatal(t);
        this.onError(t);
      }
    }
  }
}
