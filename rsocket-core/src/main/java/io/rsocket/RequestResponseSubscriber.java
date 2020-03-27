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
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Consumer;
import org.reactivestreams.Subscription;
import reactor.core.Exceptions;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Operators;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

public class RequestResponseSubscriber implements Reassemble<Payload> {

  final int streamId;
  final ByteBufAllocator allocator;
  final Consumer<? super Throwable> errorConsumer;
  final PayloadDecoder payloadDecoder;
  final int mtu;
  final IntObjectMap<Reassemble<?>> activeStreams;
  final UnboundedProcessor<ByteBuf> sendProcessor;

  final RSocket handler;
  final CompositeByteBuf frames;

  volatile Subscription s;
  static final AtomicReferenceFieldUpdater<RequestResponseSubscriber, Subscription> S =
      AtomicReferenceFieldUpdater.newUpdater(
          RequestResponseSubscriber.class, Subscription.class, "s");

  public RequestResponseSubscriber(
      int streamId,
      ByteBufAllocator allocator,
      PayloadDecoder payloadDecoder,
      ByteBuf firstFrame,
      int mtu,
      Consumer<? super Throwable> errorConsumer,
      IntObjectMap<Reassemble<?>> activeStreams,
      UnboundedProcessor<ByteBuf> sendProcessor,
      RSocket handler) {
    this.streamId = streamId;
    this.allocator = allocator;
    this.errorConsumer = errorConsumer;
    this.mtu = mtu;
    this.activeStreams = activeStreams;
    this.sendProcessor = sendProcessor;
    this.payloadDecoder = payloadDecoder;
    this.handler = handler;
    this.frames = allocator.compositeBuffer().addComponent(true, firstFrame);
  }

  public RequestResponseSubscriber(
      int streamId,
      ByteBufAllocator allocator,
      int mtu,
      Consumer<? super Throwable> errorConsumer,
      IntObjectMap<Reassemble<?>> activeStreams,
      UnboundedProcessor<ByteBuf> sendProcessor) {
    this.streamId = streamId;
    this.allocator = allocator;
    this.errorConsumer = errorConsumer;
    this.mtu = mtu;
    this.activeStreams = activeStreams;
    this.sendProcessor = sendProcessor;

    this.payloadDecoder = null;
    this.handler = null;
    this.frames = null;
  }

  @Override
  public void onSubscribe(Subscription subscription) {
    if (Operators.setOnce(S, this, subscription)) {
      subscription.request(Long.MAX_VALUE);
    }
  }

  @Override
  public void onNext(@Nullable Payload p) {
    if (!Operators.terminate(S, this)) {
      if (p != null) {
        ReferenceCountUtil.safeRelease(p);
      }
      return;
    }

    this.activeStreams.remove(streamId, this);

    final int streamId = this.streamId;
    final UnboundedProcessor<ByteBuf> sender = this.sendProcessor;
    final ByteBufAllocator allocator = this.allocator;

    if (p != null) {
      if (p.refCnt() <= 0) {
        final IllegalReferenceCountException t = new IllegalReferenceCountException(0);
        final ByteBuf errorFrame = ErrorFrameFlyweight.encode(allocator, streamId, t);
        errorConsumer.accept(t);
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
                  allocator, mtu, FrameType.NEXT_COMPLETE, streamId, slicedMetadata, slicedData);

          sender.onNext(first);

          while (slicedData.isReadable() || slicedMetadata.isReadable()) {
            final ByteBuf following =
                FragmentationUtils.encodeFollowsFragment(
                    allocator, mtu, streamId, true, slicedMetadata, slicedData);
            sender.onNext(following);
          }

        } else {
          final ByteBuf data = p.data();
          final ByteBuf metadata = p.metadata();

          if (((data.readableBytes() + (hasMetadata ? metadata.readableBytes() : 0))
                  & ~FrameLengthFlyweight.FRAME_LENGTH_MASK)
              != 0) {
            final Throwable t = new IllegalArgumentException("Too Big Payload size");
            final ByteBuf errorFrame = ErrorFrameFlyweight.encode(allocator, streamId, t);
            errorConsumer.accept(t);
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
        ReferenceCountUtil.safeRelease(p);
        Exceptions.throwIfFatal(t);

        errorConsumer.accept(t);

        final ByteBuf errorFrame = ErrorFrameFlyweight.encode(allocator, streamId, t);
        sender.onNext(errorFrame);
      }
    } else {
      final ByteBuf completeFrame = PayloadFrameFlyweight.encodeComplete(allocator, streamId);
      sender.onNext(completeFrame);
    }
  }

  @Override
  public void onError(Throwable t) {
    this.errorConsumer.accept(t);
    if (!Operators.terminate(S, this)) {
      Operators.onErrorDropped(t, Context.empty());
      return;
    }

    final CompositeByteBuf frames = this.frames;
    if (frames != null && frames.refCnt() > 0) {
      ReferenceCountUtil.safeRelease(frames);
    }

    final int streamId = this.streamId;

    this.activeStreams.remove(streamId, this);

    final ByteBuf errorFrame = ErrorFrameFlyweight.encode(this.allocator, streamId, t);
    this.sendProcessor.onNext(errorFrame);
  }

  @Override
  public void onComplete() {
    onNext(null);
  }

  @Override
  public void request(long n) {
    // noop
  }

  @Override
  public void cancel() {
    if (!Operators.terminate(S, this)) {
      return;
    }

    final CompositeByteBuf frames = this.frames;
    if (frames != null && frames.refCnt() > 0) {
      ReferenceCountUtil.safeRelease(frames);
    }

    this.activeStreams.remove(this.streamId, this);
  }

  @Override
  public boolean isReassemblingNow() {
    return this.frames != null;
  }

  @Override
  public void reassemble(ByteBuf dataAndMetadata, boolean hasFollows, boolean terminal) {
    final CompositeByteBuf frames = this.frames.addComponent(true, dataAndMetadata);

    if (!hasFollows) {
      try {
        Mono<Payload> source = this.handler.requestResponse(this.payloadDecoder.apply(frames));
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
