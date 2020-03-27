package io.rsocket;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.collection.IntObjectMap;
import io.rsocket.frame.decoder.PayloadDecoder;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.function.Consumer;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Operators;
import reactor.util.context.Context;

final class FireAndForgetSubscriber implements CoreSubscriber<Void>, Reassemble<Void> {

  final int streamId;
  final ByteBufAllocator allocator;
  final Consumer<? super Throwable> errorConsumer;
  final PayloadDecoder payloadDecoder;
  final IntObjectMap<Reassemble<?>> activeStreams;
  final RSocket handler;

  final CompositeByteBuf frames;

  volatile int state;
  static final AtomicIntegerFieldUpdater<FireAndForgetSubscriber> STATE =
      AtomicIntegerFieldUpdater.newUpdater(FireAndForgetSubscriber.class, "state");

  static final int STATE_TERMINATED = 1;

  FireAndForgetSubscriber(Consumer<? super Throwable> errorConsumer) {
    this.streamId = 0;
    this.allocator = null;
    this.errorConsumer = errorConsumer;
    this.payloadDecoder = null;
    this.activeStreams = null;
    this.handler = null;
    this.frames = null;
  }

  FireAndForgetSubscriber(
      int streamId,
      ByteBuf firstFrame,
      ByteBufAllocator allocator,
      PayloadDecoder payloadDecoder,
      Consumer<? super Throwable> errorConsumer,
      IntObjectMap<Reassemble<?>> activeStreams,
      RSocket handler) {
    this.streamId = streamId;
    this.allocator = allocator;
    this.errorConsumer = errorConsumer;
    this.payloadDecoder = payloadDecoder;
    this.activeStreams = activeStreams;
    this.handler = handler;

    this.frames = allocator.compositeBuffer().addComponent(true, firstFrame);
  }

  @Override
  public void onSubscribe(Subscription s) {
    s.request(Long.MAX_VALUE);
  }

  @Override
  public void onNext(Void voidVal) {}

  @Override
  public void onError(Throwable t) {
    final CompositeByteBuf frames = this.frames;
    if (frames != null && frames.refCnt() > 0) {
      ReferenceCountUtil.safeRelease(frames);
    }

    this.errorConsumer.accept(t);
    Operators.onErrorDropped(t, Context.empty());
  }

  @Override
  public void onComplete() {}

  @Override
  public boolean isReassemblingNow() {
    return this.frames != null;
  }

  @Override
  public void reassemble(ByteBuf dataAndMetadata, boolean hasFollows, boolean terminal) {
    if (state == STATE_TERMINATED) {
      ReferenceCountUtil.safeRelease(dataAndMetadata);
    }

    final CompositeByteBuf frames = this.frames.addComponent(true, dataAndMetadata);

    if (!hasFollows) {
      this.activeStreams.remove(this.streamId, this);
      try {
        Mono<Void> source = this.handler.fireAndForget(this.payloadDecoder.apply(frames));
        ReferenceCountUtil.safeRelease(frames);
        source.subscribe(this);
      } catch (Throwable t) {
        ReferenceCountUtil.safeRelease(frames);
        Exceptions.throwIfFatal(t);
        this.onError(t);
      }
    }
  }

  @Override
  public void request(long n) {}

  @Override
  public void cancel() {
    if (STATE.getAndSet(this, STATE_TERMINATED) == STATE_TERMINATED) {
      return;
    }

    final CompositeByteBuf frames = this.frames;
    if (frames != null && frames.refCnt() > 0) {
      ReferenceCountUtil.safeRelease(frames);
    }
  }
}
