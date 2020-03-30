package io.rsocket;

import static io.rsocket.fragmentation.FragmentationUtils.isFragmentable;
import static io.rsocket.fragmentation.FragmentationUtils.isValid;

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
import java.time.Duration;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;
import reactor.core.Scannable;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Operators;
import reactor.util.annotation.NonNull;
import reactor.util.annotation.Nullable;

final class FireAndForgetMono extends Mono<Void> implements Scannable {

  volatile int once;

  @SuppressWarnings("rawtypes")
  static final AtomicIntegerFieldUpdater<FireAndForgetMono> ONCE =
      AtomicIntegerFieldUpdater.newUpdater(FireAndForgetMono.class, "once");

  final ByteBufAllocator allocator;
  final Payload payload;
  final int mtu;
  final StateAware parent;
  final StreamIdSupplier streamIdSupplier;
  final IntObjectMap<?> activeStreams;
  final UnboundedProcessor<ByteBuf> sendProcessor;

  FireAndForgetMono(
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
    final Payload p = this.payload;

    if (p.refCnt() > 0) {
      if (once == 0 && ONCE.compareAndSet(this, 0, 1)) {
        try {
          final boolean hasMetadata = p.hasMetadata();
          final ByteBuf data = p.data();
          final ByteBuf metadata = p.metadata();
          final int mtu = this.mtu;

          if (hasMetadata ? !isValid(mtu, data, metadata) : !isValid(mtu, data)) {
            Operators.error(actual, new IllegalArgumentException("Too Big Payload size"));
          } else {
            final Throwable throwable = parent.checkAvailable();
            if (throwable != null) {
              Operators.error(actual, throwable);
            } else {
              final int streamId = this.streamIdSupplier.nextStreamId(this.activeStreams);
              final UnboundedProcessor<ByteBuf> sender = this.sendProcessor;
              final ByteBufAllocator allocator = this.allocator;

              if (hasMetadata ? isFragmentable(mtu, data, metadata) : isFragmentable(mtu, data)) {
                final ByteBuf slicedData = data.slice();
                final ByteBuf slicedMetadata =
                    hasMetadata ? metadata.slice() : Unpooled.EMPTY_BUFFER;

                final ByteBuf first =
                    FragmentationUtils.encodeFirstFragment(
                        allocator,
                        mtu,
                        FrameType.REQUEST_FNF,
                        streamId,
                        slicedMetadata,
                        slicedData);
                sender.onNext(first);

                while (slicedData.isReadable() || slicedMetadata.isReadable()) {
                  ByteBuf following =
                      FragmentationUtils.encodeFollowsFragment(
                          allocator, mtu, streamId, false, slicedMetadata, slicedData);
                  sender.onNext(following);
                }
              } else {
                final ByteBuf slicedRetainedData = data.retainedSlice();
                final ByteBuf slicedRetainedMetadata =
                    hasMetadata ? metadata.retainedSlice() : null;

                final ByteBuf requestFrame =
                    RequestFireAndForgetFrameFlyweight.encode(
                        allocator, streamId, false, slicedRetainedMetadata, slicedRetainedData);
                sender.onNext(requestFrame);
              }

              Operators.complete(actual);
            }
          }
        } catch (Throwable e) {
          Operators.error(actual, e);
        }
      } else {
        Operators.error(
            actual,
            new IllegalStateException("UnicastFireAndForgetMono allows only a single Subscriber"));
      }

      p.release();
    } else {
      Operators.error(actual, new IllegalReferenceCountException(0));
    }
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
      if (this.payload.refCnt() > 0) {
        try {
          ByteBuf data = this.payload.data().retainedSlice();
          ByteBuf metadata =
              this.payload.hasMetadata() ? this.payload.metadata().retainedSlice() : null;

          int streamId = this.streamIdSupplier.nextStreamId(this.activeStreams);

          ByteBuf requestFrame =
              RequestFireAndForgetFrameFlyweight.encode(
                  this.allocator, streamId, false, metadata, data);

          this.sendProcessor.onNext(requestFrame);
          return null;
        } finally {
          ReferenceCountUtil.safeRelease(this.payload);
        }
      } else {
        return null;
      }
    } else {
      ReferenceCountUtil.safeRelease(this.payload);
      throw Exceptions.propagate(throwable);
    }
  }

  @Override
  public Object scanUnsafe(Scannable.Attr key) {
    return null; // no particular key to be represented, still useful in hooks
  }

  @Override
  @NonNull
  public String stepName() {
    return "source(UnicastFireAndForgetMono)";
  }
}
