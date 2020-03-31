package io.rsocket;

import static io.rsocket.fragmentation.FragmentationUtils.isValid;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.util.IllegalReferenceCountException;
import io.rsocket.frame.MetadataPushFrameFlyweight;
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

final class MetadataPushMono extends Mono<Void> implements Scannable {

  volatile int once;

  @SuppressWarnings("rawtypes")
  static final AtomicIntegerFieldUpdater<MetadataPushMono> ONCE =
      AtomicIntegerFieldUpdater.newUpdater(MetadataPushMono.class, "once");

  final ByteBufAllocator allocator;
  final Payload payload;
  final int mtu;
  final StateAware parent;
  final UnboundedProcessor<ByteBuf> sendProcessor;

  MetadataPushMono(
      @NonNull ByteBufAllocator allocator,
      @NonNull Payload payload,
      int mtu,
      @NonNull StateAware parent,
      @NonNull UnboundedProcessor<ByteBuf> sendProcessor) {
    this.allocator = allocator;
    this.payload = payload;
    this.mtu = mtu;
    this.parent = parent;
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

          if (!hasMetadata) {
            Operators.error(
                actual, new IllegalArgumentException("Payload must contains metadata to push"));
          } else if (data.isReadable()) {
            Operators.error(
                actual,
                new IllegalArgumentException(
                    "Found readable data in Payload but expected present metadata only"));
          } else if (!isValid(0, metadata)) {
            Operators.error(
                actual,
                new IllegalArgumentException(
                    "Too Big Payload size"
                        + (this.mtu > 0
                            ? "Notice, Metadata Push is non-fragmentable frame type"
                            : "")));
          } else {
            final Throwable throwable = this.parent.error();
            if (throwable != null) {
              Operators.error(actual, throwable);
            } else {
              final ByteBuf slicedRetainedMetadata = metadata.retainedSlice();

              final ByteBuf requestFrame =
                  MetadataPushFrameFlyweight.encode(this.allocator, slicedRetainedMetadata);
              this.sendProcessor.onNext(requestFrame);

              Operators.complete(actual);
            }
          }
        } catch (Throwable e) {
          Operators.error(actual, e);
        }
      } else {
        Operators.error(
            actual, new IllegalStateException("MetadataPushMono allows only a single Subscriber"));
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
    final Payload p = this.payload;

    if (p.refCnt() > 0) {
      if (once == 0 && ONCE.compareAndSet(this, 0, 1)) {
        try {
          final boolean hasMetadata = p.hasMetadata();
          final ByteBuf data = p.data();
          final ByteBuf metadata = p.metadata();

          if (!hasMetadata) {
            p.release();
            throw new IllegalArgumentException("Payload must contains metadata to push");
          } else if (data.isReadable()) {
            p.release();
            throw new IllegalArgumentException(
                "Found readable data in Payload but expected present metadata only");
          } else if (!isValid(0, metadata)) {
            p.release();
            throw new IllegalArgumentException(
                "Too Big Payload size"
                    + (this.mtu > 0 ? "Notice, Metadata Push is non-fragmentable frame type" : ""));
          } else {
            final Throwable throwable = this.parent.error();
            if (throwable != null) {
              p.release();
              throw throwable;
            } else {
              final ByteBuf slicedRetainedMetadata = metadata.retainedSlice();

              final ByteBuf requestFrame =
                  MetadataPushFrameFlyweight.encode(this.allocator, slicedRetainedMetadata);
              this.sendProcessor.onNext(requestFrame);

              p.release();

              return null;
            }
          }
        } catch (Throwable e) {
          p.release();
          throw Exceptions.propagate(e);
        }
      } else {
        p.release();
        throw new IllegalStateException("MetadataPushMono allows only a single Subscriber");
      }
    } else {
      throw new IllegalReferenceCountException(0);
    }
  }

  @Override
  public Object scanUnsafe(Attr key) {
    return null; // no particular key to be represented, still useful in hooks
  }

  @Override
  @NonNull
  public String stepName() {
    return "source(MetadataPushMono)";
  }
}
