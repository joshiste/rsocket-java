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
import io.rsocket.frame.FrameType;
import io.rsocket.frame.RequestResponseFrameFlyweight;
import io.rsocket.frame.decoder.PayloadDecoder;
import io.rsocket.internal.UnboundedProcessor;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.Exceptions;
import reactor.core.Scannable;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Operators;
import reactor.util.annotation.NonNull;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

final class RequestResponseMono extends Mono<Payload> implements Reassemble<Payload>, Scannable {

  final ByteBufAllocator allocator;
  final Payload payload;
  final int mtu;
  final StateAware parent;
  final StreamIdSupplier streamIdSupplier;
  final IntObjectMap<Reassemble<?>> activeSubscriber;
  final UnboundedProcessor<ByteBuf> sendProcessor;
  final PayloadDecoder payloadDecoder;

  static final int STATE_UNSUBSCRIBED = 0;
  static final int STATE_SUBSCRIBED = 1;
  static final int STATE_REQUESTED = 2;
  static final int STATE_TERMINATED = 3;

  volatile int state;
  static final AtomicIntegerFieldUpdater<RequestResponseMono> STATE =
      AtomicIntegerFieldUpdater.newUpdater(RequestResponseMono.class, "state");

  int streamId;
  CoreSubscriber<? super Payload> actual;
  CompositeByteBuf frames;

  RequestResponseMono(
      ByteBufAllocator allocator,
      Payload payload,
      int mtu,
      StateAware parent,
      StreamIdSupplier streamIdSupplier,
      IntObjectMap<Reassemble<?>> activeSubscribers,
      UnboundedProcessor<ByteBuf> sendProcessor,
      PayloadDecoder payloadDecoder) {

    this.allocator = allocator;
    this.payload = payload;
    this.mtu = mtu;
    this.parent = parent;
    this.streamIdSupplier = streamIdSupplier;
    this.activeSubscriber = activeSubscribers;
    this.sendProcessor = sendProcessor;
    this.payloadDecoder = payloadDecoder;
  }

  @Override
  @NonNull
  public Context currentContext() {
    int state = this.state;

    if (state >= STATE_SUBSCRIBED) {
      return this.actual.currentContext();
    }

    return Context.empty();
  }

  @Override
  public final void onSubscribe(Subscription subscription) {
    subscription.cancel();
    // TODO: Add logging
  }

  @Override
  public final void onComplete() {
    onNext(null);
  }

  @Override
  public final void onError(Throwable cause) {
    if (this.state == STATE_TERMINATED) {
      Operators.onErrorDropped(cause, currentContext());
      return;
    }

    final CompositeByteBuf frames = this.frames;
    this.frames = null;

    if (STATE.getAndSet(this, STATE_TERMINATED) == STATE_TERMINATED) {
      Operators.onErrorDropped(cause, currentContext());
      return;
    }

    if (frames != null && frames.refCnt() > 0) {
      frames.release();
    }

    this.activeSubscriber.remove(this.streamId, this);

    this.actual.onError(cause);
  }

  @Override
  public final void onNext(@Nullable Payload value) {
    if (this.state == STATE_TERMINATED
        || STATE.getAndSet(this, STATE_TERMINATED) == STATE_TERMINATED) {
      if (value != null) {
        value.release();
      }
      return;
    }

    final CoreSubscriber<? super Payload> a = this.actual;

    this.activeSubscriber.remove(this.streamId, this);

    if (value != null) {
      a.onNext(value);
    }
    a.onComplete();
  }

  @Override
  public void subscribe(CoreSubscriber<? super Payload> actual) {
    final Payload p = this.payload;

    if (p.refCnt() > 0) {
      if (this.state == STATE_UNSUBSCRIBED
          && STATE.compareAndSet(this, STATE_UNSUBSCRIBED, STATE_SUBSCRIBED)) {
        this.actual = actual;

        final int mtu = this.mtu;
        final boolean hasMetadata = p.hasMetadata();
        final ByteBuf data = p.data();
        final ByteBuf metadata = p.metadata();

        if (hasMetadata ? !isValid(mtu, data, metadata) : !isValid(mtu, data)) {
          Operators.error(actual, new IllegalArgumentException("Too Big Payload size"));
          p.release();
          return;
        }

        // call onSubscribe if has value in the result or no result delivered so far
        actual.onSubscribe(this);
      } else {
        Operators.error(
            actual,
            new IllegalStateException("RequestResponseMono allows only a single Subscriber"));
      }
    } else {
      Operators.error(actual, new IllegalReferenceCountException(0));
    }
  }

  @Override
  public final void request(long n) {
    if (Operators.validate(n)
        && this.state == STATE_SUBSCRIBED
        && STATE.compareAndSet(this, STATE_SUBSCRIBED, STATE_REQUESTED)) {

      final Throwable throwable = this.parent.checkAvailable();
      if (throwable != null) {
        this.state = STATE_TERMINATED;
        this.actual.onError(throwable);
        return;
      }

      final IntObjectMap<Reassemble<?>> as = this.activeSubscriber;
      final Payload p = this.payload;
      final int mtu = this.mtu;
      final UnboundedProcessor<ByteBuf> sender = this.sendProcessor;
      final ByteBufAllocator allocator = this.allocator;

      try {
        final boolean hasMetadata = p.hasMetadata();
        final ByteBuf data = p.data();
        final ByteBuf metadata = p.metadata();
        final int streamId = this.streamIdSupplier.nextStreamId(as);

        this.streamId = streamId;

        if (hasMetadata ? isFragmentable(mtu, data, metadata) : isFragmentable(mtu, data)) {
          final ByteBuf slicedData = data.slice();
          final ByteBuf slicedMetadata = hasMetadata ? metadata.slice() : Unpooled.EMPTY_BUFFER;
          final ByteBuf first =
              FragmentationUtils.encodeFirstFragment(
                  allocator, mtu, FrameType.REQUEST_RESPONSE, streamId, slicedMetadata, slicedData);

          as.put(streamId, this);
          sender.onNext(first);

          while (slicedData.isReadable() || slicedMetadata.isReadable()) {
            final ByteBuf following =
                FragmentationUtils.encodeFollowsFragment(
                    allocator, mtu, streamId, false, slicedMetadata, slicedData);
            sender.onNext(following);
          }
        } else {
          final ByteBuf slicedRetainedData = data.retainedSlice();
          final ByteBuf slicedRetainedMetadata = hasMetadata ? metadata.retainedSlice() : null;
          final ByteBuf requestFrame =
              RequestResponseFrameFlyweight.encode(
                  allocator, streamId, false, slicedRetainedMetadata, slicedRetainedData);

          as.put(streamId, this);
          sender.onNext(requestFrame);
        }

        p.release();
      } catch (Throwable e) {
        this.state = STATE_TERMINATED;

        ReferenceCountUtil.safeRelease(p);
        Exceptions.throwIfFatal(e);

        final int streamId = this.streamId;
        if (as.remove(streamId, this)) {
          final ByteBuf cancelFrame = CancelFrameFlyweight.encode(allocator, streamId);
          sender.onNext(cancelFrame);
        }

        this.actual.onError(e);
      }
    }
  }

  @Override
  public final void cancel() {
    int state = this.state;
    if (state == STATE_TERMINATED) {
      return;
    }

    final CompositeByteBuf frames = this.frames;
    this.frames = null;

    if (STATE.getAndSet(this, STATE_TERMINATED) != STATE_TERMINATED) {
      if (state == STATE_SUBSCRIBED) {
        this.payload.release();
      } else {
        if (frames != null && frames.refCnt() > 0) {
          frames.release();
        }

        final int streamId = this.streamId;
        this.activeSubscriber.remove(streamId, this);
        this.sendProcessor.onNext(CancelFrameFlyweight.encode(this.allocator, streamId));
      }
    }
  }

  @Override
  public boolean isReassemblingNow() {
    return this.frames != null;
  }

  @Override
  public void reassemble(ByteBuf followingFrame, boolean hasFollows, boolean terminal) {
    if (this.state == STATE_TERMINATED) {
      return;
    }

    CompositeByteBuf frames = this.frames;

    if (frames == null) {
      frames = ReassemblyUtils.addFollowingFrame(this.allocator.compositeBuffer(), followingFrame);
      this.frames = frames;
      if (this.state == STATE_TERMINATED) {
        this.frames = null;
        ReferenceCountUtil.safeRelease(frames);
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
      } catch (Throwable t) {
        ReferenceCountUtil.safeRelease(frames);
        Exceptions.throwIfFatal(t);
        // sends cancel frame to prevent any further frames
        this.cancel();
        // terminates downstream
        this.actual.onError(t);
      }
    }
  }

  @Override
  @Nullable
  public Object scanUnsafe(Attr key) {
    // touch guard
    int state = this.state;

    if (key == Attr.TERMINATED) return state == STATE_TERMINATED;
    if (key == Attr.PREFETCH) return Integer.MAX_VALUE;

    return null;
  }

  @Override
  @NonNull
  public String stepName() {
    return "source(RequestResponseMono)";
  }
}
