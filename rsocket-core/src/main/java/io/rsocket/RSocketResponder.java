/*
 * Copyright 2015-2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.rsocket;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.collection.IntObjectMap;
import io.rsocket.frame.ErrorFrameFlyweight;
import io.rsocket.frame.FrameHeaderFlyweight;
import io.rsocket.frame.FrameType;
import io.rsocket.frame.RequestChannelFrameFlyweight;
import io.rsocket.frame.RequestNFrameFlyweight;
import io.rsocket.frame.RequestStreamFrameFlyweight;
import io.rsocket.frame.decoder.PayloadDecoder;
import io.rsocket.internal.SynchronizedIntObjectHashMap;
import io.rsocket.internal.UnboundedProcessor;
import io.rsocket.lease.ResponderLeaseHandler;
import java.util.function.Consumer;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import reactor.core.Disposable;
import reactor.core.Exceptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;

/** Responder side of RSocket. Receives {@link ByteBuf}s from a peer's {@link RSocketRequester} */
class RSocketResponder implements ResponderRSocket {

  private final DuplexConnection connection;
  private final RSocket requestHandler;
  private final ResponderRSocket responderRSocket;
  private final PayloadDecoder payloadDecoder;
  private final Consumer<Throwable> errorConsumer;
  private final ResponderLeaseHandler leaseHandler;

  private final IntObjectMap<Reassemble<?>> activeStreams;

  private final UnboundedProcessor<ByteBuf> sendProcessor;
  private final ByteBufAllocator allocator;
  private final FireAndForgetSubscriber noOpFireAndForgetSubscriber;

  private final int mtu;
  private final MetadataPushSubscriber metadataPushSubscriber;

  RSocketResponder(
      ByteBufAllocator allocator,
      DuplexConnection connection,
      RSocket requestHandler,
      PayloadDecoder payloadDecoder,
      Consumer<Throwable> errorConsumer,
      ResponderLeaseHandler leaseHandler,
      int mtu) {
    this.allocator = allocator;
    this.connection = connection;

    this.requestHandler = requestHandler;
    this.responderRSocket =
        (requestHandler instanceof ResponderRSocket) ? (ResponderRSocket) requestHandler : null;

    this.payloadDecoder = payloadDecoder;
    this.errorConsumer = errorConsumer;
    this.leaseHandler = leaseHandler;
    this.activeStreams = new SynchronizedIntObjectHashMap<>();

    // DO NOT Change the order here. The Send processor must be subscribed to before receiving
    // connections
    this.sendProcessor = new UnboundedProcessor<>();
    this.mtu = mtu;
    this.noOpFireAndForgetSubscriber = new FireAndForgetSubscriber(errorConsumer);
    this.metadataPushSubscriber = new MetadataPushSubscriber(errorConsumer);

    connection
        .send(sendProcessor)
        .doFinally(this::handleSendProcessorCancel)
        .subscribe(null, this::handleSendProcessorError);

    Disposable receiveDisposable = connection.receive().subscribe(this::handleFrame, errorConsumer);
    Disposable sendLeaseDisposable = leaseHandler.send(sendProcessor::onNextPrioritized);

    this.connection
        .onClose()
        .doFinally(
            s -> {
              cleanup();
              receiveDisposable.dispose();
              sendLeaseDisposable.dispose();
            })
        .subscribe(null, errorConsumer);
  }

  private void handleSendProcessorError(Throwable t) {
    for (Reassemble<?> reassemble : activeStreams.values()) {
      reassemble.onError(t);
    }
  }

  private void handleSendProcessorCancel(SignalType t) {
    if (SignalType.ON_ERROR == t) {
      return;
    }

    for (Reassemble<?> reassemble : activeStreams.values()) {
      reassemble.cancel();
    }
  }

  @Override
  public Mono<Void> fireAndForget(Payload payload) {
    try {
      if (leaseHandler.useLease()) {
        return requestHandler.fireAndForget(payload);
      } else {
        payload.release();
        return Mono.error(leaseHandler.leaseError());
      }
    } catch (Throwable t) {
      return Mono.error(t);
    }
  }

  @Override
  public Mono<Payload> requestResponse(Payload payload) {
    try {
      if (leaseHandler.useLease()) {
        return requestHandler.requestResponse(payload);
      } else {
        payload.release();
        return Mono.error(leaseHandler.leaseError());
      }
    } catch (Throwable t) {
      return Mono.error(t);
    }
  }

  @Override
  public Flux<Payload> requestStream(Payload payload) {
    try {
      if (leaseHandler.useLease()) {
        return requestHandler.requestStream(payload);
      } else {
        payload.release();
        return Flux.error(leaseHandler.leaseError());
      }
    } catch (Throwable t) {
      return Flux.error(t);
    }
  }

  @Override
  public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
    try {
      if (leaseHandler.useLease()) {
        return requestHandler.requestChannel(payloads);
      } else {
        return Flux.error(leaseHandler.leaseError());
      }
    } catch (Throwable t) {
      return Flux.error(t);
    }
  }

  @Override
  public Flux<Payload> requestChannel(Payload payload, Publisher<Payload> payloads) {
    try {
      if (leaseHandler.useLease()) {
        final ResponderRSocket responderRSocket = this.responderRSocket;
        if (responderRSocket != null) {
          return responderRSocket.requestChannel(payload, payloads);
        } else {
          return requestHandler.requestChannel(payloads);
        }
      } else {
        payload.release();
        return Flux.error(leaseHandler.leaseError());
      }
    } catch (Throwable t) {
      return Flux.error(t);
    }
  }

  @Override
  public Mono<Void> metadataPush(Payload payload) {
    try {
      return requestHandler.metadataPush(payload);
    } catch (Throwable t) {
      return Mono.error(t);
    }
  }

  @Override
  public void dispose() {
    connection.dispose();
  }

  @Override
  public boolean isDisposed() {
    return connection.isDisposed();
  }

  @Override
  public Mono<Void> onClose() {
    return connection.onClose();
  }

  private void cleanup() {
    cleanUpSendingSubscriptions();

    requestHandler.dispose();
    sendProcessor.dispose();
  }

  private synchronized void cleanUpSendingSubscriptions() {
    activeStreams.values().forEach(Subscription::cancel);
    activeStreams.clear();
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private void handleFrame(ByteBuf frame) {
    try {
      int streamId = FrameHeaderFlyweight.streamId(frame);
      Reassemble receiver;
      FrameType frameType = FrameHeaderFlyweight.frameType(frame);
      switch (frameType) {
        case REQUEST_FNF:
          handleFireAndForget(streamId, frame);
          break;
        case REQUEST_RESPONSE:
          handleRequestResponse(streamId, frame);
          break;
        case CANCEL:
          handleCancelFrame(streamId);
          break;
        case REQUEST_N:
          handleRequestN(streamId, frame);
          break;
        case REQUEST_STREAM:
          int streamInitialRequestN = RequestStreamFrameFlyweight.initialRequestN(frame);
          handleStream(streamId, frame, streamInitialRequestN);
          break;
        case REQUEST_CHANNEL:
          int channelInitialRequestN = RequestChannelFrameFlyweight.initialRequestN(frame);
          handleChannel(streamId, frame, channelInitialRequestN);
          break;
        case METADATA_PUSH:
          handleMetadataPush(metadataPush(payloadDecoder.apply(frame)));
          break;
        case PAYLOAD:
          // TODO: Hook in receiving socket.
          break;
        case NEXT:
          receiver = activeStreams.get(streamId);
          if (receiver != null) {
            boolean hasFollows = FrameHeaderFlyweight.hasFollows(frame);
            if (receiver.isReassemblingNow() || hasFollows) {
              receiver.reassemble(frame, hasFollows, false);
            } else {
              receiver.onNext(payloadDecoder.apply(frame));
            }
          }
          break;
        case COMPLETE:
          receiver = activeStreams.get(streamId);
          if (receiver != null) {
            if (receiver.isReassemblingNow()) {
              receiver.reassemble(Unpooled.EMPTY_BUFFER, false, true);
            } else {
              receiver.onComplete();
            }
          }
          break;
        case ERROR:
          receiver = activeStreams.get(streamId);
          if (receiver != null) {
            receiver.onError(io.rsocket.exceptions.Exceptions.from(streamId, frame));
          }
          break;
        case NEXT_COMPLETE:
          receiver = activeStreams.get(streamId);
          if (receiver != null) {
            if (receiver.isReassemblingNow()) {
              receiver.reassemble(frame, false, true);
            } else {
              receiver.onNext(payloadDecoder.apply(frame));
              receiver.onComplete();
            }
          }
          break;
        case SETUP:
          handleError(streamId, new IllegalStateException("Setup frame received post setup."));
          break;
        case LEASE:
        default:
          handleError(
              streamId,
              new IllegalStateException("ServerRSocket: Unexpected frame type: " + frameType));
          break;
      }
      ReferenceCountUtil.safeRelease(frame);
    } catch (Throwable t) {
      ReferenceCountUtil.safeRelease(frame);
      throw Exceptions.propagate(t);
    }
  }

  private void handleFireAndForget(int streamId, ByteBuf frame) {
    final IntObjectMap<Reassemble<?>> activeStreams = this.activeStreams;
    if (!activeStreams.containsKey(streamId)) {
      if (FrameHeaderFlyweight.hasFollows(frame)) {
        FireAndForgetSubscriber subscriber =
            new FireAndForgetSubscriber(
                streamId, frame, allocator, payloadDecoder, errorConsumer, activeStreams, this);
        if (activeStreams.putIfAbsent(streamId, subscriber) != null) {
          subscriber.cancel();
        }
      } else {
        fireAndForget(this.payloadDecoder.apply(frame)).subscribe(this.noOpFireAndForgetSubscriber);
      }
    }
  }

  private void handleRequestResponse(int streamId, ByteBuf frame) {
    final IntObjectMap<Reassemble<?>> activeStreams = this.activeStreams;
    if (!activeStreams.containsKey(streamId)) {
      if (FrameHeaderFlyweight.hasFollows(frame)) {
        RequestResponseSubscriber subscriber =
            new RequestResponseSubscriber(
                streamId,
                this.allocator,
                this.payloadDecoder,
                frame,
                this.mtu,
                this.errorConsumer,
                this.activeStreams,
                this.sendProcessor,
                this);
        if (activeStreams.putIfAbsent(streamId, subscriber) != null) {
          subscriber.cancel();
        }
      } else {
        RequestResponseSubscriber subscriber =
            new RequestResponseSubscriber(
                streamId,
                this.allocator,
                this.mtu,
                this.errorConsumer,
                this.activeStreams,
                this.sendProcessor);
        if (activeStreams.putIfAbsent(streamId, subscriber) == null) {
          this.requestResponse(payloadDecoder.apply(frame)).subscribe(subscriber);
        }
      }
    }
  }

  private void handleStream(int streamId, ByteBuf frame, int initialRequestN) {
    final IntObjectMap<Reassemble<?>> activeStreams = this.activeStreams;
    if (!activeStreams.containsKey(streamId)) {
      if (FrameHeaderFlyweight.hasFollows(frame)) {
        RequestStreamSubscriber subscriber =
            new RequestStreamSubscriber(
                streamId,
                initialRequestN == Integer.MAX_VALUE ? Long.MAX_VALUE : initialRequestN,
                this.allocator,
                this.payloadDecoder,
                frame,
                this.mtu,
                this.errorConsumer,
                this.activeStreams,
                this.sendProcessor,
                this);
        if (activeStreams.putIfAbsent(streamId, subscriber) != null) {
          subscriber.cancel();
        }
      } else {
        RequestStreamSubscriber subscriber =
            new RequestStreamSubscriber(
                streamId,
                initialRequestN == Integer.MAX_VALUE ? Long.MAX_VALUE : initialRequestN,
                this.allocator,
                this.mtu,
                this.errorConsumer,
                this.activeStreams,
                this.sendProcessor);
        if (activeStreams.putIfAbsent(streamId, subscriber) == null) {
          this.requestStream(payloadDecoder.apply(frame)).subscribe(subscriber);
        }
      }
    }
  }

  private void handleChannel(int streamId, ByteBuf frame, int initialRequestN) {
    final IntObjectMap<Reassemble<?>> activeStreams = this.activeStreams;
    if (!activeStreams.containsKey(streamId)) {
      if (FrameHeaderFlyweight.hasFollows(frame)) {
        RequestChannelSubscriber subscriber =
            new RequestChannelSubscriber(
                streamId,
                initialRequestN == Integer.MAX_VALUE ? Long.MAX_VALUE : initialRequestN,
                this.allocator,
                this.payloadDecoder,
                frame,
                this.mtu,
                this.errorConsumer,
                this.activeStreams,
                this.sendProcessor,
                this);
        if (activeStreams.putIfAbsent(streamId, subscriber) != null) {
          subscriber.cancel();
        }
      } else {
        final Payload firstPayload = this.payloadDecoder.apply(frame);
        RequestChannelSubscriber subscriber =
            new RequestChannelSubscriber(
                streamId,
                initialRequestN == Integer.MAX_VALUE ? Long.MAX_VALUE : initialRequestN,
                this.allocator,
                this.payloadDecoder,
                firstPayload,
                this.mtu,
                this.errorConsumer,
                this.activeStreams,
                this.sendProcessor);
        if (activeStreams.putIfAbsent(streamId, subscriber) == null) {
          this.requestChannel(firstPayload, subscriber).subscribe(subscriber.senderSubscriber);
        } else {
          firstPayload.release();
        }
      }
    }
  }

  private void handleMetadataPush(Mono<Void> result) {
    result.subscribe(this.metadataPushSubscriber);
  }

  private void handleCancelFrame(int streamId) {
    Subscription subscription = activeStreams.remove(streamId);

    if (subscription != null) {
      subscription.cancel();
    }
  }

  private void handleError(int streamId, Throwable t) {
    errorConsumer.accept(t);
    sendProcessor.onNext(ErrorFrameFlyweight.encode(allocator, streamId, t));
  }

  private void handleRequestN(int streamId, ByteBuf frame) {
    Subscription subscription = activeStreams.get(streamId);

    if (subscription != null) {
      int n = RequestNFrameFlyweight.requestN(frame);
      subscription.request(n >= Integer.MAX_VALUE ? Long.MAX_VALUE : n);
    }
  }
}
