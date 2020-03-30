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

package io.rsocket.examples.transport.tcp.channel;

import io.netty.util.ReferenceCounted;
import io.rsocket.AbstractRSocket;
import io.rsocket.ConnectionSetupPayload;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.RSocketFactory;
import io.rsocket.SocketAcceptor;
import io.rsocket.frame.decoder.PayloadDecoder;
import io.rsocket.transport.local.LocalClientTransport;
import io.rsocket.transport.local.LocalServerTransport;
import io.rsocket.util.ByteBufPayload;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

public final class ChannelEchoClient {
  static final Payload payload1 = ByteBufPayload.create("Hello ");

  public static void main(String[] args) throws InterruptedException {
    RSocketFactory.receive()
        .frameDecoder(PayloadDecoder.ZERO_COPY)
        .acceptor(new SocketAcceptorImpl())
        .transport(LocalServerTransport.create("localhost"))
        .start()
        .subscribe();

    RSocket socket =
        RSocketFactory.connect()
            .keepAliveAckTimeout(Duration.ofMinutes(10))
            .frameDecoder(PayloadDecoder.ZERO_COPY)
            .transport(LocalClientTransport.create("localhost"))
            .start()
            .block();

    AtomicLong receivedRequests = new AtomicLong();
    AtomicLong sentRequests = new AtomicLong();

    Flux.range(0, 100000)
        .map(t -> payload1.retain())
        .doOnRequest(receivedRequests::addAndGet)
        .transform(socket::requestChannel)
        .doOnRequest(sentRequests::addAndGet)
        .map(ReferenceCounted::release)
        .limitRate(1)
        .blockLast();

    System.gc();
    Thread.sleep(1000);
    System.gc();

    if ((sentRequests.get() - 1) != receivedRequests.get()) {
        throw new IllegalStateException("Requested [" + sentRequests.get() + "], but received [" + receivedRequests.get() + "]");
    }
  }

  private static class SocketAcceptorImpl implements SocketAcceptor {
    @Override
    public Mono<RSocket> accept(ConnectionSetupPayload setupPayload, RSocket reactiveSocket) {
      return Mono.just(
          new AbstractRSocket() {

            @Override
            public Mono<Void> fireAndForget(Payload payload) {
              //                  System.out.println(payload.getDataUtf8());
              payload.release();
              return Mono.empty();
            }

            @Override
            public Mono<Payload> requestResponse(Payload payload) {
              return Mono.just(payload);
            }

            @Override
            public Flux<Payload> requestChannel(Publisher<Payload> payloads) {
              return Flux.from(payloads)
                      .subscribeOn(Schedulers.single());
            }
          });
    }
  }
}
