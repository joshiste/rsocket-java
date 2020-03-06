package io.rsocket;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface InteractionsFactory {

    Mono<Void> fireAndForget(Payload payload);

    Mono<Payload> requestRespinse(Payload payload);

    Flux<Payload> requestStream(Payload payload);

    Flux<Payload> requestChannel(Flux<Payload> payloadFlux);
}
