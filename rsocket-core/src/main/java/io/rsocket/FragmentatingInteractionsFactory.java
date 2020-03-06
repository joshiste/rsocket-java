package io.rsocket;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.util.collection.IntObjectMap;
import io.rsocket.internal.UnboundedProcessor;
import org.reactivestreams.Processor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class FragmentatingInteractionsFactory implements InteractionsFactory {
    final int mtu;
    final StateAware stateAware;
    final ByteBufAllocator allocator;
    final StreamIdSupplier streamIdSupplier;
    final IntObjectMap<Processor<Payload, Payload>> registry;
    final UnboundedProcessor<ByteBuf> sendProcessor;

    public FragmentatingInteractionsFactory(int mtu, StateAware stateAware, ByteBufAllocator allocator, StreamIdSupplier streamIdSupplier, IntObjectMap<Processor<Payload, Payload>> registry, UnboundedProcessor<ByteBuf> sendProcessor) {
        this.mtu = mtu;
        this.stateAware = stateAware;
        this.allocator = allocator;
        this.streamIdSupplier = streamIdSupplier;
        this.registry = registry;
        this.sendProcessor = sendProcessor;
    }
    @Override
    public Mono<Void> fireAndForget(Payload payload) {
        return new FragmentatingFireAndForgetMono(allocator, payload, mtu, stateAware, streamIdSupplier, registry, sendProcessor);
    }

    @Override
    public Mono<Payload> requestRespinse(Payload payload) {
        return null;
    }

    @Override
    public Flux<Payload> requestStream(Payload payload) {
        return null;
    }

    @Override
    public Flux<Payload> requestChannel(Flux<Payload> payloadFlux) {
        return null;
    }
}
