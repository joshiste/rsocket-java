package io.rsocket;

import io.netty.buffer.ByteBuf;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;

public interface Reassemble<T> extends Subscription, CoreSubscriber<T> {

  boolean isReassemblingNow();

  void reassemble(ByteBuf dataAndMetadata, boolean hasFollows, boolean terminal);
}
