package io.rsocket;

import io.netty.buffer.ByteBuf;
import org.reactivestreams.Subscription;

public interface Reassemble extends Subscription {

  boolean isReassemblingNow();

  void reassemble(ByteBuf dataAndMetadata, boolean hasFollows);
}
