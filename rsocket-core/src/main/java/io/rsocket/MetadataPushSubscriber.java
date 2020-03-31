package io.rsocket;

import java.util.function.Consumer;
import org.reactivestreams.Subscription;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Operators;
import reactor.util.context.Context;

final class MetadataPushSubscriber implements CoreSubscriber<Void> {

  private final Consumer<? super Throwable> errorConsumer;

  MetadataPushSubscriber(Consumer<? super Throwable> errorConsumer) {
    this.errorConsumer = errorConsumer;
  }

  @Override
  public void onSubscribe(Subscription s) {
    s.request(Long.MAX_VALUE);
  }

  @Override
  public void onNext(Void voidVal) {}

  @Override
  public void onError(Throwable t) {
    errorConsumer.accept(t);
    Operators.onErrorDropped(t, Context.empty());
  }

  @Override
  public void onComplete() {}
}
