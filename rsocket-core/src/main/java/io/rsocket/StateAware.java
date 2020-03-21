package io.rsocket;

public interface StateAware {

  Throwable checkAvailable();
}
