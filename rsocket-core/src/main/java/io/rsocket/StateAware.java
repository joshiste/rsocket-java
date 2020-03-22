package io.rsocket;

interface StateAware {

  Throwable checkAvailable();
}
