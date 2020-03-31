package io.rsocket;

interface StateAware {

  Throwable error();

  Throwable checkAvailable();
}
