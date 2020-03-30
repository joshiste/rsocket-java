package io.rsocket;

import io.rsocket.test.util.TestClientTransport;
import io.rsocket.test.util.TestServerTransport;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

public class RSocketFactoryFragmentationTest {

  @Test
  public void serverErrorsWithEnabledFragmentationOnInsufficientMtu() {
    Assertions.assertThatThrownBy(
            () ->
                RSocketFactory.receive()
                    .fragment(2)
                    .acceptor((s, r) -> Mono.just(new AbstractRSocket() {}))
                    .transport(TestServerTransport::new)
                    .start()
                    .block())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("smallest allowed mtu size is 64 bytes, provided: 2");
  }

  @Test
  public void serverSucceedsWithEnabledFragmentationOnSufficientMtu() {
    RSocketFactory.receive()
        .fragment(100)
        .acceptor((s, r) -> Mono.just(new AbstractRSocket() {}))
        .transport(TestServerTransport::new)
        .start()
        .block();
  }

  @Test
  public void serverSucceedsWithDisabledFragmentation() {
    RSocketFactory.receive()
        .acceptor((s, r) -> Mono.just(new AbstractRSocket() {}))
        .transport(TestServerTransport::new)
        .start()
        .block();
  }

  @Test
  public void clientErrorsWithEnabledFragmentationOnInsufficientMtu() {
    Assertions.assertThatThrownBy(
            () ->
                RSocketFactory.connect()
                    .fragment(2)
                    .transport(TestClientTransport::new)
                    .start()
                    .block())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("smallest allowed mtu size is 64 bytes, provided: 2");
  }

  @Test
  public void clientSucceedsWithEnabledFragmentationOnSufficientMtu() {
    RSocketFactory.connect().fragment(100).transport(TestClientTransport::new).start().block();
  }

  @Test
  public void clientSucceedsWithDisabledFragmentation() {
    RSocketFactory.connect().transport(TestClientTransport::new).start().block();
  }
}
