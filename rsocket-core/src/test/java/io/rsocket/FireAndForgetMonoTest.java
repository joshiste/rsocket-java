package io.rsocket;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.util.CharsetUtil;
import io.netty.util.IllegalReferenceCountException;
import io.netty.util.collection.IntObjectMap;
import io.rsocket.frame.FrameLengthFlyweight;
import io.rsocket.frame.FrameType;
import io.rsocket.internal.SynchronizedIntObjectHashMap;
import io.rsocket.internal.UnboundedProcessor;
import io.rsocket.util.ByteBufPayload;
import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import reactor.core.Exceptions;
import reactor.core.Scannable;
import reactor.test.StepVerifier;
import reactor.test.util.RaceTestUtils;

public class FireAndForgetMonoTest {

  @BeforeAll
  public static void setUp() {
    StepVerifier.setDefaultTimeout(Duration.ofSeconds(2));
  }

  @ParameterizedTest
  @MethodSource("frameSent")
  public void frameShouldBeSentOnSubscription(Consumer<FireAndForgetMono> monoConsumer) {
    final UnboundedProcessor<ByteBuf> sender = new UnboundedProcessor<>();
    final Payload payload = ByteBufPayload.create("testData", "testMetadata");
    final IntObjectMap<Reassemble<?>> activeStreams = new SynchronizedIntObjectHashMap<>();

    final FireAndForgetMono fireAndForgetMono =
        new FireAndForgetMono(
            ByteBufAllocator.DEFAULT,
            payload,
            0,
            TestStateAware.empty(),
            StreamIdSupplier.clientSupplier(),
            activeStreams,
            sender);

    Assertions.assertThat(activeStreams).isEmpty();

    monoConsumer.accept(fireAndForgetMono);

    Assertions.assertThat(payload.refCnt()).isZero();
    // should not add anything to map
    Assertions.assertThat(activeStreams).isEmpty();

    final ByteBuf frame = sender.poll();
    FrameAssert.assertThat(frame)
        .isNotNull()
        .hasPayloadSize(
            "testData".getBytes(CharsetUtil.UTF_8).length
                + "testMetadata".getBytes(CharsetUtil.UTF_8).length)
        .hasMetadata("testMetadata")
        .hasData("testData")
        .hasNoFragmentsFollow()
        .typeOf(FrameType.REQUEST_FNF)
        .hasClientSideStreamId()
        .hasStreamId(1);

    Assertions.assertThat(frame.release()).isTrue();
    Assertions.assertThat(frame.refCnt()).isZero();
    Assertions.assertThat(sender.isEmpty()).isTrue();
  }

  @ParameterizedTest
  @MethodSource("frameSent")
  public void frameFragmentsShouldBeSentOnSubscription(Consumer<FireAndForgetMono> monoConsumer) {
    final UnboundedProcessor<ByteBuf> sender = new UnboundedProcessor<>();

    final byte[] metadata = new byte[65];
    final byte[] data = new byte[129];
    ThreadLocalRandom.current().nextBytes(metadata);
    ThreadLocalRandom.current().nextBytes(data);

    final Payload payload = ByteBufPayload.create(data, metadata);
    final IntObjectMap<Reassemble<?>> activeStreams = new SynchronizedIntObjectHashMap<>();

    final FireAndForgetMono fireAndForgetMono =
        new FireAndForgetMono(
            ByteBufAllocator.DEFAULT,
            payload,
            64,
            TestStateAware.empty(),
            StreamIdSupplier.clientSupplier(),
            activeStreams,
            sender);

    Assertions.assertThat(activeStreams).isEmpty();

    monoConsumer.accept(fireAndForgetMono);

    // should not add anything to map
    Assertions.assertThat(activeStreams).isEmpty();

    Assertions.assertThat(payload.refCnt()).isZero();

    final ByteBuf frameFragment1 = sender.poll();
    FrameAssert.assertThat(frameFragment1)
        .isNotNull()
        .hasPayloadSize(55) // 64 - 3 (frame headers) - 3 (encoded metadata length) - 3 frame length
        .hasMetadata(Arrays.copyOf(metadata, 55))
        .hasData(Unpooled.EMPTY_BUFFER)
        .hasFragmentsFollow()
        .typeOf(FrameType.REQUEST_FNF)
        .hasClientSideStreamId()
        .hasStreamId(1);

    Assertions.assertThat(frameFragment1.release()).isTrue();
    Assertions.assertThat(frameFragment1.refCnt()).isZero();

    final ByteBuf frameFragment2 = sender.poll();
    FrameAssert.assertThat(frameFragment2)
        .isNotNull()
        .hasPayloadSize(55) // 64 - 3 (frame headers) - 3 (encoded metadata length) - 3 frame length
        .hasMetadata(Arrays.copyOfRange(metadata, 55, 65))
        .hasData(Arrays.copyOf(data, 45))
        .hasFragmentsFollow()
        .typeOf(FrameType.NEXT)
        .hasClientSideStreamId()
        .hasStreamId(1);

    Assertions.assertThat(frameFragment2.release()).isTrue();
    Assertions.assertThat(frameFragment2.refCnt()).isZero();

    final ByteBuf frameFragment3 = sender.poll();
    FrameAssert.assertThat(frameFragment3)
        .isNotNull()
        .hasPayloadSize(58) // 64 - 3 (frame headers) - 3 frame length (no metadata - no length)
        .hasNoMetadata()
        .hasData(Arrays.copyOfRange(data, 45, 103))
        .hasFragmentsFollow()
        .typeOf(FrameType.NEXT)
        .hasClientSideStreamId()
        .hasStreamId(1);

    Assertions.assertThat(frameFragment3.release()).isTrue();
    Assertions.assertThat(frameFragment3.refCnt()).isZero();

    final ByteBuf frameFragment4 = sender.poll();
    FrameAssert.assertThat(frameFragment4)
        .isNotNull()
        .hasPayloadSize(26)
        .hasNoMetadata()
        .hasData(Arrays.copyOfRange(data, 103, 129))
        .hasNoFragmentsFollow()
        .typeOf(FrameType.NEXT)
        .hasClientSideStreamId()
        .hasStreamId(1);

    Assertions.assertThat(frameFragment4.release()).isTrue();
    Assertions.assertThat(frameFragment4.refCnt()).isZero();
    Assertions.assertThat(sender.isEmpty()).isTrue();
  }

  static Stream<Consumer<FireAndForgetMono>> frameSent() {
    return Stream.of(
        (s) -> StepVerifier.create(s).expectSubscription().expectComplete().verify(),
        FireAndForgetMono::block);
  }

  @ParameterizedTest
  @MethodSource("shouldErrorOnIncorrectRefCntInGivenPayloadSource")
  public void shouldErrorOnIncorrectRefCntInGivenPayload(Consumer<FireAndForgetMono> monoConsumer) {
    final UnboundedProcessor<ByteBuf> sender = new UnboundedProcessor<>();
    final IntObjectMap<Reassemble<?>> activeStreams = new SynchronizedIntObjectHashMap<>();
    final Payload payload = ByteBufPayload.create("");
    payload.release();

    final FireAndForgetMono fireAndForgetMono =
        new FireAndForgetMono(
            ByteBufAllocator.DEFAULT,
            payload,
            0,
            TestStateAware.empty(),
            StreamIdSupplier.clientSupplier(),
            activeStreams,
            sender);

    Assertions.assertThat(activeStreams).isEmpty();

    monoConsumer.accept(fireAndForgetMono);

    Assertions.assertThat(activeStreams).isEmpty();
    Assertions.assertThat(sender.isEmpty()).isTrue();
  }

  static Stream<Consumer<FireAndForgetMono>> shouldErrorOnIncorrectRefCntInGivenPayloadSource() {
    return Stream.of(
        (s) ->
            StepVerifier.create(s)
                .expectSubscription()
                .expectError(IllegalReferenceCountException.class)
                .verify(),
        fireAndForgetMono ->
            Assertions.assertThatThrownBy(fireAndForgetMono::block)
                .isInstanceOf(IllegalReferenceCountException.class));
  }

  @ParameterizedTest
  @MethodSource("shouldErrorIfFragmentExitsAllowanceIfFragmentationDisabledSource")
  public void shouldErrorIfFragmentExitsAllowanceIfFragmentationDisabled(
      Consumer<FireAndForgetMono> monoConsumer) {
    final UnboundedProcessor<ByteBuf> sender = new UnboundedProcessor<>();
    final IntObjectMap<Reassemble<?>> activeStreams = new SynchronizedIntObjectHashMap<>();

    final byte[] metadata = new byte[FrameLengthFlyweight.FRAME_LENGTH_MASK];
    final byte[] data = new byte[FrameLengthFlyweight.FRAME_LENGTH_MASK];
    ThreadLocalRandom.current().nextBytes(metadata);
    ThreadLocalRandom.current().nextBytes(data);

    final Payload payload = ByteBufPayload.create(data, metadata);

    final FireAndForgetMono fireAndForgetMono =
        new FireAndForgetMono(
            ByteBufAllocator.DEFAULT,
            payload,
            0,
            TestStateAware.empty(),
            StreamIdSupplier.clientSupplier(),
            activeStreams,
            sender);

    Assertions.assertThat(activeStreams).isEmpty();

    monoConsumer.accept(fireAndForgetMono);

    Assertions.assertThat(payload.refCnt()).isZero();

    Assertions.assertThat(activeStreams).isEmpty();
    Assertions.assertThat(sender.isEmpty()).isTrue();
  }

  static Stream<Consumer<FireAndForgetMono>>
      shouldErrorIfFragmentExitsAllowanceIfFragmentationDisabledSource() {
    return Stream.of(
        (s) ->
            StepVerifier.create(s)
                .expectSubscription()
                .consumeErrorWith(
                    t ->
                        Assertions.assertThat(t)
                            .hasMessage("Too Big Payload size")
                            .isInstanceOf(IllegalArgumentException.class))
                .verify(),
        fireAndForgetMono ->
            Assertions.assertThatThrownBy(fireAndForgetMono::block)
                .hasMessage("Too Big Payload size")
                .isInstanceOf(IllegalArgumentException.class));
  }

  @ParameterizedTest
  @MethodSource("shouldErrorIfNoAvailabilitySource")
  public void shouldErrorIfNoAvailability(Consumer<FireAndForgetMono> monoConsumer) {
    final UnboundedProcessor<ByteBuf> sender = new UnboundedProcessor<>();
    final IntObjectMap<Reassemble<?>> activeStreams = new SynchronizedIntObjectHashMap<>();
    final Payload payload = ByteBufPayload.create("testData", "testMetadata");

    final FireAndForgetMono fireAndForgetMono =
        new FireAndForgetMono(
            ByteBufAllocator.DEFAULT,
            payload,
            0,
            TestStateAware.error(new RuntimeException("test")),
            StreamIdSupplier.clientSupplier(),
            activeStreams,
            sender);

    Assertions.assertThat(activeStreams).isEmpty();

    monoConsumer.accept(fireAndForgetMono);

    Assertions.assertThat(payload.refCnt()).isZero();

    Assertions.assertThat(activeStreams).isEmpty();
    Assertions.assertThat(sender.isEmpty()).isTrue();
  }

  static Stream<Consumer<FireAndForgetMono>> shouldErrorIfNoAvailabilitySource() {
    return Stream.of(
        (s) ->
            StepVerifier.create(s)
                .expectSubscription()
                .consumeErrorWith(
                    t ->
                        Assertions.assertThat(t)
                            .hasMessage("test")
                            .isInstanceOf(RuntimeException.class))
                .verify(),
        fireAndForgetMono ->
            Assertions.assertThatThrownBy(fireAndForgetMono::block)
                .hasMessage("test")
                .isInstanceOf(RuntimeException.class));
  }

  @Test
  public void shouldSubscribeExactlyOnce1() {
    final UnboundedProcessor<ByteBuf> sender = new UnboundedProcessor<>();
    final IntObjectMap<Reassemble<?>> activeStreams = new SynchronizedIntObjectHashMap<>();

    for (int i = 0; i < 1000; i++) {
      final Payload payload = ByteBufPayload.create("testData", "testMetadata");

      final FireAndForgetMono fireAndForgetMono =
          new FireAndForgetMono(
              ByteBufAllocator.DEFAULT,
              payload,
              0,
              TestStateAware.empty(),
              StreamIdSupplier.clientSupplier(),
              activeStreams,
              sender);

      Assertions.assertThatThrownBy(
              () ->
                  RaceTestUtils.race(
                      () ->
                          fireAndForgetMono.subscribe(
                              null,
                              t -> {
                                throw Exceptions.propagate(t);
                              }),
                      fireAndForgetMono::block))
          .matches(
              t -> {
                if (t instanceof IllegalReferenceCountException) {
                  Assertions.assertThat(t).hasMessage("refCnt: 0");
                } else {
                  Assertions.assertThat(t)
                      .hasMessage("FireAndForgetMono allows only a single Subscriber");
                }
                return true;
              });

      final ByteBuf frame = sender.poll();
      FrameAssert.assertThat(frame)
          .isNotNull()
          .hasPayloadSize(
              "testData".getBytes(CharsetUtil.UTF_8).length
                  + "testMetadata".getBytes(CharsetUtil.UTF_8).length)
          .hasMetadata("testMetadata")
          .hasData("testData")
          .hasNoFragmentsFollow()
          .typeOf(FrameType.REQUEST_FNF)
          .hasClientSideStreamId()
          .hasStreamId(1);

      Assertions.assertThat(frame.release()).isTrue();
      Assertions.assertThat(frame.refCnt()).isZero();
    }

    Assertions.assertThat(sender.isEmpty()).isTrue();
  }

  @Test
  public void checkName() {
    final UnboundedProcessor<ByteBuf> sender = new UnboundedProcessor<>();
    final IntObjectMap<Reassemble<?>> activeStreams = new SynchronizedIntObjectHashMap<>();
    final Payload payload = ByteBufPayload.create("testData", "testMetadata");

    final FireAndForgetMono fireAndForgetMono =
        new FireAndForgetMono(
            ByteBufAllocator.DEFAULT,
            payload,
            0,
            TestStateAware.empty(),
            StreamIdSupplier.clientSupplier(),
            activeStreams,
            sender);

    Assertions.assertThat(Scannable.from(fireAndForgetMono).name())
        .isEqualTo("source(FireAndForgetMono)");
  }

  static final class TestStateAware implements StateAware {

    final Throwable error;

    TestStateAware(Throwable error) {
      this.error = error;
    }

    @Override
    public Throwable error() {
      return error;
    }

    @Override
    public Throwable checkAvailable() {
      return error;
    }

    public static TestStateAware error(Throwable e) {
      return new TestStateAware(e);
    }

    public static TestStateAware empty() {
      return new TestStateAware(null);
    }
  }
}
