package io.rsocket;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.util.CharsetUtil;
import io.netty.util.IllegalReferenceCountException;
import io.netty.util.collection.IntObjectMap;
import io.rsocket.exceptions.ApplicationErrorException;
import io.rsocket.fragmentation.FragmentationUtils;
import io.rsocket.frame.FrameLengthFlyweight;
import io.rsocket.frame.FrameType;
import io.rsocket.frame.decoder.PayloadDecoder;
import io.rsocket.internal.SynchronizedIntObjectHashMap;
import io.rsocket.internal.UnboundedProcessor;
import io.rsocket.internal.subscriber.AssertSubscriber;
import io.rsocket.util.ByteBufPayload;
import io.rsocket.util.EmptyPayload;
import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.BiFunction;
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

public class RequestStreamFluxTest {

  @BeforeAll
  public static void setUp() {
    StepVerifier.setDefaultTimeout(Duration.ofSeconds(2));
  }

  @Test
  public void requestNframeShouldBeSentOnSubscription() {
    final UnboundedProcessor<ByteBuf> sender = new UnboundedProcessor<>();
    final Payload payload = ByteBufPayload.create("testData", "testMetadata");
    final IntObjectMap<Reassemble<?>> activeStreams = new SynchronizedIntObjectHashMap<>();

    final RequestStreamFlux requestStreamFlux =
        new RequestStreamFlux(
            ByteBufAllocator.DEFAULT,
            payload,
            0,
            TestStateAware.empty(),
            StreamIdSupplier.clientSupplier(),
            activeStreams,
            sender,
            PayloadDecoder.ZERO_COPY);

    Assertions.assertThat(activeStreams).isEmpty();

    final AssertSubscriber<Payload> assertSubscriber =
        requestStreamFlux.subscribeWith(AssertSubscriber.create(0));
    Assertions.assertThat(payload.refCnt()).isOne();
    Assertions.assertThat(activeStreams).isEmpty();

    assertSubscriber.request(1);

    Assertions.assertThat(payload.refCnt()).isZero();
    Assertions.assertThat(activeStreams).containsEntry(1, requestStreamFlux);

    final ByteBuf frame = sender.poll();
    FrameAssert.assertThat(frame)
        .isNotNull()
        .hasPayloadSize(
            "testData".getBytes(CharsetUtil.UTF_8).length
                + "testMetadata".getBytes(CharsetUtil.UTF_8).length)
        .hasMetadata("testMetadata")
        .hasData("testData")
        .hasNoFragmentsFollow()
        .hasRequestN(1)
        .typeOf(FrameType.REQUEST_STREAM)
        .hasClientSideStreamId()
        .hasStreamId(1);

    Assertions.assertThat(sender.isEmpty()).isTrue();

    assertSubscriber.request(1);
    final ByteBuf requestNFrame = sender.poll();
    FrameAssert.assertThat(requestNFrame)
        .isNotNull()
        .hasRequestN(1)
        .typeOf(FrameType.REQUEST_N)
        .hasClientSideStreamId()
        .hasStreamId(1);

    Assertions.assertThat(sender.isEmpty()).isTrue();

    requestStreamFlux.onNext(EmptyPayload.INSTANCE);

    requestStreamFlux.onComplete();

    assertSubscriber.assertValues(EmptyPayload.INSTANCE).assertComplete();

    Assertions.assertThat(frame.release()).isTrue();
    Assertions.assertThat(frame.refCnt()).isZero();

    Assertions.assertThat(payload.refCnt()).isZero();
    Assertions.assertThat(activeStreams).isEmpty();

    Assertions.assertThat(sender.isEmpty()).isTrue();
  }

  @ParameterizedTest
  @MethodSource("frameShouldBeSentOnSubscriptionResponses")
  public void frameShouldBeSentOnSubscription(
      BiFunction<RequestStreamFlux, StepVerifier.Step<Payload>, StepVerifier> transformer) {
    final UnboundedProcessor<ByteBuf> sender = new UnboundedProcessor<>();
    final Payload payload = ByteBufPayload.create("testData", "testMetadata");
    final IntObjectMap<Reassemble<?>> activeStreams = new SynchronizedIntObjectHashMap<>();

    final RequestStreamFlux requestStreamFlux =
        new RequestStreamFlux(
            ByteBufAllocator.DEFAULT,
            payload,
            0,
            TestStateAware.empty(),
            StreamIdSupplier.clientSupplier(),
            activeStreams,
            sender,
            PayloadDecoder.ZERO_COPY);

    Assertions.assertThat(activeStreams).isEmpty();

    transformer
        .apply(
            requestStreamFlux,
            StepVerifier.create(requestStreamFlux, 0)
                .expectSubscription()
                .then(() -> Assertions.assertThat(payload.refCnt()).isOne())
                .then(() -> Assertions.assertThat(activeStreams).isEmpty())
                .thenRequest(1)
                .then(() -> Assertions.assertThat(payload.refCnt()).isZero())
                .then(
                    () -> Assertions.assertThat(activeStreams).containsEntry(1, requestStreamFlux)))
        .verify();

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
        .typeOf(FrameType.REQUEST_RESPONSE)
        .hasClientSideStreamId()
        .hasStreamId(1);

    Assertions.assertThat(frame.release()).isTrue();
    Assertions.assertThat(frame.refCnt()).isZero();
    if (!sender.isEmpty()) {
      FrameAssert.assertThat(sender.poll())
          .isNotNull()
          .typeOf(FrameType.CANCEL)
          .hasClientSideStreamId()
          .hasStreamId(1);
    }
    Assertions.assertThat(sender.isEmpty()).isTrue();
  }

  static Stream<BiFunction<RequestStreamFlux, StepVerifier.Step<Payload>, StepVerifier>>
      frameShouldBeSentOnSubscriptionResponses() {
    return Stream.of(
        (rsf, sv) ->
            sv.then(() -> rsf.onNext(EmptyPayload.INSTANCE))
                .expectNext(EmptyPayload.INSTANCE)
                .then(() -> rsf.onNext(EmptyPayload.INSTANCE))
                .expectNext(EmptyPayload.INSTANCE)
                .then(rsf::onComplete)
                .expectComplete(),
        (rsf, sv) -> sv.then(rsf::onComplete).expectComplete(),
        (rsf, sv) ->
            sv.then(() -> rsf.onNext(EmptyPayload.INSTANCE))
                .expectNext(EmptyPayload.INSTANCE)
                .then(() -> rsf.onError(new ApplicationErrorException("test")))
                .expectErrorSatisfies(
                    t ->
                        Assertions.assertThat(t)
                            .hasMessage("test")
                            .isInstanceOf(ApplicationErrorException.class)),
        (rsf, sv) -> {
          final byte[] metadata = new byte[65];
          final byte[] data = new byte[129];
          ThreadLocalRandom.current().nextBytes(metadata);
          ThreadLocalRandom.current().nextBytes(data);

          final Payload payload = ByteBufPayload.create(data, metadata);
          final Payload payload2 = ByteBufPayload.create(data, metadata);

          return sv.then(
                  () -> {
                    final ByteBuf followingFrame =
                        FragmentationUtils.encodeFirstFragment(
                            ByteBufAllocator.DEFAULT,
                            64,
                            FrameType.REQUEST_RESPONSE,
                            1,
                            payload.metadata(),
                            payload.data());
                    rsf.reassemble(followingFrame, true, false);
                    followingFrame.release();
                  })
              .then(
                  () -> {
                    final ByteBuf followingFrame =
                        FragmentationUtils.encodeFollowsFragment(
                            ByteBufAllocator.DEFAULT,
                            64,
                            1,
                            false,
                            payload.metadata(),
                            payload.data());
                    rsf.reassemble(followingFrame, true, false);
                    followingFrame.release();
                  })
              .then(
                  () -> {
                    final ByteBuf followingFrame =
                        FragmentationUtils.encodeFollowsFragment(
                            ByteBufAllocator.DEFAULT,
                            64,
                            1,
                            false,
                            payload.metadata(),
                            payload.data());
                    rsf.reassemble(followingFrame, true, false);
                    followingFrame.release();
                  })
              .then(
                  () -> {
                    final ByteBuf followingFrame =
                        FragmentationUtils.encodeFollowsFragment(
                            ByteBufAllocator.DEFAULT,
                            64,
                            1,
                            false,
                            payload.metadata(),
                            payload.data());
                    rsf.reassemble(followingFrame, false, false);
                    followingFrame.release();
                  })
              .assertNext(
                  p -> {
                    Assertions.assertThat(p.data()).isEqualTo(Unpooled.wrappedBuffer(data));

                    Assertions.assertThat(p.metadata()).isEqualTo(Unpooled.wrappedBuffer(metadata));
                    Assertions.assertThat(p.release()).isTrue();
                    Assertions.assertThat(p.refCnt()).isZero();
                  })
              .then(payload::release)
              .then(() -> rsf.onNext(payload2))
              .assertNext(
                  p -> {
                    Assertions.assertThat(p.data()).isEqualTo(Unpooled.wrappedBuffer(data));

                    Assertions.assertThat(p.metadata()).isEqualTo(Unpooled.wrappedBuffer(metadata));
                    Assertions.assertThat(p.release()).isTrue();
                    Assertions.assertThat(p.refCnt()).isZero();
                  })
              .then(rsf::onComplete)
              .expectComplete();
        },
        (rsf, sv) -> {
          final byte[] metadata = new byte[65];
          final byte[] data = new byte[129];
          ThreadLocalRandom.current().nextBytes(metadata);
          ThreadLocalRandom.current().nextBytes(data);

          final Payload payload0 = ByteBufPayload.create(data, metadata);
          final Payload payload = ByteBufPayload.create(data, metadata);

          ByteBuf[] fragments =
              new ByteBuf[] {
                FragmentationUtils.encodeFirstFragment(
                    ByteBufAllocator.DEFAULT,
                    64,
                    FrameType.REQUEST_RESPONSE,
                    1,
                    payload.metadata(),
                    payload.data()),
                FragmentationUtils.encodeFollowsFragment(
                    ByteBufAllocator.DEFAULT, 64, 1, false, payload.metadata(), payload.data()),
                FragmentationUtils.encodeFollowsFragment(
                    ByteBufAllocator.DEFAULT, 64, 1, false, payload.metadata(), payload.data())
              };

          final StepVerifier stepVerifier =
              sv.then(() -> rsf.onNext(payload0))
                  .assertNext(
                      p -> {
                        Assertions.assertThat(p.data()).isEqualTo(Unpooled.wrappedBuffer(data));

                        Assertions.assertThat(p.metadata())
                            .isEqualTo(Unpooled.wrappedBuffer(metadata));
                        Assertions.assertThat(p.release()).isTrue();
                        Assertions.assertThat(p.refCnt()).isZero();
                      })
                  .then(
                      () -> {
                        rsf.reassemble(fragments[0], true, false);
                        fragments[0].release();
                      })
                  .then(
                      () -> {
                        rsf.reassemble(fragments[1], true, false);
                        fragments[1].release();
                      })
                  .then(
                      () -> {
                        rsf.reassemble(fragments[2], true, false);
                        fragments[2].release();
                      })
                  .then(payload::release)
                  .thenCancel()
                  .verifyLater();

          stepVerifier.verify();

          Assertions.assertThat(fragments).allMatch(bb -> bb.refCnt() == 0);

          return stepVerifier;
        });
  }

  @ParameterizedTest
  @MethodSource("frameShouldBeSentOnSubscriptionResponses")
  public void frameFragmentsShouldBeSentOnSubscription(
      BiFunction<RequestStreamFlux, StepVerifier.Step<Payload>, StepVerifier> transformer) {
    final UnboundedProcessor<ByteBuf> sender = new UnboundedProcessor<>();

    final byte[] metadata = new byte[65];
    final byte[] data = new byte[129];
    ThreadLocalRandom.current().nextBytes(metadata);
    ThreadLocalRandom.current().nextBytes(data);

    final Payload payload = ByteBufPayload.create(data, metadata);
    final IntObjectMap<Reassemble<?>> activeStreams = new SynchronizedIntObjectHashMap<>();

    final RequestStreamFlux requestStreamFlux =
        new RequestStreamFlux(
            ByteBufAllocator.DEFAULT,
            payload,
            64,
            TestStateAware.empty(),
            StreamIdSupplier.clientSupplier(),
            activeStreams,
            sender,
            PayloadDecoder.ZERO_COPY);

    Assertions.assertThat(activeStreams).isEmpty();

    transformer
        .apply(
            requestStreamFlux,
            StepVerifier.create(requestStreamFlux, 0)
                .expectSubscription()
                .then(() -> Assertions.assertThat(payload.refCnt()).isOne())
                .then(() -> Assertions.assertThat(activeStreams).isEmpty())
                .thenRequest(1)
                .then(() -> Assertions.assertThat(payload.refCnt()).isZero())
                .then(
                    () -> Assertions.assertThat(activeStreams).containsEntry(1, requestStreamFlux)))
        .verify();

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
        .typeOf(FrameType.REQUEST_RESPONSE)
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
    if (!sender.isEmpty()) {
      FrameAssert.assertThat(sender.poll())
          .isNotNull()
          .typeOf(FrameType.CANCEL)
          .hasClientSideStreamId()
          .hasStreamId(1);
    }
    Assertions.assertThat(sender).isEmpty();
  }

  @ParameterizedTest
  @MethodSource("shouldErrorOnIncorrectRefCntInGivenPayloadSource")
  public void shouldErrorOnIncorrectRefCntInGivenPayload(Consumer<RequestStreamFlux> monoConsumer) {
    final UnboundedProcessor<ByteBuf> sender = new UnboundedProcessor<>();
    final IntObjectMap<Reassemble<?>> activeStreams = new SynchronizedIntObjectHashMap<>();
    final Payload payload = ByteBufPayload.create("");
    payload.release();

    final RequestStreamFlux requestStreamFlux =
        new RequestStreamFlux(
            ByteBufAllocator.DEFAULT,
            payload,
            0,
            TestStateAware.empty(),
            StreamIdSupplier.clientSupplier(),
            activeStreams,
            sender,
            PayloadDecoder.ZERO_COPY);

    Assertions.assertThat(activeStreams).isEmpty();

    monoConsumer.accept(requestStreamFlux);

    Assertions.assertThat(activeStreams).isEmpty();
    Assertions.assertThat(sender).isEmpty();
  }

  static Stream<Consumer<RequestStreamFlux>> shouldErrorOnIncorrectRefCntInGivenPayloadSource() {
    return Stream.of(
        (s) ->
            StepVerifier.create(s)
                .expectSubscription()
                .expectError(IllegalReferenceCountException.class)
                .verify(),
        requestStreamFlux ->
            Assertions.assertThatThrownBy(requestStreamFlux::blockLast)
                .isInstanceOf(IllegalReferenceCountException.class));
  }

  @ParameterizedTest
  @MethodSource("shouldErrorIfFragmentExitsAllowanceIfFragmentationDisabledSource")
  public void shouldErrorIfFragmentExitsAllowanceIfFragmentationDisabled(
      Consumer<RequestStreamFlux> monoConsumer) {
    final UnboundedProcessor<ByteBuf> sender = new UnboundedProcessor<>();
    final IntObjectMap<Reassemble<?>> activeStreams = new SynchronizedIntObjectHashMap<>();

    final byte[] metadata = new byte[FrameLengthFlyweight.FRAME_LENGTH_MASK];
    final byte[] data = new byte[FrameLengthFlyweight.FRAME_LENGTH_MASK];
    ThreadLocalRandom.current().nextBytes(metadata);
    ThreadLocalRandom.current().nextBytes(data);

    final Payload payload = ByteBufPayload.create(data, metadata);

    final RequestStreamFlux requestStreamFlux =
        new RequestStreamFlux(
            ByteBufAllocator.DEFAULT,
            payload,
            0,
            TestStateAware.empty(),
            StreamIdSupplier.clientSupplier(),
            activeStreams,
            sender,
            PayloadDecoder.ZERO_COPY);

    Assertions.assertThat(activeStreams).isEmpty();

    monoConsumer.accept(requestStreamFlux);

    Assertions.assertThat(payload.refCnt()).isZero();

    Assertions.assertThat(activeStreams).isEmpty();
    Assertions.assertThat(sender).isEmpty();
  }

  static Stream<Consumer<RequestStreamFlux>>
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
        requestStreamFlux ->
            Assertions.assertThatThrownBy(requestStreamFlux::blockLast)
                .hasMessage("Too Big Payload size")
                .isInstanceOf(IllegalArgumentException.class));
  }

  @ParameterizedTest
  @MethodSource("shouldErrorIfNoAvailabilitySource")
  public void shouldErrorIfNoAvailability(Consumer<RequestStreamFlux> monoConsumer) {
    final UnboundedProcessor<ByteBuf> sender = new UnboundedProcessor<>();
    final IntObjectMap<Reassemble<?>> activeStreams = new SynchronizedIntObjectHashMap<>();
    final Payload payload = ByteBufPayload.create("testData", "testMetadata");

    final RequestStreamFlux requestStreamFlux =
        new RequestStreamFlux(
            ByteBufAllocator.DEFAULT,
            payload,
            0,
            TestStateAware.error(new RuntimeException("test")),
            StreamIdSupplier.clientSupplier(),
            activeStreams,
            sender,
            PayloadDecoder.ZERO_COPY);

    Assertions.assertThat(activeStreams).isEmpty();

    monoConsumer.accept(requestStreamFlux);

    Assertions.assertThat(payload.refCnt()).isZero();

    Assertions.assertThat(activeStreams).isEmpty();
  }

  static Stream<Consumer<RequestStreamFlux>> shouldErrorIfNoAvailabilitySource() {
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
        requestStreamFlux ->
            Assertions.assertThatThrownBy(requestStreamFlux::blockLast)
                .hasMessage("test")
                .isInstanceOf(RuntimeException.class));
  }

  @Test
  public void shouldSubscribeExactlyOnce1() {
    final UnboundedProcessor<ByteBuf> sender = new UnboundedProcessor<>();

    for (int i = 0; i < 1000; i++) {
      final IntObjectMap<Reassemble<?>> activeStreams = new SynchronizedIntObjectHashMap<>();
      final Payload payload = ByteBufPayload.create("testData", "testMetadata");

      final RequestStreamFlux requestStreamFlux =
          new RequestStreamFlux(
              ByteBufAllocator.DEFAULT,
              payload,
              0,
              TestStateAware.empty(),
              StreamIdSupplier.clientSupplier(),
              activeStreams,
              sender,
              PayloadDecoder.ZERO_COPY);

      Assertions.assertThatThrownBy(
              () ->
                  RaceTestUtils.race(
                      () ->
                          requestStreamFlux.subscribe(
                              null,
                              t -> {
                                throw Exceptions.propagate(t);
                              }),
                      () ->
                          requestStreamFlux.subscribe(
                              null,
                              t -> {
                                throw Exceptions.propagate(t);
                              })))
          .matches(
              t -> {
                if (t instanceof IllegalReferenceCountException) {
                  Assertions.assertThat(t).hasMessage("refCnt: 0");
                } else {
                  Assertions.assertThat(t)
                      .hasMessage("RequestStreamFlux allows only a single Subscriber");
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
          .typeOf(FrameType.REQUEST_RESPONSE)
          .hasClientSideStreamId()
          .hasStreamId(1);

      Assertions.assertThat(frame.release()).isTrue();
      Assertions.assertThat(frame.refCnt()).isZero();
    }

    Assertions.assertThat(sender.isEmpty()).isTrue();
  }

  @Test
  public void shouldBeNoOpsOnCancel() {
    final UnboundedProcessor<ByteBuf> sender = new UnboundedProcessor<>();
    final IntObjectMap<Reassemble<?>> activeStreams = new SynchronizedIntObjectHashMap<>();
    final Payload payload = ByteBufPayload.create("testData", "testMetadata");

    final RequestStreamFlux requestStreamFlux =
        new RequestStreamFlux(
            ByteBufAllocator.DEFAULT,
            payload,
            0,
            TestStateAware.empty(),
            StreamIdSupplier.clientSupplier(),
            activeStreams,
            sender,
            PayloadDecoder.ZERO_COPY);

    StepVerifier.create(requestStreamFlux, 0)
        .expectSubscription()
        .then(() -> Assertions.assertThat(activeStreams).isEmpty())
        .thenCancel()
        .verify();

    Assertions.assertThat(payload.refCnt()).isZero();

    Assertions.assertThat(activeStreams).isEmpty();
    Assertions.assertThat(sender.isEmpty()).isTrue();
  }

  @Test
  public void shouldHaveNoLeaksOnReassemblyAndCancelRacing() {
    for (int i = 0; i < 1000; i++) {

      final UnboundedProcessor<ByteBuf> sender = new UnboundedProcessor<>();
      final IntObjectMap<Reassemble<?>> activeStreams = new SynchronizedIntObjectHashMap<>();
      final Payload payload = ByteBufPayload.create("testData", "testMetadata");

      final RequestStreamFlux requestStreamFlux =
          new RequestStreamFlux(
              ByteBufAllocator.DEFAULT,
              payload,
              0,
              TestStateAware.empty(),
              StreamIdSupplier.clientSupplier(),
              activeStreams,
              sender,
              PayloadDecoder.ZERO_COPY);

      ByteBuf frame = Unpooled.wrappedBuffer("test".getBytes(CharsetUtil.UTF_8));

      StepVerifier.create(requestStreamFlux).expectSubscription().expectComplete().verifyLater();

      final ByteBuf sentFrame = sender.poll();
      FrameAssert.assertThat(sentFrame)
          .isNotNull()
          .hasPayloadSize(
              "testData".getBytes(CharsetUtil.UTF_8).length
                  + "testMetadata".getBytes(CharsetUtil.UTF_8).length)
          .hasMetadata("testMetadata")
          .hasData("testData")
          .hasNoFragmentsFollow()
          .typeOf(FrameType.REQUEST_RESPONSE)
          .hasClientSideStreamId()
          .hasStreamId(1);

      Assertions.assertThat(sentFrame.release()).isTrue();
      Assertions.assertThat(sentFrame.refCnt()).isZero();

      RaceTestUtils.race(
          requestStreamFlux::cancel,
          () -> {
            requestStreamFlux.reassemble(frame, true, false);
            frame.release();
          });

      final ByteBuf cancellationFrame = sender.poll();
      FrameAssert.assertThat(cancellationFrame)
          .isNotNull()
          .typeOf(FrameType.CANCEL)
          .hasClientSideStreamId()
          .hasStreamId(1);

      Assertions.assertThat(payload.refCnt()).isZero();
      Assertions.assertThat(frame.refCnt()).isZero();

      Assertions.assertThat(activeStreams).isEmpty();
      Assertions.assertThat(sender.isEmpty()).isTrue();
    }
  }

  @Test
  public void shouldHaveNoLeaksOnNextAndCancelRacing() {
    for (int i = 0; i < 1000; i++) {

      final UnboundedProcessor<ByteBuf> sender = new UnboundedProcessor<>();
      final IntObjectMap<Reassemble<?>> activeStreams = new SynchronizedIntObjectHashMap<>();
      final Payload payload = ByteBufPayload.create("testData", "testMetadata");

      final RequestStreamFlux requestStreamFlux =
          new RequestStreamFlux(
              ByteBufAllocator.DEFAULT,
              payload,
              0,
              TestStateAware.empty(),
              StreamIdSupplier.clientSupplier(),
              activeStreams,
              sender,
              PayloadDecoder.ZERO_COPY);

      Payload response = ByteBufPayload.create("test", "test");

      StepVerifier.create(requestStreamFlux.doOnNext(Payload::release))
          .expectSubscription()
          .expectComplete()
          .verifyLater();

      final ByteBuf sentFrame = sender.poll();
      FrameAssert.assertThat(sentFrame)
          .isNotNull()
          .hasPayloadSize(
              "testData".getBytes(CharsetUtil.UTF_8).length
                  + "testMetadata".getBytes(CharsetUtil.UTF_8).length)
          .hasMetadata("testMetadata")
          .hasData("testData")
          .hasNoFragmentsFollow()
          .typeOf(FrameType.REQUEST_RESPONSE)
          .hasClientSideStreamId()
          .hasStreamId(1);

      Assertions.assertThat(sentFrame.release()).isTrue();
      Assertions.assertThat(sentFrame.refCnt()).isZero();

      RaceTestUtils.race(requestStreamFlux::cancel, () -> requestStreamFlux.onNext(response));

      Assertions.assertThat(payload.refCnt()).isZero();
      Assertions.assertThat(response.refCnt()).isZero();

      Assertions.assertThat(activeStreams).isEmpty();
      final boolean isEmpty = sender.isEmpty();
      if (!isEmpty) {
        final ByteBuf cancellationFrame = sender.poll();
        FrameAssert.assertThat(cancellationFrame)
            .isNotNull()
            .typeOf(FrameType.CANCEL)
            .hasClientSideStreamId()
            .hasStreamId(1);
      }
      Assertions.assertThat(sender.isEmpty()).isTrue();
    }
  }

  @Test
  public void shouldSentRequestResponseFrameOnceInCaseOfRequestRacing() {
    for (int i = 0; i < 1000; i++) {

      final UnboundedProcessor<ByteBuf> sender = new UnboundedProcessor<>();
      final IntObjectMap<Reassemble<?>> activeStreams = new SynchronizedIntObjectHashMap<>();
      final Payload payload = ByteBufPayload.create("testData", "testMetadata");

      final RequestResponseMono requestResponseMono =
          new RequestResponseMono(
              ByteBufAllocator.DEFAULT,
              payload,
              0,
              TestStateAware.empty(),
              StreamIdSupplier.clientSupplier(),
              activeStreams,
              sender,
              PayloadDecoder.ZERO_COPY);

      Payload response = ByteBufPayload.create("test", "test");

      final AssertSubscriber<Payload> assertSubscriber =
          requestResponseMono.doOnNext(Payload::release).subscribeWith(AssertSubscriber.create(0));

      RaceTestUtils.race(() -> assertSubscriber.request(1), () -> assertSubscriber.request(1));

      final ByteBuf sentFrame = sender.poll();
      FrameAssert.assertThat(sentFrame)
          .isNotNull()
          .hasPayloadSize(
              "testData".getBytes(CharsetUtil.UTF_8).length
                  + "testMetadata".getBytes(CharsetUtil.UTF_8).length)
          .hasMetadata("testMetadata")
          .hasData("testData")
          .hasNoFragmentsFollow()
          .typeOf(FrameType.REQUEST_RESPONSE)
          .hasClientSideStreamId()
          .hasStreamId(1);

      requestResponseMono.onNext(response);

      assertSubscriber.isTerminated();

      Assertions.assertThat(sentFrame.release()).isTrue();
      Assertions.assertThat(sentFrame.refCnt()).isZero();

      Assertions.assertThat(payload.refCnt()).isZero();
      Assertions.assertThat(response.refCnt()).isZero();

      Assertions.assertThat(activeStreams).isEmpty();
      Assertions.assertThat(sender.isEmpty()).isTrue();
    }
  }

  @Test
  public void checkName() {
    final UnboundedProcessor<ByteBuf> sender = new UnboundedProcessor<>();
    final IntObjectMap<Reassemble<?>> activeStreams = new SynchronizedIntObjectHashMap<>();
    final Payload payload = ByteBufPayload.create("testData", "testMetadata");

    final RequestStreamFlux requestStreamFlux =
        new RequestStreamFlux(
            ByteBufAllocator.DEFAULT,
            payload,
            0,
            TestStateAware.empty(),
            StreamIdSupplier.clientSupplier(),
            activeStreams,
            sender,
            PayloadDecoder.ZERO_COPY);

    Assertions.assertThat(Scannable.from(requestStreamFlux).name())
        .isEqualTo("source(RequestStreamFlux)");
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
