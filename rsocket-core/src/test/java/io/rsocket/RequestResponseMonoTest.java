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

public class RequestResponseMonoTest {

  @BeforeAll
  public static void setUp() {
    StepVerifier.setDefaultTimeout(Duration.ofSeconds(2));
  }

  @ParameterizedTest
  @MethodSource("frameShouldBeSentOnSubscriptionResponses")
  public void frameShouldBeSentOnSubscription(
      BiFunction<RequestResponseMono, StepVerifier.Step<Payload>, StepVerifier> transformer) {
    final UnboundedProcessor<ByteBuf> sender = new UnboundedProcessor<>();
    final Payload payload = ByteBufPayload.create("testData", "testMetadata");
    final IntObjectMap<Reassemble<?>> activeStreams = new SynchronizedIntObjectHashMap<>();

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

    Assertions.assertThat(activeStreams).isEmpty();

    transformer
        .apply(
            requestResponseMono,
            StepVerifier.create(requestResponseMono, 0)
                .expectSubscription()
                .then(() -> Assertions.assertThat(payload.refCnt()).isOne())
                .then(() -> Assertions.assertThat(activeStreams).isEmpty())
                .thenRequest(1)
                .then(() -> Assertions.assertThat(payload.refCnt()).isZero())
                .then(
                    () ->
                        Assertions.assertThat(activeStreams).containsEntry(1, requestResponseMono)))
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

  static Stream<BiFunction<RequestResponseMono, StepVerifier.Step<Payload>, StepVerifier>>
      frameShouldBeSentOnSubscriptionResponses() {
    return Stream.of(
        (rrm, sv) ->
            sv.then(() -> rrm.onNext(EmptyPayload.INSTANCE))
                .expectNext(EmptyPayload.INSTANCE)
                .expectComplete(),
        (rrm, sv) -> sv.then(rrm::onComplete).expectComplete(),
        (rrm, sv) ->
            sv.then(() -> rrm.onError(new ApplicationErrorException("test")))
                .expectErrorSatisfies(
                    t ->
                        Assertions.assertThat(t)
                            .hasMessage("test")
                            .isInstanceOf(ApplicationErrorException.class)),
        (rrm, sv) -> {
          final byte[] metadata = new byte[65];
          final byte[] data = new byte[129];
          ThreadLocalRandom.current().nextBytes(metadata);
          ThreadLocalRandom.current().nextBytes(data);

          final Payload payload = ByteBufPayload.create(data, metadata);

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
                    rrm.reassemble(followingFrame, true, false);
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
                    rrm.reassemble(followingFrame, true, false);
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
                    rrm.reassemble(followingFrame, true, false);
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
                    rrm.reassemble(followingFrame, false, false);
                    followingFrame.release();
                  })
              .assertNext(
                  p -> {
                    Assertions.assertThat(p.data()).isEqualTo(Unpooled.wrappedBuffer(data));

                    Assertions.assertThat(p.metadata()).isEqualTo(Unpooled.wrappedBuffer(metadata));
                    p.release();
                  })
              .then(payload::release)
              .expectComplete();
        },
        (rrm, sv) -> {
          final byte[] metadata = new byte[65];
          final byte[] data = new byte[129];
          ThreadLocalRandom.current().nextBytes(metadata);
          ThreadLocalRandom.current().nextBytes(data);

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
              sv.then(
                      () -> {
                        rrm.reassemble(fragments[0], true, false);
                        fragments[0].release();
                      })
                  .then(
                      () -> {
                        rrm.reassemble(fragments[1], true, false);
                        fragments[1].release();
                      })
                  .then(
                      () -> {
                        rrm.reassemble(fragments[2], true, false);
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
      BiFunction<RequestResponseMono, StepVerifier.Step<Payload>, StepVerifier> transformer) {
    final UnboundedProcessor<ByteBuf> sender = new UnboundedProcessor<>();

    final byte[] metadata = new byte[65];
    final byte[] data = new byte[129];
    ThreadLocalRandom.current().nextBytes(metadata);
    ThreadLocalRandom.current().nextBytes(data);

    final Payload payload = ByteBufPayload.create(data, metadata);
    final IntObjectMap<Reassemble<?>> activeStreams = new SynchronizedIntObjectHashMap<>();

    final RequestResponseMono requestResponseMono =
        new RequestResponseMono(
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
            requestResponseMono,
            StepVerifier.create(requestResponseMono, 0)
                .expectSubscription()
                .then(() -> Assertions.assertThat(payload.refCnt()).isOne())
                .then(() -> Assertions.assertThat(activeStreams).isEmpty())
                .thenRequest(1)
                .then(() -> Assertions.assertThat(payload.refCnt()).isZero())
                .then(
                    () ->
                        Assertions.assertThat(activeStreams).containsEntry(1, requestResponseMono)))
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
  public void shouldErrorOnIncorrectRefCntInGivenPayload(
      Consumer<RequestResponseMono> monoConsumer) {
    final UnboundedProcessor<ByteBuf> sender = new UnboundedProcessor<>();
    final IntObjectMap<Reassemble<?>> activeStreams = new SynchronizedIntObjectHashMap<>();
    final Payload payload = ByteBufPayload.create("");
    payload.release();

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

    Assertions.assertThat(activeStreams).isEmpty();

    monoConsumer.accept(requestResponseMono);

    Assertions.assertThat(activeStreams).isEmpty();
    Assertions.assertThat(sender).isEmpty();
  }

  static Stream<Consumer<RequestResponseMono>> shouldErrorOnIncorrectRefCntInGivenPayloadSource() {
    return Stream.of(
        (s) ->
            StepVerifier.create(s)
                .expectSubscription()
                .expectError(IllegalReferenceCountException.class)
                .verify(),
        requestResponseMono ->
            Assertions.assertThatThrownBy(requestResponseMono::block)
                .isInstanceOf(IllegalReferenceCountException.class));
  }

  @ParameterizedTest
  @MethodSource("shouldErrorIfFragmentExitsAllowanceIfFragmentationDisabledSource")
  public void shouldErrorIfFragmentExitsAllowanceIfFragmentationDisabled(
      Consumer<RequestResponseMono> monoConsumer) {
    final UnboundedProcessor<ByteBuf> sender = new UnboundedProcessor<>();
    final IntObjectMap<Reassemble<?>> activeStreams = new SynchronizedIntObjectHashMap<>();

    final byte[] metadata = new byte[FrameLengthFlyweight.FRAME_LENGTH_MASK];
    final byte[] data = new byte[FrameLengthFlyweight.FRAME_LENGTH_MASK];
    ThreadLocalRandom.current().nextBytes(metadata);
    ThreadLocalRandom.current().nextBytes(data);

    final Payload payload = ByteBufPayload.create(data, metadata);

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

    Assertions.assertThat(activeStreams).isEmpty();

    monoConsumer.accept(requestResponseMono);

    Assertions.assertThat(payload.refCnt()).isZero();

    Assertions.assertThat(activeStreams).isEmpty();
    Assertions.assertThat(sender).isEmpty();
  }

  static Stream<Consumer<RequestResponseMono>>
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
        requestResponseMono ->
            Assertions.assertThatThrownBy(requestResponseMono::block)
                .hasMessage("Too Big Payload size")
                .isInstanceOf(IllegalArgumentException.class));
  }

  @ParameterizedTest
  @MethodSource("shouldErrorIfNoAvailabilitySource")
  public void shouldErrorIfNoAvailability(Consumer<RequestResponseMono> monoConsumer) {
    final UnboundedProcessor<ByteBuf> sender = new UnboundedProcessor<>();
    final IntObjectMap<Reassemble<?>> activeStreams = new SynchronizedIntObjectHashMap<>();
    final Payload payload = ByteBufPayload.create("testData", "testMetadata");

    final RequestResponseMono requestResponseMono =
        new RequestResponseMono(
            ByteBufAllocator.DEFAULT,
            payload,
            0,
            TestStateAware.error(new RuntimeException("test")),
            StreamIdSupplier.clientSupplier(),
            activeStreams,
            sender,
            PayloadDecoder.ZERO_COPY);

    Assertions.assertThat(activeStreams).isEmpty();

    monoConsumer.accept(requestResponseMono);

    Assertions.assertThat(payload.refCnt()).isZero();

    Assertions.assertThat(activeStreams).isEmpty();
  }

  static Stream<Consumer<RequestResponseMono>> shouldErrorIfNoAvailabilitySource() {
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
        requestResponseMono ->
            Assertions.assertThatThrownBy(requestResponseMono::block)
                .hasMessage("test")
                .isInstanceOf(RuntimeException.class));
  }

  @Test
  public void shouldSubscribeExactlyOnce1() {
    final UnboundedProcessor<ByteBuf> sender = new UnboundedProcessor<>();

    for (int i = 0; i < 1000; i++) {
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

      Assertions.assertThatThrownBy(
              () ->
                  RaceTestUtils.race(
                      () ->
                          requestResponseMono.subscribe(
                              null,
                              t -> {
                                throw Exceptions.propagate(t);
                              }),
                      () ->
                          requestResponseMono.subscribe(
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
                      .hasMessage("RequestResponseMono allows only a single Subscriber");
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

    StepVerifier.create(requestResponseMono, 0)
        .expectSubscription()
        .then(() -> Assertions.assertThat(activeStreams).isEmpty())
        .thenCancel()
        .verify();

    Assertions.assertThat(payload.refCnt()).isZero();

    Assertions.assertThat(activeStreams).isEmpty();
    Assertions.assertThat(sender.isEmpty()).isTrue();
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
  public void shouldHaveNoLeaksOnReassemblyAndCancelRacing() {
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

      ByteBuf frame = Unpooled.wrappedBuffer("test".getBytes(CharsetUtil.UTF_8));

      StepVerifier.create(requestResponseMono).expectSubscription().expectComplete().verifyLater();

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
          requestResponseMono::cancel,
          () -> {
            requestResponseMono.reassemble(frame, true, false);
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

      StepVerifier.create(requestResponseMono.doOnNext(Payload::release))
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

      RaceTestUtils.race(requestResponseMono::cancel, () -> requestResponseMono.onNext(response));

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
  public void checkName() {
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

    Assertions.assertThat(Scannable.from(requestResponseMono).name())
        .isEqualTo("source(RequestResponseMono)");
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