package io.rsocket;

import static org.assertj.core.error.ShouldBe.shouldBe;
import static org.assertj.core.error.ShouldBeEqual.shouldBeEqual;
import static org.assertj.core.error.ShouldHave.shouldHave;
import static org.assertj.core.error.ShouldNotHave.shouldNotHave;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.util.CharsetUtil;
import io.rsocket.frame.ByteBufRepresentation;
import io.rsocket.frame.FrameHeaderFlyweight;
import io.rsocket.frame.FrameType;
import io.rsocket.frame.MetadataPushFrameFlyweight;
import io.rsocket.frame.PayloadFrameFlyweight;
import io.rsocket.frame.RequestNFrameFlyweight;
import io.rsocket.frame.RequestStreamFrameFlyweight;
import java.nio.charset.Charset;
import org.assertj.core.api.AbstractAssert;
import org.assertj.core.api.Condition;
import org.assertj.core.error.BasicErrorMessageFactory;
import org.assertj.core.internal.Failures;
import org.assertj.core.internal.Objects;

public class FrameAssert extends AbstractAssert<FrameAssert, ByteBuf> {

  public static FrameAssert assertThat(ByteBuf frame) {
    return new FrameAssert(frame);
  }

  private final Failures failures = Failures.instance();

  public FrameAssert(ByteBuf frame) {
    super(frame, FrameAssert.class);
  }

  public FrameAssert hasMetadata() {
    assertValid();

    if (!FrameHeaderFlyweight.hasMetadata(actual)) {
      throw failures.failure(info, shouldHave(actual, new Condition<>("metadata present")));
    }

    return this;
  }

  public FrameAssert hasNoMetadata() {
    assertValid();

    if (FrameHeaderFlyweight.hasMetadata(actual)) {
      throw failures.failure(info, shouldHave(actual, new Condition<>("metadata absent")));
    }

    return this;
  }

  public FrameAssert hasMetadata(String metadata, Charset charset) {
    return hasMetadata(metadata.getBytes(charset));
  }

  public FrameAssert hasMetadata(String metadataUtf8) {
    return hasMetadata(metadataUtf8, CharsetUtil.UTF_8);
  }

  public FrameAssert hasMetadata(byte[] metadata) {
    return hasMetadata(Unpooled.wrappedBuffer(metadata));
  }

  public FrameAssert hasMetadata(ByteBuf metadata) {
    hasMetadata();

    final FrameType frameType = FrameHeaderFlyweight.frameType(actual);
    ByteBuf content;
    if (frameType == FrameType.METADATA_PUSH) {
      content = MetadataPushFrameFlyweight.metadata(actual);
    } else if (frameType.hasInitialRequestN()) {
      content = RequestStreamFrameFlyweight.metadata(actual);
    } else {
      content = PayloadFrameFlyweight.metadata(actual);
    }

    if (!ByteBufUtil.equals(content, metadata)) {
      throw failures.failure(info, shouldBeEqual(content, metadata, new ByteBufRepresentation()));
    }

    return this;
  }

  public FrameAssert hasData(String dataUtf8) {
    return hasData(dataUtf8, CharsetUtil.UTF_8);
  }

  public FrameAssert hasData(String data, Charset charset) {
    return hasData(data.getBytes(charset));
  }

  public FrameAssert hasData(byte[] data) {
    return hasData(Unpooled.wrappedBuffer(data));
  }

  public FrameAssert hasData(ByteBuf data) {
    assertValid();

    ByteBuf content;
    final FrameType frameType = FrameHeaderFlyweight.frameType(actual);
    if (!frameType.canHaveData()) {
      throw failures.failure(
          info,
          new BasicErrorMessageFactory(
              "%nExpecting:  %n<%s>   %nto have data content but frame type  %n<%s> does not support data content",
              actual, frameType));
    } else if (frameType.hasInitialRequestN()) {
      content = RequestStreamFrameFlyweight.data(actual);
    } else {
      content = PayloadFrameFlyweight.data(actual);
    }

    if (!ByteBufUtil.equals(content, data)) {
      throw failures.failure(info, shouldBeEqual(content, data, new ByteBufRepresentation()));
    }

    return this;
  }

  public FrameAssert hasFragmentsFollow() {
    return hasFollows(true);
  }

  public FrameAssert hasNoFragmentsFollow() {
    return hasFollows(false);
  }

  public FrameAssert hasFollows(boolean hasFollows) {
    assertValid();

    if (FrameHeaderFlyweight.hasFollows(actual) != hasFollows) {
      throw failures.failure(
          info,
          hasFollows
              ? shouldHave(actual, new Condition<>("follows fragment present"))
              : shouldNotHave(actual, new Condition<>("follows fragment present")));
    }

    return this;
  }

  public FrameAssert typeOf(FrameType frameType) {
    assertValid();

    final FrameType currentFrameType = FrameHeaderFlyweight.frameType(actual);
    if (currentFrameType != frameType) {
      throw failures.failure(
          info, shouldBe(currentFrameType, new Condition<>("frame of type [" + frameType + "]")));
    }

    return this;
  }

  public FrameAssert hasStreamId(int streamId) {
    assertValid();

    final int currentStreamId = FrameHeaderFlyweight.streamId(actual);
    if (currentStreamId != streamId) {
      throw failures.failure(
          info,
          new BasicErrorMessageFactory(
              "%nExpecting streamId:%n<%s>%n to be equal %n<%s>", currentStreamId, streamId));
    }

    return this;
  }

  public FrameAssert hasStreamIdZero() {
    return hasStreamId(0);
  }

  public FrameAssert hasClientSideStreamId() {
    assertValid();

    final int currentStreamId = FrameHeaderFlyweight.streamId(actual);
    if (currentStreamId % 2 != 1) {
      throw failures.failure(
          info,
          new BasicErrorMessageFactory(
              "%nExpecting Client Side StreamId %nbut was "
                  + (currentStreamId == 0 ? "Stream Id 0" : "Server Side Stream Id")));
    }

    return this;
  }

  public FrameAssert hasServerSideStreamId() {
    assertValid();

    final int currentStreamId = FrameHeaderFlyweight.streamId(actual);
    if (currentStreamId == 0 || currentStreamId % 2 != 0) {
      throw failures.failure(
          info,
          new BasicErrorMessageFactory(
              "%nExpecting %n  Server Side Stream Id %nbut was %n  "
                  + (currentStreamId == 0 ? "Stream Id 0" : "Client Side Stream Id")));
    }

    return this;
  }

  public FrameAssert hasPayloadSize(int frameLength) {
    assertValid();

    final FrameType currentFrameType = FrameHeaderFlyweight.frameType(actual);

    final int currentFrameLength =
        actual.readableBytes()
            - FrameHeaderFlyweight.size()
            - (FrameHeaderFlyweight.hasMetadata(actual) ? 3 : 0)
            - (currentFrameType.hasInitialRequestN() ? Integer.BYTES : 0);
    if (currentFrameLength != frameLength) {
      throw failures.failure(
          info,
          new BasicErrorMessageFactory(
              "%nExpecting %n<%s> %nframe payload size to be equal to  %n<%s>  %nbut was  %n<%s>",
              actual, frameLength, currentFrameLength));
    }

    return this;
  }

  public FrameAssert hasRequestN(int n) {
    assertValid();

    final FrameType currentFrameType = FrameHeaderFlyweight.frameType(actual);
    int requestN;
    if (currentFrameType.hasInitialRequestN()) {
      requestN = RequestStreamFrameFlyweight.initialRequestN(actual);
    } else if (currentFrameType == FrameType.REQUEST_N) {
      requestN = RequestNFrameFlyweight.requestN(actual);
    } else {
      throw failures.failure(
          info,
          new BasicErrorMessageFactory(
              "%nExpecting:  %n<%s>   %nto have requestN but frame type  %n<%s> does not support requestN",
              actual, currentFrameType));
    }

    if (requestN != n) {
      throw failures.failure(
          info,
          new BasicErrorMessageFactory(
              "%nExpecting:  %n<%s>   %nto have  %nrequestN(<%s>)  but got  %nrequestN(<%s>)",
              actual, n, requestN));
    }

    return this;
  }

  private void assertValid() {
    Objects.instance().assertNotNull(info, actual);

    try {
      FrameHeaderFlyweight.frameType(actual);
    } catch (Throwable t) {
      throw failures.failure(
          info, shouldBe(actual, new Condition<>("a valid frame, but got exception [" + t + "]")));
    }
  }
}
