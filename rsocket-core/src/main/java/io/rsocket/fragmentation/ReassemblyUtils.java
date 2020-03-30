package io.rsocket.fragmentation;

import static io.rsocket.frame.FrameLengthFlyweight.FRAME_LENGTH_MASK;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.rsocket.frame.FrameHeaderFlyweight;

public class ReassemblyUtils {

  public static CompositeByteBuf addFollowingFrame(
      CompositeByteBuf frames, ByteBuf followingFrame) {
    if (frames.readableBytes() == 0) {
      return frames.addComponent(true, followingFrame.retain());
    }

    final boolean hasMetadata = FrameHeaderFlyweight.hasMetadata(followingFrame);

    // skip headers
    followingFrame.skipBytes(FrameHeaderFlyweight.size());

    // if has metadata, then we have to increase metadata length in containing frames
    // CompositeByteBuf
    if (hasMetadata) {
      frames.markReaderIndex().skipBytes(FrameHeaderFlyweight.size());

      final int nextMetadataLength = decodeLength(frames) + decodeLength(followingFrame);

      frames.resetReaderIndex();

      frames.markWriterIndex();
      frames.writerIndex(FrameHeaderFlyweight.size());

      encodeLength(frames, nextMetadataLength);

      frames.resetWriterIndex();
    }

    return frames.addComponent(true, followingFrame.retain());
  }

  private static void encodeLength(final ByteBuf byteBuf, final int length) {
    if ((length & ~FRAME_LENGTH_MASK) != 0) {
      throw new IllegalArgumentException("Length is larger than 24 bits");
    }
    // Write each byte separately in reverse order, this mean we can write 1 << 23 without
    // overflowing.
    byteBuf.writeByte(length >> 16);
    byteBuf.writeByte(length >> 8);
    byteBuf.writeByte(length);
  }

  private static int decodeLength(final ByteBuf byteBuf) {
    int length = (byteBuf.readByte() & 0xFF) << 16;
    length |= (byteBuf.readByte() & 0xFF) << 8;
    length |= byteBuf.readByte() & 0xFF;
    return length;
  }
}
