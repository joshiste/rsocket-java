package io.rsocket.fragmentation;

import io.netty.buffer.ByteBuf;
import io.rsocket.frame.FrameHeaderFlyweight;

public class ReassemblyUtils {

  public static ByteBuf dataAndMetadata(ByteBuf frame) {
    return frame.retainedSlice(FrameHeaderFlyweight.size(), frame.readableBytes());
  }
}
