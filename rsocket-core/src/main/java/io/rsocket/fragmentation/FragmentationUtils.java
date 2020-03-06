package io.rsocket.fragmentation;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.rsocket.frame.FrameHeaderFlyweight;
import io.rsocket.frame.FrameType;
import io.rsocket.frame.PayloadFrameFlyweight;
import io.rsocket.frame.RequestChannelFrameFlyweight;
import io.rsocket.frame.RequestFireAndForgetFrameFlyweight;
import io.rsocket.frame.RequestResponseFrameFlyweight;
import io.rsocket.frame.RequestStreamFrameFlyweight;

public class FragmentationUtils {

    public static ByteBuf encodeFollowsFragment(
            ByteBufAllocator allocator, int mtu, int streamId, ByteBuf metadata, ByteBuf data) {
        // subtract the header bytes
        int remaining = mtu - FrameHeaderFlyweight.size();

        ByteBuf metadataFragment = null;
        if (metadata.isReadable()) {
            // subtract the metadata frame length
            remaining -= 3;
            int r = Math.min(remaining, metadata.readableBytes());
            remaining -= r;
            metadataFragment = metadata.readRetainedSlice(r);
        }

        ByteBuf dataFragment = Unpooled.EMPTY_BUFFER;
        if (remaining > 0 && data.isReadable()) {
            int r = Math.min(remaining, data.readableBytes());
            dataFragment = data.readRetainedSlice(r);
        }

        boolean follows = data.isReadable() || metadata.isReadable();
        return PayloadFrameFlyweight.encode(
                allocator, streamId, follows, false, true, metadataFragment, dataFragment);
    }


    public static ByteBuf encodeFirstFragment(
            ByteBufAllocator allocator,
            int mtu,
            FrameType frameType,
            int streamId,
            ByteBuf metadata,
            ByteBuf data) {
        // subtract the header bytes
        int remaining = mtu - FrameHeaderFlyweight.size();

        ByteBuf metadataFragment = null;
        if (metadata.isReadable()) {
            // subtract the metadata frame length
            remaining -= 3;
            int r = Math.min(remaining, metadata.readableBytes());
            remaining -= r;
            metadataFragment = metadata.readRetainedSlice(r);
        }

        ByteBuf dataFragment = Unpooled.EMPTY_BUFFER;
        if (remaining > 0 && data.isReadable()) {
            int r = Math.min(remaining, data.readableBytes());
            dataFragment = data.readRetainedSlice(r);
        }

        switch (frameType) {
            case REQUEST_FNF:
                return RequestFireAndForgetFrameFlyweight.encode(
                        allocator, streamId, true, metadataFragment, dataFragment);
            case REQUEST_RESPONSE:
                return RequestResponseFrameFlyweight.encode(
                        allocator, streamId, true, metadataFragment, dataFragment);
            // Payload and synthetic types from the responder side
            case PAYLOAD:
                return PayloadFrameFlyweight.encode(
                        allocator, streamId, true, false, false, metadataFragment, dataFragment);
            case NEXT:
                return PayloadFrameFlyweight.encode(
                        allocator, streamId, true, false, true, metadataFragment, dataFragment);
            case NEXT_COMPLETE:
                return PayloadFrameFlyweight.encode(
                        allocator, streamId, true, true, true, metadataFragment, dataFragment);
            case COMPLETE:
                return PayloadFrameFlyweight.encode(
                        allocator, streamId, true, true, false, metadataFragment, dataFragment);
            default:
                throw new IllegalStateException("unsupported fragment type: " + frameType);
        }
    }

    public static ByteBuf encodeFirstFragment(
            ByteBufAllocator allocator,
            int mtu,
            int initialRequestN,
            FrameType frameType,
            int streamId,
            ByteBuf metadata,
            ByteBuf data) {
        // subtract the header bytes
        int remaining = mtu - FrameHeaderFlyweight.size()
                // substract the initial request n
                - Integer.BYTES;

        ByteBuf metadataFragment = null;
        if (metadata.isReadable()) {
            // subtract the metadata frame length
            remaining -= 3;
            int r = Math.min(remaining, metadata.readableBytes());
            remaining -= r;
            metadataFragment = metadata.readRetainedSlice(r);
        }

        ByteBuf dataFragment = Unpooled.EMPTY_BUFFER;
        if (remaining > 0 && data.isReadable()) {
            int r = Math.min(remaining, data.readableBytes());
            dataFragment = data.readRetainedSlice(r);
        }

        switch (frameType) {
            // Requester Side
            case REQUEST_STREAM:
                return RequestStreamFrameFlyweight.encode(
                        allocator,
                        streamId,
                        true,
                        initialRequestN,
                        metadataFragment,
                        dataFragment);
            case REQUEST_CHANNEL:
                return RequestChannelFrameFlyweight.encode(
                        allocator,
                        streamId,
                        true,
                        false,
                        initialRequestN,
                        metadataFragment,
                        dataFragment);
            default:
                throw new IllegalStateException("unsupported fragment type: " + frameType);
        }
    }
}
