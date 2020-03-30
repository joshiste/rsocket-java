/// *
// * Copyright 2015-2018 the original author or authors.
// *
// * Licensed under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// *
// *     http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
// package io.rsocket.fragmentation;
//
// import io.netty.buffer.ByteBuf;
// import io.netty.buffer.ByteBufAllocator;
// import io.netty.buffer.Unpooled;
// import io.rsocket.frame.*;
// import io.rsocket.util.DefaultPayload;
// import java.util.concurrent.ThreadLocalRandom;
// import org.junit.Assert;
// import org.junit.jupiter.api.DisplayName;
// import org.junit.jupiter.api.Test;
// import org.reactivestreams.Publisher;
// import reactor.core.publisher.Flux;
// import reactor.test.StepVerifier;
//
// final class FrameFragmenterTest {
//  private static byte[] data = new byte[4096];
//  private static byte[] metadata = new byte[4096];
//
//  static {
//    ThreadLocalRandom.current().nextBytes(data);
//    ThreadLocalRandom.current().nextBytes(metadata);
//  }
//
//  private ByteBufAllocator allocator = ByteBufAllocator.DEFAULT;
//
//  @Test
//  void testGettingData() {
//    ByteBuf rr =
//        RequestResponseFrameFlyweight.encode(allocator, 1, true, DefaultPayload.create(data));
//    ByteBuf fnf =
//        RequestFireAndForgetFrameFlyweight.encode(allocator, 1, true,
// DefaultPayload.create(data));
//    ByteBuf rs =
//        RequestStreamFrameFlyweight.encode(allocator, 1, true, 1, DefaultPayload.create(data));
//    ByteBuf rc =
//        RequestChannelFrameFlyweight.encode(
//            allocator, 1, true, false, 1, DefaultPayload.create(data));
//
//    ByteBuf data = FrameFragmenter.getData(rr, FrameType.REQUEST_RESPONSE);
//    Assert.assertEquals(data, Unpooled.wrappedBuffer(data));
//    data.release();
//
//    data = FrameFragmenter.getData(fnf, FrameType.REQUEST_FNF);
//    Assert.assertEquals(data, Unpooled.wrappedBuffer(data));
//    data.release();
//
//    data = FrameFragmenter.getData(rs, FrameType.REQUEST_STREAM);
//    Assert.assertEquals(data, Unpooled.wrappedBuffer(data));
//    data.release();
//
//    data = FrameFragmenter.getData(rc, FrameType.REQUEST_CHANNEL);
//    Assert.assertEquals(data, Unpooled.wrappedBuffer(data));
//    data.release();
//  }
//
//  @Test
//  void testGettingMetadata() {
//    ByteBuf rr =
//        RequestResponseFrameFlyweight.encode(
//            allocator, 1, true, DefaultPayload.create(data, metadata));
//    ByteBuf fnf =
//        RequestFireAndForgetFrameFlyweight.encode(
//            allocator, 1, true, DefaultPayload.create(data, metadata));
//    ByteBuf rs =
//        RequestStreamFrameFlyweight.encode(
//            allocator, 1, true, 1, DefaultPayload.create(data, metadata));
//    ByteBuf rc =
//        RequestChannelFrameFlyweight.encode(
//            allocator, 1, true, false, 1, DefaultPayload.create(data, metadata));
//
//    ByteBuf data = FrameFragmenter.getMetadata(rr, FrameType.REQUEST_RESPONSE);
//    Assert.assertEquals(data, Unpooled.wrappedBuffer(metadata));
//    data.release();
//
//    data = FrameFragmenter.getMetadata(fnf, FrameType.REQUEST_FNF);
//    Assert.assertEquals(data, Unpooled.wrappedBuffer(metadata));
//    data.release();
//
//    data = FrameFragmenter.getMetadata(rs, FrameType.REQUEST_STREAM);
//    Assert.assertEquals(data, Unpooled.wrappedBuffer(metadata));
//    data.release();
//
//    data = FrameFragmenter.getMetadata(rc, FrameType.REQUEST_CHANNEL);
//    Assert.assertEquals(data, Unpooled.wrappedBuffer(metadata));
//    data.release();
//  }
//
//  @Test
//  void returnEmptBufferWhenNoMetadataPresent() {
//    ByteBuf rr =
//        RequestResponseFrameFlyweight.encode(allocator, 1, true, DefaultPayload.create(data));
//
//    ByteBuf data = FrameFragmenter.getMetadata(rr, FrameType.REQUEST_RESPONSE);
//    Assert.assertEquals(data, Unpooled.EMPTY_BUFFER);
//    data.release();
//  }
//
//  @DisplayName("encode first frame")
//  @Test
//  void encodeFirstFrameWithData() {
//    ByteBuf rr =
//        RequestResponseFrameFlyweight.encode(allocator, 1, true, DefaultPayload.create(data));
//
//    ByteBuf fragment =
//        FrameFragmenter.encodeFirstFragment(
//            allocator,
//            256,
//            rr,
//            FrameType.REQUEST_RESPONSE,
//            1,
//            Unpooled.EMPTY_BUFFER,
//            Unpooled.wrappedBuffer(data));
//
//    Assert.assertEquals(256, fragment.readableBytes());
//    Assert.assertEquals(FrameType.REQUEST_RESPONSE, FrameHeaderFlyweight.frameType(fragment));
//    Assert.assertEquals(1, FrameHeaderFlyweight.streamId(fragment));
//    Assert.assertTrue(FrameHeaderFlyweight.hasFollows(fragment));
//
//    ByteBuf data = RequestResponseFrameFlyweight.data(fragment);
//    ByteBuf byteBuf = Unpooled.wrappedBuffer(this.data).readSlice(data.readableBytes());
//    Assert.assertEquals(byteBuf, data);
//
//    Assert.assertFalse(FrameHeaderFlyweight.hasMetadata(fragment));
//  }
//
//  @DisplayName("encode first channel frame")
//  @Test
//  void encodeFirstWithDataChannel() {
//    ByteBuf rc =
//        RequestChannelFrameFlyweight.encode(
//            allocator, 1, true, false, 10, DefaultPayload.create(data));
//
//    ByteBuf fragment =
//        FrameFragmenter.encodeFirstFragment(
//            allocator,
//            256,
//            rc,
//            FrameType.REQUEST_CHANNEL,
//            1,
//            Unpooled.EMPTY_BUFFER,
//            Unpooled.wrappedBuffer(data));
//
//    Assert.assertEquals(256, fragment.readableBytes());
//    Assert.assertEquals(FrameType.REQUEST_CHANNEL, FrameHeaderFlyweight.frameType(fragment));
//    Assert.assertEquals(1, FrameHeaderFlyweight.streamId(fragment));
//    Assert.assertEquals(10, RequestChannelFrameFlyweight.initialRequestN(fragment));
//    Assert.assertTrue(FrameHeaderFlyweight.hasFollows(fragment));
//
//    ByteBuf data = RequestChannelFrameFlyweight.data(fragment);
//    ByteBuf byteBuf = Unpooled.wrappedBuffer(this.data).readSlice(data.readableBytes());
//    Assert.assertEquals(byteBuf, data);
//
//    Assert.assertFalse(FrameHeaderFlyweight.hasMetadata(fragment));
//  }
//
//  @DisplayName("encode first stream frame")
//  @Test
//  void encodeFirstWithDataStream() {
//    ByteBuf rc =
//        RequestStreamFrameFlyweight.encode(allocator, 1, true, 50, DefaultPayload.create(data));
//
//    ByteBuf fragment =
//        FrameFragmenter.encodeFirstFragment(
//            allocator,
//            256,
//            rc,
//            FrameType.REQUEST_STREAM,
//            1,
//            Unpooled.EMPTY_BUFFER,
//            Unpooled.wrappedBuffer(data));
//
//    Assert.assertEquals(256, fragment.readableBytes());
//    Assert.assertEquals(FrameType.REQUEST_STREAM, FrameHeaderFlyweight.frameType(fragment));
//    Assert.assertEquals(1, FrameHeaderFlyweight.streamId(fragment));
//    Assert.assertEquals(50, RequestStreamFrameFlyweight.initialRequestN(fragment));
//    Assert.assertTrue(FrameHeaderFlyweight.hasFollows(fragment));
//
//    ByteBuf data = RequestStreamFrameFlyweight.data(fragment);
//    ByteBuf byteBuf = Unpooled.wrappedBuffer(this.data).readSlice(data.readableBytes());
//    Assert.assertEquals(byteBuf, data);
//
//    Assert.assertFalse(FrameHeaderFlyweight.hasMetadata(fragment));
//  }
//
//  @DisplayName("encode first frame with only metadata")
//  @Test
//  void encodeFirstFrameWithMetadata() {
//    ByteBuf rr =
//        RequestResponseFrameFlyweight.encode(
//            allocator,
//            1,
//            true,
//            DefaultPayload.create(Unpooled.EMPTY_BUFFER, Unpooled.wrappedBuffer(metadata)));
//
//    ByteBuf fragment =
//        FrameFragmenter.encodeFirstFragment(
//            allocator,
//            256,
//            rr,
//            FrameType.REQUEST_RESPONSE,
//            1,
//            Unpooled.wrappedBuffer(metadata),
//            Unpooled.EMPTY_BUFFER);
//
//    Assert.assertEquals(256, fragment.readableBytes());
//    Assert.assertEquals(FrameType.REQUEST_RESPONSE, FrameHeaderFlyweight.frameType(fragment));
//    Assert.assertEquals(1, FrameHeaderFlyweight.streamId(fragment));
//    Assert.assertTrue(FrameHeaderFlyweight.hasFollows(fragment));
//
//    ByteBuf data = RequestResponseFrameFlyweight.data(fragment);
//    Assert.assertEquals(data, Unpooled.EMPTY_BUFFER);
//
//    Assert.assertTrue(FrameHeaderFlyweight.hasMetadata(fragment));
//  }
//
//  @DisplayName("encode first stream frame with data and metadata")
//  @Test
//  void encodeFirstWithDataAndMetadataStream() {
//    ByteBuf rc =
//        RequestStreamFrameFlyweight.encode(
//            allocator, 1, true, 50, DefaultPayload.create(data, metadata));
//
//    ByteBuf fragment =
//        FrameFragmenter.encodeFirstFragment(
//            allocator,
//            256,
//            rc,
//            FrameType.REQUEST_STREAM,
//            1,
//            Unpooled.wrappedBuffer(metadata),
//            Unpooled.wrappedBuffer(data));
//
//    Assert.assertEquals(256, fragment.readableBytes());
//    Assert.assertEquals(FrameType.REQUEST_STREAM, FrameHeaderFlyweight.frameType(fragment));
//    Assert.assertEquals(1, FrameHeaderFlyweight.streamId(fragment));
//    Assert.assertEquals(50, RequestStreamFrameFlyweight.initialRequestN(fragment));
//    Assert.assertTrue(FrameHeaderFlyweight.hasFollows(fragment));
//
//    ByteBuf data = RequestStreamFrameFlyweight.data(fragment);
//    Assert.assertEquals(0, data.readableBytes());
//
//    ByteBuf metadata = RequestStreamFrameFlyweight.metadata(fragment);
//    ByteBuf byteBuf = Unpooled.wrappedBuffer(this.metadata).readSlice(metadata.readableBytes());
//    Assert.assertEquals(byteBuf, metadata);
//
//    Assert.assertTrue(FrameHeaderFlyweight.hasMetadata(fragment));
//  }
//
//  @DisplayName("fragments frame with only data")
//  @Test
//  void fragmentData() {
//    ByteBuf rr =
//        RequestResponseFrameFlyweight.encode(allocator, 1, true, DefaultPayload.create(data));
//
//    Publisher<ByteBuf> fragments =
//        FrameFragmenter.fragmentFrame(allocator, 1024, rr, FrameType.REQUEST_RESPONSE, false);
//
//    StepVerifier.create(Flux.from(fragments).doOnError(Throwable::printStackTrace))
//        .expectNextCount(1)
//        .assertNext(
//            byteBuf -> {
//              Assert.assertEquals(FrameType.NEXT, FrameHeaderFlyweight.frameType(byteBuf));
//              Assert.assertEquals(1, FrameHeaderFlyweight.streamId(byteBuf));
//              Assert.assertTrue(FrameHeaderFlyweight.hasFollows(byteBuf));
//            })
//        .expectNextCount(2)
//        .assertNext(
//            byteBuf -> {
//              Assert.assertEquals(FrameType.NEXT, FrameHeaderFlyweight.frameType(byteBuf));
//              Assert.assertFalse(FrameHeaderFlyweight.hasFollows(byteBuf));
//            })
//        .verifyComplete();
//  }
//
//  @DisplayName("fragments frame with only metadata")
//  @Test
//  void fragmentMetadata() {
//    ByteBuf rr =
//        RequestStreamFrameFlyweight.encode(
//            allocator,
//            1,
//            true,
//            10,
//            DefaultPayload.create(Unpooled.EMPTY_BUFFER, Unpooled.wrappedBuffer(metadata)));
//
//    Publisher<ByteBuf> fragments =
//        FrameFragmenter.fragmentFrame(allocator, 1024, rr, FrameType.REQUEST_STREAM, false);
//
//    StepVerifier.create(Flux.from(fragments).doOnError(Throwable::printStackTrace))
//        .expectNextCount(1)
//        .assertNext(
//            byteBuf -> {
//              Assert.assertEquals(FrameType.NEXT, FrameHeaderFlyweight.frameType(byteBuf));
//              Assert.assertEquals(1, FrameHeaderFlyweight.streamId(byteBuf));
//              Assert.assertTrue(FrameHeaderFlyweight.hasFollows(byteBuf));
//            })
//        .expectNextCount(2)
//        .assertNext(
//            byteBuf -> {
//              Assert.assertEquals(FrameType.NEXT, FrameHeaderFlyweight.frameType(byteBuf));
//              Assert.assertFalse(FrameHeaderFlyweight.hasFollows(byteBuf));
//            })
//        .verifyComplete();
//  }
//
//  @DisplayName("fragments frame with  data and metadata")
//  @Test
//  void fragmentDataAndMetadata() {
//    ByteBuf rr =
//        RequestResponseFrameFlyweight.encode(
//            allocator, 1, true, DefaultPayload.create(data, metadata));
//
//    Publisher<ByteBuf> fragments =
//        FrameFragmenter.fragmentFrame(allocator, 1024, rr, FrameType.REQUEST_RESPONSE, false);
//
//    StepVerifier.create(Flux.from(fragments).doOnError(Throwable::printStackTrace))
//        .assertNext(
//            byteBuf -> {
//              Assert.assertEquals(
//                  FrameType.REQUEST_RESPONSE, FrameHeaderFlyweight.frameType(byteBuf));
//              Assert.assertTrue(FrameHeaderFlyweight.hasFollows(byteBuf));
//            })
//        .expectNextCount(6)
//        .assertNext(
//            byteBuf -> {
//              Assert.assertEquals(FrameType.NEXT, FrameHeaderFlyweight.frameType(byteBuf));
//              Assert.assertTrue(FrameHeaderFlyweight.hasFollows(byteBuf));
//            })
//        .assertNext(
//            byteBuf -> {
//              Assert.assertEquals(FrameType.NEXT, FrameHeaderFlyweight.frameType(byteBuf));
//              Assert.assertFalse(FrameHeaderFlyweight.hasFollows(byteBuf));
//            })
//        .verifyComplete();
//  }
// }
