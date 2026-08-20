// ***************************************************************
//  test_zfgw_datastream.cpp
//  Regression tests for DataStream receive-path dedup / delivery.
//  Yongming Wang(wangym@gmail.com)
//  Copyright (C) 2026 - All Rights Reserved
// ***************************************************************
#include <gtest/gtest.h>

#include "zfgw_datastream.h"
#include "zfgw_segment.h"
#include "zfgw.h"

#include <string>
#include <vector>

namespace {

using namespace zfgw;

// Minimal IFgwChannel that never touches a transport — enough to drive
// DataStream::onChannelBytes(). Used as a raw pointer only (no SmartPtr), so
// its zce::Object refcount is never decremented to zero.
class MockChannel : public IFgwChannel {
  public:
    explicit MockChannel(zce_uint32 id) : IFgwChannel(id, ZFGW_CHANNEL_TCP, 100) {}
    int  connect(const FgwEndpoint&) override { return 0; }
    void close() override {}
    int  sendBytes(const zce_byte*, zce_uint32 len) override { return (int)len; }
};

// Captures everything DataStream delivers for a session.
class CaptureHandler : public ISessionHandler {
  public:
    int         opens = 0;
    int         closes = 0;
    std::string bytes;
    void onSessionOpen(DataStream*, zce_uint32) override { ++opens; }
    void onSessionData(DataStream*, zce_uint32, const zce_byte* buf, zce_uint32 len) override {
        bytes.append(reinterpret_cast<const char*>(buf), len);
    }
    void onSessionClose(DataStream*, zce_uint32) override { ++closes; }
};

// Encode one segment (SYN/DATA/FIN) with the given ingress/session/seq/payload.
std::vector<zce_byte> makeSegment(zce_uint8 flags, zce_uint32 ingress, zce_uint32 session,
                                  zce_uint32 seq, const std::string& payload) {
    FgwSegmentHeader hdr;
    hdr.flags       = flags;
    hdr.payload_len = (zce_uint16)payload.size();
    hdr.session_id  = session;
    hdr.seq_num     = seq;
    hdr.ingress_id  = ingress;

    std::vector<zce_byte> out(FgwSegmentHeader::HEADER_SIZE + payload.size() + 8);
    int wrote = fgwSegmentEncode(out.data(), (int)out.size(), hdr,
                                 payload.empty() ? nullptr
                                                 : reinterpret_cast<const zce_byte*>(payload.data()),
                                 (int)payload.size());
    EXPECT_GT(wrote, 0);
    out.resize(wrote > 0 ? (size_t)wrote : 0);
    return out;
}

DataStream makeStream() {
    // A null ChannelManager is fine: the receive path never dereferences it, and
    // DataStream guards the (un)subscribe calls on a null manager.
    zce::SmartPtr<ChannelManager> no_manager;
    return DataStream(no_manager, /*local_ingress*/ 0, /*segment*/ 1200, /*window*/ 1024,
                      /*verify_crc*/ true);
}

// The regression: SYN and the first DATA segment both legitimately carry seq 0.
// Before the DedupKey control/data discriminator, the DATA was dropped as a
// duplicate of the SYN and never delivered.
TEST(FgwDataStreamTest, FirstDataNotDroppedBySynDedup) {
    DataStream ds = makeStream();
    MockChannel ch(1);
    CaptureHandler handler;
    ds.setUnknownSessionCallback(
        [&](zce_uint32, zce_uint32) -> ISessionHandler* { return &handler; });

    const zce_uint32 kSession = 7;
    auto syn  = makeSegment(FgwSegmentHeader::FLAG_SYN, 0, kSession, 0, "");
    auto data = makeSegment(FgwSegmentHeader::FLAG_DATA, 0, kSession, 0, "hello");

    ds.onChannelBytes(&ch, syn.data(), (zce_uint32)syn.size());
    ds.onChannelBytes(&ch, data.data(), (zce_uint32)data.size());

    EXPECT_EQ(handler.opens, 1);
    EXPECT_EQ(handler.bytes, "hello");
}

// True duplicates (same class + seq, e.g. a multipath copy) must still be
// deduplicated: the payload is delivered exactly once.
TEST(FgwDataStreamTest, DuplicateDataDeliveredOnce) {
    DataStream ds = makeStream();
    MockChannel ch(1);
    CaptureHandler handler;
    ds.setUnknownSessionCallback(
        [&](zce_uint32, zce_uint32) -> ISessionHandler* { return &handler; });

    const zce_uint32 kSession = 9;
    auto syn   = makeSegment(FgwSegmentHeader::FLAG_SYN, 0, kSession, 0, "");
    auto data0 = makeSegment(FgwSegmentHeader::FLAG_DATA, 0, kSession, 0, "AB");
    auto data0dup = data0;  // identical copy
    auto data1 = makeSegment(FgwSegmentHeader::FLAG_DATA, 0, kSession, 1, "CD");

    ds.onChannelBytes(&ch, syn.data(), (zce_uint32)syn.size());
    ds.onChannelBytes(&ch, data0.data(), (zce_uint32)data0.size());
    ds.onChannelBytes(&ch, data0dup.data(), (zce_uint32)data0dup.size());
    ds.onChannelBytes(&ch, data1.data(), (zce_uint32)data1.size());

    EXPECT_EQ(handler.bytes, "ABCD");
}

#ifndef USE_GTEST_MAIN
int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
#endif

}  // namespace
