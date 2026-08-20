// ***************************************************************
//  test_zfgw_datastream.cpp
//  Regression tests for DataStream receive-path dedup / delivery.
//  Yongming Wang(wangym@gmail.com)
//  Copyright (C) 2026 - All Rights Reserved
// ***************************************************************
#include <gtest/gtest.h>

#include "zfgw_datastream.h"
#include "zfgw_channel_manager.h"
#include "zfgw_segment.h"
#include "zfgw.h"

#include <zce/zce_reactor.h>

#include <string>
#include <vector>

namespace {

using namespace zfgw;

// Minimal IFgwChannel that never touches a transport — enough to drive
// DataStream::onChannelBytes(). Used as a raw pointer only (no SmartPtr), so
// its zce::Object refcount is never decremented to zero.
class MockChannel : public IFgwChannel {
  public:
    explicit MockChannel(zce_uint32 id, zce_uint32 prio = 100)
        : IFgwChannel(id, ZFGW_CHANNEL_TCP, prio) {}
    int  connect(const FgwEndpoint&) override { return 0; }
    void close() override {}
    int  sendBytes(const zce_byte*, zce_uint32 len) override { return (int)len; }
    /// Mark connected (and thus live) for selection tests.
    void setLive() { markConnected(true); }
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

// A handler that re-enters closeSession() from onSessionClose — exactly what
// the Outport's FgwRelaySession does — which used to erase the map entry a
// second time inside onChannelBytes() and crash.
class ReentrantCloseHandler : public ISessionHandler {
  public:
    DataStream* ds = nullptr;
    zce_uint32  ingress = 0;
    int         closes = 0;
    void onSessionData(DataStream*, zce_uint32, const zce_byte*, zce_uint32) override {}
    void onSessionClose(DataStream* s, zce_uint32 sid) override {
        ++closes;
        if (ds) ds->closeSession(sid, ingress);  // re-enter; erases the session
    }
};

// Receiving a FIN must not double-erase the session when the handler's
// onSessionClose re-enters closeSession(). Needs a real (empty) ChannelManager
// so closeSession()'s FIN send resolves to "no live channel" instead of a null
// manager deref.
TEST(FgwDataStreamTest, FinCloseReentrancyNoCrash) {
    zce::SmartPtr<zce::Reactor> reactor(zce::ReactorSigt::instance());
    zce::SmartPtr<ChannelManager> mgr(new ChannelManager(reactor, nullptr));
    DataStream ds(mgr, /*ingress*/ 0, 1200, 1024, /*verify_crc*/ true);

    MockChannel ch(1);
    ReentrantCloseHandler handler;
    handler.ds = &ds;
    handler.ingress = 0;
    ds.setUnknownSessionCallback(
        [&](zce_uint32, zce_uint32) -> ISessionHandler* { return &handler; });

    const zce_uint32 kSession = 5;
    auto syn  = makeSegment(FgwSegmentHeader::FLAG_SYN, 0, kSession, 0, "");
    auto data = makeSegment(FgwSegmentHeader::FLAG_DATA, 0, kSession, 0, "x");
    auto fin  = makeSegment(FgwSegmentHeader::FLAG_FIN, 0, kSession, 1, "");

    ds.onChannelBytes(&ch, syn.data(), (zce_uint32)syn.size());
    ds.onChannelBytes(&ch, data.data(), (zce_uint32)data.size());
    ds.onChannelBytes(&ch, fin.data(), (zce_uint32)fin.size());   // must not crash
    EXPECT_EQ(handler.closes, 1);

    // A second FIN for a now-gone session must also be a no-op, not a crash.
    ds.onChannelBytes(&ch, fin.data(), (zce_uint32)fin.size());
    SUCCEED();
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

// LinkSelector explore/exploit: primary (exploit) + one rotating probe (explore).
TEST(FgwLinkSelectorTest, ExploreExploitDualSend) {
    LinkSelector sel;  // default WEIGHTED = explore/exploit

    zce::SmartPtr<IFgwChannel> a(new MockChannel(1, 100));
    zce::SmartPtr<IFgwChannel> b(new MockChannel(2, 80));
    zce::SmartPtr<IFgwChannel> c(new MockChannel(3, 60));
    static_cast<MockChannel*>(a.get())->setLive();
    static_cast<MockChannel*>(b.get())->setLive();
    static_cast<MockChannel*>(c.get())->setLive();
    std::vector<FgwChannelPtr> pool{a, b, c};

    // Default primary = best weight (ch 1); exactly two copies (primary + probe).
    auto s1 = sel.select(pool);
    ASSERT_EQ(s1.size(), 2u);
    EXPECT_EQ(s1[0], 1u);
    // Probe rotates across the non-primary links on successive calls.
    auto s2 = sel.select(pool);
    EXPECT_EQ(s2[0], 1u);
    EXPECT_NE(s1[1], s2[1]);
    EXPECT_NE(s1[1], 1u);
    EXPECT_NE(s2[1], 1u);

    // A fed-back recommendation overrides the primary.
    sel.setPrimaryChannel(3);
    auto s3 = sel.select(pool);
    ASSERT_EQ(s3.size(), 2u);
    EXPECT_EQ(s3[0], 3u);   // primary is now the fed-back link
    EXPECT_NE(s3[1], 3u);   // probe is one of the others

    // A single live link yields a single copy (no probe).
    std::vector<FgwChannelPtr> one{a};
    auto s4 = sel.select(one);
    ASSERT_EQ(s4.size(), 1u);
    EXPECT_EQ(s4[0], 1u);
}

// Heartbeat stall decision (pure logic). The receive clock is baselined at
// connect time, so there is no special "never received" case: a link is stalled
// purely by how long ago the last byte arrived.
TEST(FgwHeartbeatTest, StallDecision) {
    // Received recently, within timeout -> live.
    EXPECT_FALSE(fgwLinkStalled(1400, 1000, 500));
    // Exactly at the timeout -> not yet stalled (strictly greater).
    EXPECT_FALSE(fgwLinkStalled(1500, 1000, 500));
    // Past the timeout -> stalled.
    EXPECT_TRUE(fgwLinkStalled(1600, 1000, 500));
    // Tick wraparound: now has wrapped past 0, last is just before the wrap;
    // the unsigned delta (116) is still compared correctly.
    EXPECT_TRUE(fgwLinkStalled(/*now*/ 100, /*last*/ 0xFFFFFFF0u, /*timeout*/ 50));
    EXPECT_FALSE(fgwLinkStalled(/*now*/ 100, /*last*/ 0xFFFFFFF0u, /*timeout*/ 200));
}

#ifndef USE_GTEST_MAIN
int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
#endif

}  // namespace
