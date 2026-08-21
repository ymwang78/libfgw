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
#include "zfgw_pack.h"
#include "zfgw.h"

#include <zce/zce_reactor.h>

#include <atomic>
#include <string>
#include <thread>
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

// Encode a FLAG_ACK control segment carrying an FgwLinkFeedback.
std::vector<zce_byte> makeFeedback(zce_uint32 epoch, zce_uint32 primary) {
    FgwLinkFeedback fb;
    fb.decision_epoch      = epoch;
    fb.recommended_primary = primary;
    fb.alive_bitmap        = 0;
    fb.highest_seq         = 0;
    zce_byte payload[64];
    int plen = zce::zdp::zds_pack(payload, (int)sizeof(payload), fb, nullptr, true);
    EXPECT_GT(plen, 0);

    FgwSegmentHeader hdr;
    hdr.flags       = FgwSegmentHeader::FLAG_ACK;
    hdr.payload_len = (zce_uint16)plen;
    std::vector<zce_byte> out(FgwSegmentHeader::HEADER_SIZE + plen + 8);
    int wrote = fgwSegmentEncode(out.data(), (int)out.size(), hdr, payload, plen);
    EXPECT_GT(wrote, 0);
    out.resize(wrote > 0 ? (size_t)wrote : 0);
    return out;
}

// Encode a FLAG_HELLO segment carrying an FgwHello with the given generation.
std::vector<zce_byte> makeHello(zce_uint32 generation, zce_uint32 channel_id) {
    FgwHello h;
    h.proto_version = kFgwProtoVersion;
    h.role          = 0;
    h.ingress_id    = 0;
    h.outport_id    = 0;
    h.channel_id    = channel_id;
    h.generation    = generation;
    zce_byte payload[64];
    int plen = zce::zdp::zds_pack(payload, (int)sizeof(payload), h, nullptr, true);
    EXPECT_GT(plen, 0);
    FgwSegmentHeader hdr;
    hdr.flags       = FgwSegmentHeader::FLAG_HELLO;
    hdr.payload_len = (zce_uint16)plen;
    std::vector<zce_byte> out(FgwSegmentHeader::HEADER_SIZE + plen + 8);
    int wrote = fgwSegmentEncode(out.data(), (int)out.size(), hdr, payload, plen);
    EXPECT_GT(wrote, 0);
    out.resize(wrote > 0 ? (size_t)wrote : 0);
    return out;
}

// Feed one arrival race for a given seq: the winner channel delivers first (a
// new segment), the loser delivers the duplicate.
void race(DataStream& ds, MockChannel& winner, MockChannel& loser, zce_uint32 seq) {
    auto seg = makeSegment(FgwSegmentHeader::FLAG_DATA, 0, /*session*/ 1, seq, "x");
    ds.onChannelBytes(&winner, seg.data(), (zce_uint32)seg.size());
    ds.onChannelBytes(&loser, seg.data(), (zce_uint32)seg.size());
}

// The arbiter must compare links by WIN-RATE, not raw win count: the incumbent
// primary races on every segment and accrues many wins against weak links, but a
// consistently-faster probe that races less often must still be recommended.
TEST(FgwDataStreamTest, RacingUsesWinRateNotCount) {
    zce::SmartPtr<ChannelManager> no_manager;
    DataStream ds(no_manager, /*ingress*/ 0, 1200, 1024, /*verify_crc*/ true);
    CaptureHandler handler;
    ds.setUnknownSessionCallback([&](zce_uint32, zce_uint32) -> ISessionHandler* { return &handler; });

    MockChannel a(0x1001), b(0x1002), c(0x1003), d(0x1004);
    a.setPeerIdentity(/*peer_channel*/ 1, 0, 0);  // incumbent primary
    b.setPeerIdentity(2, 0, 0);                    // consistently fastest probe
    c.setPeerIdentity(3, 0, 0);
    d.setPeerIdentity(4, 0, 0);

    zce_uint32 seq = 0;
    for (int round = 0; round < 5; ++round) {
        race(ds, b, a, seq++);   // B beats the primary A
        race(ds, a, c, seq++);   // A beats C
        race(ds, a, d, seq++);   // A beats D
    }
    // A: wins 10 / races 15 = 0.67; B: wins 5 / races 5 = 1.0. Raw-count would
    // pick A (10 > 5) and never converge; win-rate picks B.
    EXPECT_EQ(ds.recommendedPrimary(), 2u);
}

// A peer restart (new Hello generation) resets the epoch comparison so the
// restarted peer's low epochs are accepted instead of discarded forever.
TEST(FgwDataStreamTest, PeerRestartResetsEpoch) {
    zce::SmartPtr<zce::Reactor> reactor(zce::ReactorSigt::instance());
    zce::SmartPtr<ChannelManager> mgr(new ChannelManager(reactor, nullptr));
    DataStream ds(mgr, /*ingress*/ 0, 1200, 1024, /*verify_crc*/ true);
    MockChannel ch(1);

    auto h1 = makeHello(/*gen*/ 100, /*channel_id*/ 5);
    ds.onChannelBytes(&ch, h1.data(), (zce_uint32)h1.size());
    auto f10 = makeFeedback(/*epoch*/ 10, /*primary*/ 42);
    ds.onChannelBytes(&ch, f10.data(), (zce_uint32)f10.size());
    EXPECT_EQ(mgr->selector().primaryChannel(), 42u);

    // Peer restarts: new generation, epoch sequence starts over at 1.
    auto h2 = makeHello(200, 6);
    ds.onChannelBytes(&ch, h2.data(), (zce_uint32)h2.size());
    auto f1 = makeFeedback(1, 7);
    ds.onChannelBytes(&ch, f1.data(), (zce_uint32)f1.size());
    EXPECT_EQ(mgr->selector().primaryChannel(), 7u);  // low epoch applied after restart
}

// Inbound racing feedback sets the LinkSelector primary, and a stale (older or
// equal) epoch is ignored.
TEST(FgwDataStreamTest, AppliesRacingFeedbackByEpoch) {
    zce::SmartPtr<zce::Reactor> reactor(zce::ReactorSigt::instance());
    zce::SmartPtr<ChannelManager> mgr(new ChannelManager(reactor, nullptr));
    DataStream ds(mgr, /*ingress*/ 0, 1200, 1024, /*verify_crc*/ true);
    MockChannel ch(1);

    auto f5 = makeFeedback(/*epoch*/ 5, /*primary*/ 42);
    ds.onChannelBytes(&ch, f5.data(), (zce_uint32)f5.size());
    EXPECT_EQ(mgr->selector().primaryChannel(), 42u);

    // Older epoch must be ignored.
    auto f3 = makeFeedback(3, 99);
    ds.onChannelBytes(&ch, f3.data(), (zce_uint32)f3.size());
    EXPECT_EQ(mgr->selector().primaryChannel(), 42u);

    // Newer epoch applies.
    auto f6 = makeFeedback(6, 7);
    ds.onChannelBytes(&ch, f6.data(), (zce_uint32)f6.size());
    EXPECT_EQ(mgr->selector().primaryChannel(), 7u);
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

// rx_buffer must stay bounded by recv_window: out-of-window segments (a stalled
// gap plus a long reordered/malicious run) are dropped instead of buffered.
TEST(FgwDataStreamTest, RecvWindowBounded) {
    zce::SmartPtr<ChannelManager> no_manager;
    DataStream ds(no_manager, /*ingress*/ 0, 1200, /*recv_window*/ 4, /*verify_crc*/ true);
    CaptureHandler handler;
    ds.setUnknownSessionCallback(
        [&](zce_uint32, zce_uint32) -> ISessionHandler* { return &handler; });
    MockChannel ch(1);

    auto syn = makeSegment(FgwSegmentHeader::FLAG_SYN, 0, /*session*/ 1, 0, "");
    ds.onChannelBytes(&ch, syn.data(), (zce_uint32)syn.size());

    // seq 0 is missing; feed DATA 1..10. Window [0,4) buffers only 1,2,3; the
    // rest are dropped, so rx_buffer never exceeds the window.
    for (zce_uint32 seq = 1; seq <= 10; ++seq) {
        auto d = makeSegment(FgwSegmentHeader::FLAG_DATA, 0, 1, seq, "x");
        ds.onChannelBytes(&ch, d.data(), (zce_uint32)d.size());
    }
    EXPECT_EQ(handler.bytes, "");  // nothing deliverable yet (seq 0 missing)

    // seq 0 arrives -> deliver 0,1,2,3, then stall at 4 (dropped by the cap).
    auto d0 = makeSegment(FgwSegmentHeader::FLAG_DATA, 0, 1, 0, "0");
    ds.onChannelBytes(&ch, d0.data(), (zce_uint32)d0.size());
    EXPECT_EQ(handler.bytes, "0xxx");
}

// A segment rejected by the receive window must NOT be recorded in the dedup
// table: once the window advances to cover that seq, a delayed multipath copy
// has to be accepted. Recording it early would reject the copy as a duplicate
// and stall the session forever — defeating the redundancy of dual-send.
TEST(FgwDataStreamTest, OutOfWindowDropIsNotDeduped) {
    zce::SmartPtr<ChannelManager> no_manager;
    DataStream ds(no_manager, /*ingress*/ 0, 1200, /*recv_window*/ 4, /*verify_crc*/ true);
    CaptureHandler handler;
    ds.setUnknownSessionCallback(
        [&](zce_uint32, zce_uint32) -> ISessionHandler* { return &handler; });
    MockChannel ch(1);

    auto syn = makeSegment(FgwSegmentHeader::FLAG_SYN, 0, /*session*/ 1, 0, "");
    ds.onChannelBytes(&ch, syn.data(), (zce_uint32)syn.size());

    // Early copy of seq 4 while the window is [0,4) -> dropped as out-of-window.
    auto d4 = makeSegment(FgwSegmentHeader::FLAG_DATA, 0, 1, 4, "E");
    ds.onChannelBytes(&ch, d4.data(), (zce_uint32)d4.size());
    EXPECT_EQ(handler.bytes, "");

    // Fill 0..3; the window advances to [4,8).
    for (zce_uint32 seq = 0; seq <= 3; ++seq) {
        auto d = makeSegment(FgwSegmentHeader::FLAG_DATA, 0, 1, seq, "a");
        ds.onChannelBytes(&ch, d.data(), (zce_uint32)d.size());
    }
    EXPECT_EQ(handler.bytes, "aaaa");

    // A delayed multipath copy of seq 4 now arrives — it must be delivered, not
    // rejected as a duplicate of the earlier out-of-window drop.
    ds.onChannelBytes(&ch, d4.data(), (zce_uint32)d4.size());
    EXPECT_EQ(handler.bytes, "aaaaE");
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

// Concurrent select() on a selector shared by multiple DataStreams must be a
// data-race-free (the probe rotation counter is atomic). Every result must stay
// well-formed and both alternate links must still get probed.
TEST(FgwLinkSelectorTest, ConcurrentSelectIsThreadSafe) {
    LinkSelector sel;
    zce::SmartPtr<IFgwChannel> a(new MockChannel(1, 100));
    zce::SmartPtr<IFgwChannel> b(new MockChannel(2, 80));
    zce::SmartPtr<IFgwChannel> c(new MockChannel(3, 60));
    static_cast<MockChannel*>(a.get())->setLive();
    static_cast<MockChannel*>(b.get())->setLive();
    static_cast<MockChannel*>(c.get())->setLive();
    std::vector<FgwChannelPtr> pool{a, b, c};

    std::atomic<int> saw2{0}, saw3{0}, bad{0};
    auto worker = [&]() {
        for (int i = 0; i < 5000; ++i) {
            auto s = sel.select(pool);
            if (s.size() != 2 || s[0] != 1u) { ++bad; continue; }
            if (s[1] == 2u) ++saw2;
            else if (s[1] == 3u) ++saw3;
            else ++bad;
        }
    };
    std::vector<std::thread> ts;
    for (int t = 0; t < 4; ++t) ts.emplace_back(worker);
    for (auto& t : ts) t.join();

    EXPECT_EQ(bad.load(), 0);
    EXPECT_GT(saw2.load(), 0);
    EXPECT_GT(saw3.load(), 0);
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
