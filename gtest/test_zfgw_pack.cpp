// ***************************************************************
//  test_zfgw_pack.cpp  — ZDS pack/unpack round-trip tests for zfgw.
//
//  These tests exercise the hand-written zfgw_pack.cpp against the
//  real libzce ZDS runtime.
//
//  Yongming Wang(wangym@gmail.com)
//  Copyright (C) 2026 - All Rights Reserved
// ***************************************************************
#include "zfgw_proto.h"
#include "zfgw_pack.h"
#include "zfgw.h"

#include <gtest/gtest.h>
#include <vector>

namespace {

using namespace zfgw;

template <typename T>
std::vector<zce_byte> pack_to_bytes(const T& t) {
    int need = zce::zdp::zds_pack(nullptr, 0, t, nullptr, true);
    EXPECT_GE(need, 0);
    std::vector<zce_byte> buf(need);
    if (need > 0) {
        int wrote = zce::zdp::zds_pack(buf.data(), need, t, nullptr, true);
        EXPECT_EQ(wrote, need);
    }
    return buf;
}

TEST(FgwPackTest, EndpointRoundTrip) {
    FgwEndpoint src;
    src.host = "example.com";
    src.port = 4242;

    auto bytes = pack_to_bytes(src);
    ASSERT_GT(bytes.size(), 0u);

    FgwEndpoint out;
    int consumed = zce::zdp::zds_unpack(out, bytes.data(), (int)bytes.size(), nullptr, true);
    ASSERT_GT(consumed, 0);
    EXPECT_EQ(out.host, src.host);
    EXPECT_EQ(out.port, src.port);
}

TEST(FgwPackTest, ChannelConfigRoundTrip) {
    FgwChannelConfig src;
    src.channel_id = 17;
    src.kind       = 1;  // UTP
    src.priority   = 250;
    src.remote.host = "10.0.0.1";
    src.remote.port = 6000;

    auto bytes = pack_to_bytes(src);
    ASSERT_GT(bytes.size(), 0u);

    FgwChannelConfig out;
    int consumed = zce::zdp::zds_unpack(out, bytes.data(), (int)bytes.size(), nullptr, true);
    ASSERT_GT(consumed, 0);
    EXPECT_TRUE(out == src);
}

TEST(FgwPackTest, FgwConfigWithChannels) {
    FgwConfig src;
    src.role               = 0;
    src.ingress_id         = 42;
    src.inport_listen_port = 1080;
    src.route_outport_id   = 7;
    src.config_version     = kFgwConfigVersion;
    src.segment_size       = 1400;
    src.recv_window        = 2048;
    src.heartbeat_interval = 3;
    src.multipath_mode     = 2;

    FgwChannelConfig c1;
    c1.channel_id = 1; c1.kind = 0;
    c1.remote.host = "a.example"; c1.remote.port = 5001;
    c1.priority = 100;

    FgwChannelConfig c2;
    c2.channel_id = 2; c2.kind = 1;
    c2.remote.host = "b.example"; c2.remote.port = 5002;
    c2.priority = 80;

    src.channels.push_back(c1);
    src.channels.push_back(c2);

    auto bytes = pack_to_bytes(src);
    ASSERT_GT(bytes.size(), 0u);

    FgwConfig out;
    int consumed = zce::zdp::zds_unpack(out, bytes.data(), (int)bytes.size(), nullptr, true);
    ASSERT_GT(consumed, 0);
    EXPECT_TRUE(out == src);
    ASSERT_EQ(out.channels.size(), src.channels.size());
    EXPECT_TRUE(out.channels[0] == c1);
    EXPECT_TRUE(out.channels[1] == c2);
}

TEST(FgwPackTest, StatusAggregates) {
    FgwStatus src;
    src.running       = 1;
    src.role          = 1;
    src.session_count = 2;

    FgwLinkQuality lq;
    lq.channel_id = 1;
    lq.connected  = 1;
    lq.rtt_ms     = 42;
    lq.loss_rate  = 0.01;
    lq.bytes_sent = 100000;
    lq.bytes_recv = 200000;
    src.links.push_back(lq);

    FgwSessionStat ss;
    ss.session_id     = 7;
    ss.ingress_id     = 99;
    ss.client_addr    = "1.2.3.4:5678";
    ss.target_addr    = "example:80";
    ss.bytes_sent     = 512;
    ss.bytes_recv     = 1024;
    ss.open_timestamp = 1717000000;
    src.sessions.push_back(ss);

    auto bytes = pack_to_bytes(src);
    ASSERT_GT(bytes.size(), 0u);

    FgwStatus out;
    int consumed = zce::zdp::zds_unpack(out, bytes.data(), (int)bytes.size(), nullptr, true);
    ASSERT_GT(consumed, 0);
    EXPECT_TRUE(out == src);
}

// The schema guard: only the current stamp is accepted. 0 means the field was
// absent — a pre-v0.3.0 file, whose bits 5+ denote different fields (several of
// the same type), so decoding it further would silently shift values.
TEST(FgwPackTest, ConfigVersionGuard) {
    EXPECT_TRUE(fgwConfigVersionOk(kFgwConfigVersion));
    EXPECT_FALSE(fgwConfigVersionOk(0));            // legacy / field absent
    EXPECT_FALSE(fgwConfigVersionOk(1));            // a legacy route_outport_id
    EXPECT_FALSE(fgwConfigVersionOk(0xF6C00002u));  // older schema revision

    // A default-constructed config is not accepted until a writer stamps it.
    FgwConfig fresh;
    EXPECT_FALSE(fgwConfigVersionOk(fresh.config_version));

    // The stamp survives a pack/unpack round-trip.
    FgwConfig src;
    src.role           = 1;
    src.config_version = kFgwConfigVersion;
    std::vector<zce_byte> buf(512);
    int wrote = zce::zdp::zds_pack(buf.data(), (int)buf.size(), src, nullptr, true);
    ASSERT_GT(wrote, 0);
    FgwConfig out;
    ASSERT_GT(zce::zdp::zds_unpack(out, buf.data(), wrote, nullptr, true), 0);
    EXPECT_TRUE(fgwConfigVersionOk(out.config_version));
}

}  // namespace

#ifndef USE_GTEST_MAIN
int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
#endif
