// ***************************************************************
//  test_zfgw_segment.cpp
//  Unit tests for libfgw's segment framing (zfgw_segment.cpp).
//
//  Only the pure-C++ segment layer is exercised here; no zce
//  runtime is required.
//
//  Yongming Wang(wangym@gmail.com)
//  Copyright (C) 2026 - All Rights Reserved
// ***************************************************************
#include "zfgw_segment.h"

#include <gtest/gtest.h>
#include <cstring>
#include <vector>

namespace {

using namespace zfgw;

TEST(FgwSegmentTest, EncodeDecodeRoundTrip) {
    const zce_byte payload[] = {0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08};

    FgwSegmentHeader hdr;
    hdr.payload_len = sizeof(payload);
    hdr.session_id  = 0x12345678;
    hdr.seq_num     = 42;
    hdr.ingress_id  = 0xA01FE004;
    hdr.flags       = FgwSegmentHeader::FLAG_DATA;

    std::vector<zce_byte> buf(64);
    int wrote = fgwSegmentEncode(buf.data(), (int)buf.size(), hdr, payload, sizeof(payload));
    ASSERT_GT(wrote, (int)sizeof(payload));

    FgwSegmentHeader decoded{};
    int peeked = fgwSegmentPeekHeader(buf.data(), wrote, decoded);
    ASSERT_GT(peeked, 0);
    EXPECT_EQ(peeked, FgwSegmentHeader::HEADER_SIZE);
    EXPECT_TRUE(decoded.isHdrExt());

    EXPECT_EQ(decoded.payload_len, sizeof(payload));
    EXPECT_EQ(decoded.session_id,  0x12345678u);
    EXPECT_EQ(decoded.seq_num,     42u);
    EXPECT_EQ(decoded.ingress_id,  0xA01FE004u);
    EXPECT_TRUE(decoded.isData());

    EXPECT_TRUE(fgwSegmentVerifyCrc(buf.data(), decoded, peeked));

    // Tamper one byte in the payload → CRC check must fail.
    buf[peeked + 3] ^= 0x55;
    EXPECT_FALSE(fgwSegmentVerifyCrc(buf.data(), decoded, peeked));
}

TEST(FgwSegmentTest, ShortBufferRejected) {
    // Peek on a too-small buffer returns 0 (needs more input).
    zce_byte small_buf[4] = {};
    FgwSegmentHeader decoded{};
    int ret = fgwSegmentPeekHeader(small_buf, sizeof(small_buf), decoded);
    EXPECT_EQ(ret, 0);

    // A 16-byte buffer whose first byte is not MAGIC must be rejected.
    zce_byte bad_magic[16] = {};
    ret = fgwSegmentPeekHeader(bad_magic, sizeof(bad_magic), decoded);
    EXPECT_LT(ret, 0);
}

TEST(FgwSegmentTest, Crc32Deterministic) {
    const zce_byte msg[] = {'1', '2', '3', '4', '5', '6', '7', '8', '9'};
    zce_uint32 c = fgwCrc32(msg, sizeof(msg), 0);
    // Recompute — the table is static so two calls must agree exactly.
    EXPECT_EQ(c, fgwCrc32(msg, sizeof(msg), 0));
    // Ensure a different seed yields a different value (sanity only).
    EXPECT_NE(c, fgwCrc32(msg, sizeof(msg), 1));
}

TEST(FgwSegmentTest, SynFlagIdentified) {
    FgwSegmentHeader hdr;
    hdr.payload_len = 0;
    hdr.session_id  = 7;
    hdr.seq_num     = 0;
    hdr.flags       = FgwSegmentHeader::FLAG_SYN;

    std::vector<zce_byte> buf(32);
    int wrote = fgwSegmentEncode(buf.data(), (int)buf.size(), hdr, nullptr, 0);
    ASSERT_GT(wrote, 0);

    FgwSegmentHeader decoded{};
    int peeked = fgwSegmentPeekHeader(buf.data(), wrote, decoded);
    ASSERT_GT(peeked, 0);
    EXPECT_TRUE(decoded.isSyn());
    EXPECT_FALSE(decoded.isData());
    EXPECT_TRUE(fgwSegmentVerifyCrc(buf.data(), decoded, peeked));
}

}  // namespace

#ifndef USE_GTEST_MAIN
int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
#endif
