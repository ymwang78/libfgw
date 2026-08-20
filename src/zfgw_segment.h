// ***************************************************************
//  zfgw_segment.h  — on-the-wire packet framing for the libfgw
//                    multipath data channel.
//  Yongming Wang(wangym@gmail.com)
//  Copyright (C) 2026 - All Rights Reserved
// ***************************************************************
#pragma once

#include <zce/zce_types.h>
#include <zce/zce_dblock.h>
#include <cstdint>

namespace zfgw {

/// Fixed framing header for every multipath segment.
///
/// **Legacy (16 bytes)** — peer does not set FLAG_HDR_EXT in `flags`:
///   Offset  Size  Field
///     0      1    Magic     (0xF6)
///     1      1    Flags
///     2      2    PayloadLen (big-endian)
///     4      4    SessionId
///     8      4    SeqNum
///    12      4    Crc32
///   ingress_id is treated as 0.
///
/// **Extended (20 bytes)** — `flags` includes FLAG_HDR_EXT:
///    12      4    IngressId  (big-endian, disambiguates multiple Inport VMs)
///    16      4    Crc32
///
struct FgwSegmentHeader {
    static constexpr zce_uint8 MAGIC = 0xF6;
    static constexpr zce_uint16 MAX_PAYLOAD = 65519;
    /// Current wire header size when FLAG_HDR_EXT is set (preferred).
    static constexpr int HEADER_SIZE = 20;
    /// Legacy header size (no ingress field on wire).
    static constexpr int LEGACY_HEADER_SIZE = 16;

    enum Flag : zce_uint8 {
        FLAG_SYN       = 0x01,
        FLAG_FIN       = 0x02,
        FLAG_DATA      = 0x04,
        FLAG_ACK       = 0x08,
        FLAG_HEARTBEAT = 0x10,
        /// Per-link handshake: payload carries ZDS(FgwHello). Handled at the
        /// channel/identity level, never as a session segment.
        FLAG_HELLO     = 0x20,
        /// If set, header is 20 bytes and bytes 12–15 carry ingress_id.
        FLAG_HDR_EXT   = 0x80,
    };

    zce_uint8  magic       = MAGIC;
    zce_uint8  flags       = 0;
    zce_uint16 payload_len = 0;
    zce_uint32 session_id  = 0;
    zce_uint32 seq_num     = 0;
    zce_uint32 ingress_id  = 0;
    zce_uint32 crc32       = 0;

    bool isData() const      { return (flags & FLAG_DATA) != 0; }
    bool isHello() const     { return (flags & FLAG_HELLO) != 0; }
    bool isHeartbeat() const { return (flags & FLAG_HEARTBEAT) != 0; }
    bool isFin() const       { return (flags & FLAG_FIN) != 0; }
    bool isSyn() const       { return (flags & FLAG_SYN) != 0; }
    bool isAck() const       { return (flags & FLAG_ACK) != 0; }
    bool isHdrExt() const    { return (flags & FLAG_HDR_EXT) != 0; }
};

/// Encode a segment (header + payload). Always emits **20-byte** extended header
/// (FLAG_HDR_EXT set) so multi-Inport ingress_id is carried on the wire.
int fgwSegmentEncode(zce_byte* out, int out_size, const FgwSegmentHeader& hdr,
                     const zce_byte* payload, int payload_len);

/// Decode header from stream. Returns bytes consumed (16 legacy or 20 extended),
/// 0 if more data needed, or negative ZFGW_ERRCODE.
int fgwSegmentPeekHeader(const zce_byte* in, int in_size, FgwSegmentHeader& hdr);

/// Verify CRC. `header_wire_bytes` must be 16 or 20 matching peek result.
bool fgwSegmentVerifyCrc(const zce_byte* in, const FgwSegmentHeader& hdr, int header_wire_bytes);

zce_uint32 fgwCrc32(const zce_byte* data, int len, zce_uint32 seed = 0);

}  // namespace zfgw
