// ***************************************************************
//  zfgw_segment.cpp
//  Yongming Wang(wangym@gmail.com)
//  Copyright (C) 2026 - All Rights Reserved
// ***************************************************************
#include "zfgw_segment.h"
#include "zfgw.h"
#include <cstring>

namespace zfgw {

namespace {

constexpr zce_uint32 kCrc32Poly = 0xEDB88320u;

struct Crc32Table {
    zce_uint32 table[256];
    Crc32Table() {
        for (zce_uint32 i = 0; i < 256; ++i) {
            zce_uint32 c = i;
            for (int k = 0; k < 8; ++k) {
                c = (c & 1) ? (kCrc32Poly ^ (c >> 1)) : (c >> 1);
            }
            table[i] = c;
        }
    }
};

static const Crc32Table kCrc32Tbl;

inline void pack_be16(zce_byte* p, zce_uint16 v) {
    p[0] = (zce_byte)((v >> 8) & 0xFF);
    p[1] = (zce_byte)(v & 0xFF);
}
inline void pack_be32(zce_byte* p, zce_uint32 v) {
    p[0] = (zce_byte)((v >> 24) & 0xFF);
    p[1] = (zce_byte)((v >> 16) & 0xFF);
    p[2] = (zce_byte)((v >> 8) & 0xFF);
    p[3] = (zce_byte)(v & 0xFF);
}
inline zce_uint16 unpack_be16(const zce_byte* p) {
    return (zce_uint16)((zce_uint16)p[0] << 8 | p[1]);
}
inline zce_uint32 unpack_be32(const zce_byte* p) {
    return ((zce_uint32)p[0] << 24) | ((zce_uint32)p[1] << 16) |
           ((zce_uint32)p[2] << 8)  | (zce_uint32)p[3];
}

bool crc_ok(const zce_byte* in, int header_wire, int payload_len, zce_uint32 expect_crc) {
    const int total = header_wire + payload_len;
    const int crc_off = header_wire - 4;
    zce_uint32 crc = 0xFFFFFFFFu;
    for (int i = 0; i < crc_off; ++i) {
        crc = kCrc32Tbl.table[(crc ^ in[i]) & 0xFF] ^ (crc >> 8);
    }
    for (int i = 0; i < 4; ++i) {
        crc = kCrc32Tbl.table[(crc ^ 0) & 0xFF] ^ (crc >> 8);
    }
    for (int i = header_wire; i < total; ++i) {
        crc = kCrc32Tbl.table[(crc ^ in[i]) & 0xFF] ^ (crc >> 8);
    }
    crc ^= 0xFFFFFFFFu;
    return crc == expect_crc;
}

}  // namespace

zce_uint32 fgwCrc32(const zce_byte* data, int len, zce_uint32 seed) {
    zce_uint32 c = seed ^ 0xFFFFFFFFu;
    for (int i = 0; i < len; ++i) {
        c = kCrc32Tbl.table[(c ^ data[i]) & 0xFF] ^ (c >> 8);
    }
    return c ^ 0xFFFFFFFFu;
}

int fgwSegmentEncode(zce_byte* out, int out_size, const FgwSegmentHeader& hdr,
                     const zce_byte* payload, int payload_len) {
    if (!out) return ZFGW_ERRCODE_MEMORY;
    if (payload_len < 0 || payload_len > FgwSegmentHeader::MAX_PAYLOAD) {
        return ZFGW_ERRCODE_BADSEGMENT;
    }
    // Emit a Routed (24-byte) header when the caller flagged routing, otherwise
    // the Extended (20-byte) header. Both always set FLAG_HDR_EXT.
    const bool routed = (hdr.flags & FgwSegmentHeader::FLAG_HDR_ROUTE) != 0;
    const int hdr_sz = routed ? FgwSegmentHeader::ROUTED_HEADER_SIZE
                              : FgwSegmentHeader::HEADER_SIZE;
    const int total  = hdr_sz + payload_len;
    if (out_size < total) return ZFGW_ERRCODE_MEMORY;

    zce_uint8 flags = (zce_uint8)(hdr.flags | FgwSegmentHeader::FLAG_HDR_EXT);
    out[0] = FgwSegmentHeader::MAGIC;
    out[1] = flags;
    pack_be16(out + 2, (zce_uint16)payload_len);
    pack_be32(out + 4, hdr.session_id);
    pack_be32(out + 8, hdr.seq_num);
    pack_be32(out + 12, hdr.ingress_id);

    int crc_off;
    if (routed) {
        pack_be32(out + 16, hdr.outport_id);
        pack_be32(out + 20, 0);  // CRC placeholder
        crc_off = 20;
    } else {
        pack_be32(out + 16, 0);  // CRC placeholder
        crc_off = 16;
    }

    if (payload_len > 0 && payload) {
        std::memcpy(out + hdr_sz, payload, payload_len);
    }

    const zce_uint32 crc = fgwCrc32(out, total);
    pack_be32(out + crc_off, crc);
    return total;
}

int fgwSegmentPeekHeader(const zce_byte* in, int in_size, FgwSegmentHeader& hdr) {
    if (in_size < FgwSegmentHeader::LEGACY_HEADER_SIZE) return 0;
    if (in[0] != FgwSegmentHeader::MAGIC) return ZFGW_ERRCODE_BADSEGMENT;

    hdr.magic       = in[0];
    hdr.flags       = in[1];
    hdr.payload_len = unpack_be16(in + 2);
    hdr.session_id  = unpack_be32(in + 4);
    hdr.seq_num     = unpack_be32(in + 8);

    if (hdr.payload_len > FgwSegmentHeader::MAX_PAYLOAD) {
        return ZFGW_ERRCODE_BADSEGMENT;
    }

    if (hdr.isHdrRoute()) {
        if (in_size < FgwSegmentHeader::ROUTED_HEADER_SIZE) return 0;
        hdr.ingress_id = unpack_be32(in + 12);
        hdr.outport_id = unpack_be32(in + 16);
        hdr.crc32      = unpack_be32(in + 20);
        return FgwSegmentHeader::ROUTED_HEADER_SIZE;
    }

    if (hdr.isHdrExt()) {
        if (in_size < FgwSegmentHeader::HEADER_SIZE) return 0;
        hdr.ingress_id = unpack_be32(in + 12);
        hdr.outport_id = 0;
        hdr.crc32      = unpack_be32(in + 16);
        return FgwSegmentHeader::HEADER_SIZE;
    }

    hdr.ingress_id = 0;
    hdr.outport_id = 0;
    hdr.crc32        = unpack_be32(in + 12);
    return FgwSegmentHeader::LEGACY_HEADER_SIZE;
}

bool fgwSegmentVerifyCrc(const zce_byte* in, const FgwSegmentHeader& hdr, int header_wire_bytes) {
    if (header_wire_bytes != FgwSegmentHeader::LEGACY_HEADER_SIZE &&
        header_wire_bytes != FgwSegmentHeader::HEADER_SIZE &&
        header_wire_bytes != FgwSegmentHeader::ROUTED_HEADER_SIZE) {
        return false;
    }
    return crc_ok(in, header_wire_bytes, (int)hdr.payload_len, hdr.crc32);
}

}  // namespace zfgw
