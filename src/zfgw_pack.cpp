// ***************************************************************
//  zfgw_pack
//  ---------------------------------------------------------------
//  Hand-maintained ZDS serialisation for zfgw_proto.h.
//  Follows the same bitmask/struct-prefix convention emitted by zGen.
//
//  Yongming Wang(wangym@gmail.com)
//  Copyright (C) 2026 - All Rights Reserved
// *****************************************************************

#include "zfgw_proto.h"
#include "zfgw_pack.h"
#include <zce/zds_schema.h>

using namespace zce;
using namespace zdp;

// ── FgwEndpoint ──────────────────────────────────────────────────────────────
zfgw::FgwEndpoint zfgw::FgwEndpoint::_empty;

int zce::zdp::zds_pack(zce_byte* buf, int size, const zfgw::FgwEndpoint& _t,
                       zds_context_t* ctx, bool has_prefix) {
    int len = 0, ret = 0;
    zce_uint64 _sp = 0;
    if (!zdp::is_empty_member(_t.host)) _sp |= 1ull << 0;
    if (!zdp::is_empty_member(_t.port)) _sp |= 1ull << 1;

    len = zds_pack_struct_header(buf, size, _sp, ctx, has_prefix);
    CHECKLEN_MOVEBUF_ADDRET_DECSIZE;

    if (_sp & (1ull << 0)) { len = zds_pack_builtin(buf, size, _t.host, ctx); CHECKLEN_MOVEBUF_ADDRET_DECSIZE; }
    if (_sp & (1ull << 1)) { len = zds_pack_builtin(buf, size, _t.port, ctx); CHECKLEN_MOVEBUF_ADDRET_DECSIZE; }
    return ret;
}

int zce::zdp::zds_unpack(zfgw::FgwEndpoint& _t, const zce_byte* buf, int size,
                         zds_context_t* ctx, bool has_prefix) {
    int len = 0, ret = 0;
    zce_uint64 _sp = 0;
    len = zds_unpack_struct_header(_sp, buf, size, ctx, has_prefix);
    CHECKLEN_MOVEBUF_ADDRET_DECSIZE;

    if (_sp & (1ull << 0)) { len = zds_unpack_builtin(_t.host, buf, size, ctx); CHECKLEN_MOVEBUF_ADDRET_DECSIZE; }
    else { _t.host = {}; }
    if (_sp & (1ull << 1)) { len = zds_unpack_builtin(_t.port, buf, size, ctx); CHECKLEN_MOVEBUF_ADDRET_DECSIZE; }
    else { _t.port = 0; }

    _sp >>= 2;
    while (_sp) {
        if (_sp & 1) { len = zds_unpack_skip(buf, size, ctx); CHECKLEN_MOVEBUF_ADDRET_DECSIZE; }
        _sp >>= 1;
    }
    return ret;
}

// ── FgwChannelConfig ─────────────────────────────────────────────────────────
zfgw::FgwChannelConfig zfgw::FgwChannelConfig::_empty;

int zce::zdp::zds_pack(zce_byte* buf, int size, const zfgw::FgwChannelConfig& _t,
                       zds_context_t* ctx, bool has_prefix) {
    int len = 0, ret = 0;
    zce_uint64 _sp = 0;
    if (!zdp::is_empty_member(_t.channel_id)) _sp |= 1ull << 0;
    if (!zdp::is_empty_member(_t.kind))       _sp |= 1ull << 1;
    if (!(_t.remote == zfgw::FgwEndpoint::_empty)) _sp |= 1ull << 2;
    if (!zdp::is_empty_member(_t.priority))   _sp |= 1ull << 3;

    len = zds_pack_struct_header(buf, size, _sp, ctx, has_prefix);
    CHECKLEN_MOVEBUF_ADDRET_DECSIZE;

    if (_sp & (1ull << 0)) { len = zds_pack_builtin(buf, size, _t.channel_id, ctx); CHECKLEN_MOVEBUF_ADDRET_DECSIZE; }
    if (_sp & (1ull << 1)) { len = zds_pack_builtin(buf, size, _t.kind,       ctx); CHECKLEN_MOVEBUF_ADDRET_DECSIZE; }
    if (_sp & (1ull << 2)) { len = zds_pack        (buf, size, _t.remote,     ctx, true); CHECKLEN_MOVEBUF_ADDRET_DECSIZE; }
    if (_sp & (1ull << 3)) { len = zds_pack_builtin(buf, size, _t.priority,   ctx); CHECKLEN_MOVEBUF_ADDRET_DECSIZE; }
    return ret;
}

int zce::zdp::zds_unpack(zfgw::FgwChannelConfig& _t, const zce_byte* buf, int size,
                         zds_context_t* ctx, bool has_prefix) {
    int len = 0, ret = 0;
    zce_uint64 _sp = 0;
    len = zds_unpack_struct_header(_sp, buf, size, ctx, has_prefix);
    CHECKLEN_MOVEBUF_ADDRET_DECSIZE;

    if (_sp & (1ull << 0)) { len = zds_unpack_builtin(_t.channel_id, buf, size, ctx); CHECKLEN_MOVEBUF_ADDRET_DECSIZE; } else _t.channel_id = 0;
    if (_sp & (1ull << 1)) { len = zds_unpack_builtin(_t.kind,       buf, size, ctx); CHECKLEN_MOVEBUF_ADDRET_DECSIZE; } else _t.kind = 0;
    if (_sp & (1ull << 2)) { len = zds_unpack        (_t.remote,     buf, size, ctx, true); CHECKLEN_MOVEBUF_ADDRET_DECSIZE; } else _t.remote = {};
    if (_sp & (1ull << 3)) { len = zds_unpack_builtin(_t.priority,   buf, size, ctx); CHECKLEN_MOVEBUF_ADDRET_DECSIZE; } else _t.priority = 100;

    _sp >>= 4;
    while (_sp) {
        if (_sp & 1) { len = zds_unpack_skip(buf, size, ctx); CHECKLEN_MOVEBUF_ADDRET_DECSIZE; }
        _sp >>= 1;
    }
    return ret;
}

// ── FgwConfig ────────────────────────────────────────────────────────────────
zfgw::FgwConfig zfgw::FgwConfig::_empty;

int zce::zdp::zds_pack(zce_byte* buf, int size, const zfgw::FgwConfig& _t,
                       zds_context_t* ctx, bool has_prefix) {
    int len = 0, ret = 0;
    zce_uint64 _sp = 0;
    if (!zdp::is_empty_member(_t.role))                _sp |= 1ull << 0;
    if (!zdp::is_empty_member(_t.inport_listen_port))  _sp |= 1ull << 1;
    if (!zdp::is_empty_member(_t.outport_listen_host)) _sp |= 1ull << 2;
    if (!zdp::is_empty_member(_t.outport_listen_port)) _sp |= 1ull << 3;
    if (!zdp::is_empty_member(_t.egress_bind_ip))      _sp |= 1ull << 4;
    if (!_t.channels.empty())                          _sp |= 1ull << 5;
    if (!zdp::is_empty_member(_t.segment_size))        _sp |= 1ull << 6;
    if (!zdp::is_empty_member(_t.recv_window))         _sp |= 1ull << 7;
    if (!zdp::is_empty_member(_t.heartbeat_interval))  _sp |= 1ull << 8;
    if (!zdp::is_empty_member(_t.link_timeout))        _sp |= 1ull << 9;
    if (!zdp::is_empty_member(_t.reconnect_max))       _sp |= 1ull << 10;
    if (!zdp::is_empty_member(_t.multipath_mode))      _sp |= 1ull << 11;
    if (!zdp::is_empty_member(_t.enable_crc))          _sp |= 1ull << 12;
    if (!zdp::is_empty_member(_t.ingress_id))         _sp |= 1ull << 13;

    len = zds_pack_struct_header(buf, size, _sp, ctx, has_prefix);
    CHECKLEN_MOVEBUF_ADDRET_DECSIZE;

    if (_sp & (1ull <<  0)) { len = zds_pack_builtin(buf, size, _t.role, ctx);                CHECKLEN_MOVEBUF_ADDRET_DECSIZE; }
    if (_sp & (1ull <<  1)) { len = zds_pack_builtin(buf, size, _t.inport_listen_port, ctx);  CHECKLEN_MOVEBUF_ADDRET_DECSIZE; }
    if (_sp & (1ull <<  2)) { len = zds_pack_builtin(buf, size, _t.outport_listen_host, ctx); CHECKLEN_MOVEBUF_ADDRET_DECSIZE; }
    if (_sp & (1ull <<  3)) { len = zds_pack_builtin(buf, size, _t.outport_listen_port, ctx); CHECKLEN_MOVEBUF_ADDRET_DECSIZE; }
    if (_sp & (1ull <<  4)) { len = zds_pack_builtin(buf, size, _t.egress_bind_ip, ctx);      CHECKLEN_MOVEBUF_ADDRET_DECSIZE; }
    if (_sp & (1ull <<  5)) { len = zds_pack_array  (buf, size, _t.channels, ctx);            CHECKLEN_MOVEBUF_ADDRET_DECSIZE; }
    if (_sp & (1ull <<  6)) { len = zds_pack_builtin(buf, size, _t.segment_size, ctx);        CHECKLEN_MOVEBUF_ADDRET_DECSIZE; }
    if (_sp & (1ull <<  7)) { len = zds_pack_builtin(buf, size, _t.recv_window, ctx);         CHECKLEN_MOVEBUF_ADDRET_DECSIZE; }
    if (_sp & (1ull <<  8)) { len = zds_pack_builtin(buf, size, _t.heartbeat_interval, ctx);  CHECKLEN_MOVEBUF_ADDRET_DECSIZE; }
    if (_sp & (1ull <<  9)) { len = zds_pack_builtin(buf, size, _t.link_timeout, ctx);        CHECKLEN_MOVEBUF_ADDRET_DECSIZE; }
    if (_sp & (1ull << 10)) { len = zds_pack_builtin(buf, size, _t.reconnect_max, ctx);       CHECKLEN_MOVEBUF_ADDRET_DECSIZE; }
    if (_sp & (1ull << 11)) { len = zds_pack_builtin(buf, size, _t.multipath_mode, ctx);      CHECKLEN_MOVEBUF_ADDRET_DECSIZE; }
    if (_sp & (1ull << 12)) { len = zds_pack_builtin(buf, size, _t.enable_crc, ctx);          CHECKLEN_MOVEBUF_ADDRET_DECSIZE; }
    if (_sp & (1ull << 13)) { len = zds_pack_builtin(buf, size, _t.ingress_id, ctx);           CHECKLEN_MOVEBUF_ADDRET_DECSIZE; }
    return ret;
}

int zce::zdp::zds_unpack(zfgw::FgwConfig& _t, const zce_byte* buf, int size,
                         zds_context_t* ctx, bool has_prefix) {
    int len = 0, ret = 0;
    zce_uint64 _sp = 0;
    len = zds_unpack_struct_header(_sp, buf, size, ctx, has_prefix);
    CHECKLEN_MOVEBUF_ADDRET_DECSIZE;

    if (_sp & (1ull <<  0)) { len = zds_unpack_builtin(_t.role, buf, size, ctx);                CHECKLEN_MOVEBUF_ADDRET_DECSIZE; } else _t.role = 0;
    if (_sp & (1ull <<  1)) { len = zds_unpack_builtin(_t.inport_listen_port, buf, size, ctx);  CHECKLEN_MOVEBUF_ADDRET_DECSIZE; } else _t.inport_listen_port = 1080;
    if (_sp & (1ull <<  2)) { len = zds_unpack_builtin(_t.outport_listen_host, buf, size, ctx); CHECKLEN_MOVEBUF_ADDRET_DECSIZE; } else _t.outport_listen_host = {};
    if (_sp & (1ull <<  3)) { len = zds_unpack_builtin(_t.outport_listen_port, buf, size, ctx); CHECKLEN_MOVEBUF_ADDRET_DECSIZE; } else _t.outport_listen_port = 0;
    if (_sp & (1ull <<  4)) { len = zds_unpack_builtin(_t.egress_bind_ip, buf, size, ctx);      CHECKLEN_MOVEBUF_ADDRET_DECSIZE; } else _t.egress_bind_ip = {};
    if (_sp & (1ull <<  5)) { len = zds_unpack_array  (_t.channels, buf, size, ctx);            CHECKLEN_MOVEBUF_ADDRET_DECSIZE; } else _t.channels.clear();
    if (_sp & (1ull <<  6)) { len = zds_unpack_builtin(_t.segment_size, buf, size, ctx);        CHECKLEN_MOVEBUF_ADDRET_DECSIZE; } else _t.segment_size = 1200;
    if (_sp & (1ull <<  7)) { len = zds_unpack_builtin(_t.recv_window, buf, size, ctx);         CHECKLEN_MOVEBUF_ADDRET_DECSIZE; } else _t.recv_window = 1024;
    if (_sp & (1ull <<  8)) { len = zds_unpack_builtin(_t.heartbeat_interval, buf, size, ctx);  CHECKLEN_MOVEBUF_ADDRET_DECSIZE; } else _t.heartbeat_interval = 5;
    if (_sp & (1ull <<  9)) { len = zds_unpack_builtin(_t.link_timeout, buf, size, ctx);        CHECKLEN_MOVEBUF_ADDRET_DECSIZE; } else _t.link_timeout = 15;
    if (_sp & (1ull << 10)) { len = zds_unpack_builtin(_t.reconnect_max, buf, size, ctx);       CHECKLEN_MOVEBUF_ADDRET_DECSIZE; } else _t.reconnect_max = 60;
    if (_sp & (1ull << 11)) { len = zds_unpack_builtin(_t.multipath_mode, buf, size, ctx);      CHECKLEN_MOVEBUF_ADDRET_DECSIZE; } else _t.multipath_mode = 2;
    if (_sp & (1ull << 12)) { len = zds_unpack_builtin(_t.enable_crc, buf, size, ctx);          CHECKLEN_MOVEBUF_ADDRET_DECSIZE; } else _t.enable_crc = 1;
    if (_sp & (1ull << 13)) { len = zds_unpack_builtin(_t.ingress_id, buf, size, ctx);           CHECKLEN_MOVEBUF_ADDRET_DECSIZE; } else _t.ingress_id = 0;

    _sp >>= 14;
    while (_sp) {
        if (_sp & 1) { len = zds_unpack_skip(buf, size, ctx); CHECKLEN_MOVEBUF_ADDRET_DECSIZE; }
        _sp >>= 1;
    }
    return ret;
}

// ── FgwLinkQuality ───────────────────────────────────────────────────────────
zfgw::FgwLinkQuality zfgw::FgwLinkQuality::_empty;

int zce::zdp::zds_pack(zce_byte* buf, int size, const zfgw::FgwLinkQuality& _t,
                       zds_context_t* ctx, bool has_prefix) {
    int len = 0, ret = 0;
    zce_uint64 _sp = 0;
    if (!zdp::is_empty_member(_t.channel_id))      _sp |= 1ull << 0;
    if (!zdp::is_empty_member(_t.connected))       _sp |= 1ull << 1;
    if (!zdp::is_empty_member(_t.rtt_ms))          _sp |= 1ull << 2;
    if (!zdp::is_empty_member(_t.loss_rate))       _sp |= 1ull << 3;
    if (!zdp::is_empty_member(_t.bytes_sent))      _sp |= 1ull << 4;
    if (!zdp::is_empty_member(_t.bytes_recv))      _sp |= 1ull << 5;
    if (!zdp::is_empty_member(_t.send_segments))   _sp |= 1ull << 6;
    if (!zdp::is_empty_member(_t.recv_segments))   _sp |= 1ull << 7;
    if (!zdp::is_empty_member(_t.rexmit_segments)) _sp |= 1ull << 8;

    len = zds_pack_struct_header(buf, size, _sp, ctx, has_prefix);
    CHECKLEN_MOVEBUF_ADDRET_DECSIZE;

    if (_sp & (1ull << 0)) { len = zds_pack_builtin(buf, size, _t.channel_id, ctx);      CHECKLEN_MOVEBUF_ADDRET_DECSIZE; }
    if (_sp & (1ull << 1)) { len = zds_pack_builtin(buf, size, _t.connected, ctx);       CHECKLEN_MOVEBUF_ADDRET_DECSIZE; }
    if (_sp & (1ull << 2)) { len = zds_pack_builtin(buf, size, _t.rtt_ms, ctx);          CHECKLEN_MOVEBUF_ADDRET_DECSIZE; }
    if (_sp & (1ull << 3)) { len = zds_pack_builtin(buf, size, _t.loss_rate, ctx);       CHECKLEN_MOVEBUF_ADDRET_DECSIZE; }
    if (_sp & (1ull << 4)) { len = zds_pack_builtin(buf, size, _t.bytes_sent, ctx);      CHECKLEN_MOVEBUF_ADDRET_DECSIZE; }
    if (_sp & (1ull << 5)) { len = zds_pack_builtin(buf, size, _t.bytes_recv, ctx);      CHECKLEN_MOVEBUF_ADDRET_DECSIZE; }
    if (_sp & (1ull << 6)) { len = zds_pack_builtin(buf, size, _t.send_segments, ctx);   CHECKLEN_MOVEBUF_ADDRET_DECSIZE; }
    if (_sp & (1ull << 7)) { len = zds_pack_builtin(buf, size, _t.recv_segments, ctx);   CHECKLEN_MOVEBUF_ADDRET_DECSIZE; }
    if (_sp & (1ull << 8)) { len = zds_pack_builtin(buf, size, _t.rexmit_segments, ctx); CHECKLEN_MOVEBUF_ADDRET_DECSIZE; }
    return ret;
}

int zce::zdp::zds_unpack(zfgw::FgwLinkQuality& _t, const zce_byte* buf, int size,
                         zds_context_t* ctx, bool has_prefix) {
    int len = 0, ret = 0;
    zce_uint64 _sp = 0;
    len = zds_unpack_struct_header(_sp, buf, size, ctx, has_prefix);
    CHECKLEN_MOVEBUF_ADDRET_DECSIZE;

    if (_sp & (1ull << 0)) { len = zds_unpack_builtin(_t.channel_id, buf, size, ctx);      CHECKLEN_MOVEBUF_ADDRET_DECSIZE; } else _t.channel_id = 0;
    if (_sp & (1ull << 1)) { len = zds_unpack_builtin(_t.connected, buf, size, ctx);       CHECKLEN_MOVEBUF_ADDRET_DECSIZE; } else _t.connected = 0;
    if (_sp & (1ull << 2)) { len = zds_unpack_builtin(_t.rtt_ms, buf, size, ctx);          CHECKLEN_MOVEBUF_ADDRET_DECSIZE; } else _t.rtt_ms = 0;
    if (_sp & (1ull << 3)) { len = zds_unpack_builtin(_t.loss_rate, buf, size, ctx);       CHECKLEN_MOVEBUF_ADDRET_DECSIZE; } else _t.loss_rate = 0.0;
    if (_sp & (1ull << 4)) { len = zds_unpack_builtin(_t.bytes_sent, buf, size, ctx);      CHECKLEN_MOVEBUF_ADDRET_DECSIZE; } else _t.bytes_sent = 0;
    if (_sp & (1ull << 5)) { len = zds_unpack_builtin(_t.bytes_recv, buf, size, ctx);      CHECKLEN_MOVEBUF_ADDRET_DECSIZE; } else _t.bytes_recv = 0;
    if (_sp & (1ull << 6)) { len = zds_unpack_builtin(_t.send_segments, buf, size, ctx);   CHECKLEN_MOVEBUF_ADDRET_DECSIZE; } else _t.send_segments = 0;
    if (_sp & (1ull << 7)) { len = zds_unpack_builtin(_t.recv_segments, buf, size, ctx);   CHECKLEN_MOVEBUF_ADDRET_DECSIZE; } else _t.recv_segments = 0;
    if (_sp & (1ull << 8)) { len = zds_unpack_builtin(_t.rexmit_segments, buf, size, ctx); CHECKLEN_MOVEBUF_ADDRET_DECSIZE; } else _t.rexmit_segments = 0;

    _sp >>= 9;
    while (_sp) {
        if (_sp & 1) { len = zds_unpack_skip(buf, size, ctx); CHECKLEN_MOVEBUF_ADDRET_DECSIZE; }
        _sp >>= 1;
    }
    return ret;
}

// ── FgwSessionStat ───────────────────────────────────────────────────────────
zfgw::FgwSessionStat zfgw::FgwSessionStat::_empty;

int zce::zdp::zds_pack(zce_byte* buf, int size, const zfgw::FgwSessionStat& _t,
                       zds_context_t* ctx, bool has_prefix) {
    int len = 0, ret = 0;
    zce_uint64 _sp = 0;
    if (!zdp::is_empty_member(_t.session_id))     _sp |= 1ull << 0;
    if (!zdp::is_empty_member(_t.client_addr))    _sp |= 1ull << 1;
    if (!zdp::is_empty_member(_t.target_addr))    _sp |= 1ull << 2;
    if (!zdp::is_empty_member(_t.bytes_sent))     _sp |= 1ull << 3;
    if (!zdp::is_empty_member(_t.bytes_recv))     _sp |= 1ull << 4;
    if (!zdp::is_empty_member(_t.open_timestamp)) _sp |= 1ull << 5;
    if (!zdp::is_empty_member(_t.ingress_id))     _sp |= 1ull << 6;

    len = zds_pack_struct_header(buf, size, _sp, ctx, has_prefix);
    CHECKLEN_MOVEBUF_ADDRET_DECSIZE;

    if (_sp & (1ull << 0)) { len = zds_pack_builtin(buf, size, _t.session_id, ctx);     CHECKLEN_MOVEBUF_ADDRET_DECSIZE; }
    if (_sp & (1ull << 1)) { len = zds_pack_builtin(buf, size, _t.client_addr, ctx);    CHECKLEN_MOVEBUF_ADDRET_DECSIZE; }
    if (_sp & (1ull << 2)) { len = zds_pack_builtin(buf, size, _t.target_addr, ctx);    CHECKLEN_MOVEBUF_ADDRET_DECSIZE; }
    if (_sp & (1ull << 3)) { len = zds_pack_builtin(buf, size, _t.bytes_sent, ctx);     CHECKLEN_MOVEBUF_ADDRET_DECSIZE; }
    if (_sp & (1ull << 4)) { len = zds_pack_builtin(buf, size, _t.bytes_recv, ctx);     CHECKLEN_MOVEBUF_ADDRET_DECSIZE; }
    if (_sp & (1ull << 5)) { len = zds_pack_builtin(buf, size, _t.open_timestamp, ctx); CHECKLEN_MOVEBUF_ADDRET_DECSIZE; }
    if (_sp & (1ull << 6)) { len = zds_pack_builtin(buf, size, _t.ingress_id, ctx);     CHECKLEN_MOVEBUF_ADDRET_DECSIZE; }
    return ret;
}

int zce::zdp::zds_unpack(zfgw::FgwSessionStat& _t, const zce_byte* buf, int size,
                         zds_context_t* ctx, bool has_prefix) {
    int len = 0, ret = 0;
    zce_uint64 _sp = 0;
    len = zds_unpack_struct_header(_sp, buf, size, ctx, has_prefix);
    CHECKLEN_MOVEBUF_ADDRET_DECSIZE;

    if (_sp & (1ull << 0)) { len = zds_unpack_builtin(_t.session_id, buf, size, ctx);     CHECKLEN_MOVEBUF_ADDRET_DECSIZE; } else _t.session_id = 0;
    if (_sp & (1ull << 1)) { len = zds_unpack_builtin(_t.client_addr, buf, size, ctx);    CHECKLEN_MOVEBUF_ADDRET_DECSIZE; } else _t.client_addr = {};
    if (_sp & (1ull << 2)) { len = zds_unpack_builtin(_t.target_addr, buf, size, ctx);    CHECKLEN_MOVEBUF_ADDRET_DECSIZE; } else _t.target_addr = {};
    if (_sp & (1ull << 3)) { len = zds_unpack_builtin(_t.bytes_sent, buf, size, ctx);     CHECKLEN_MOVEBUF_ADDRET_DECSIZE; } else _t.bytes_sent = 0;
    if (_sp & (1ull << 4)) { len = zds_unpack_builtin(_t.bytes_recv, buf, size, ctx);     CHECKLEN_MOVEBUF_ADDRET_DECSIZE; } else _t.bytes_recv = 0;
    if (_sp & (1ull << 5)) { len = zds_unpack_builtin(_t.open_timestamp, buf, size, ctx); CHECKLEN_MOVEBUF_ADDRET_DECSIZE; } else _t.open_timestamp = 0;
    if (_sp & (1ull << 6)) { len = zds_unpack_builtin(_t.ingress_id, buf, size, ctx);     CHECKLEN_MOVEBUF_ADDRET_DECSIZE; } else _t.ingress_id = 0;

    _sp >>= 7;
    while (_sp) {
        if (_sp & 1) { len = zds_unpack_skip(buf, size, ctx); CHECKLEN_MOVEBUF_ADDRET_DECSIZE; }
        _sp >>= 1;
    }
    return ret;
}

// ── FgwStatus ────────────────────────────────────────────────────────────────
zfgw::FgwStatus zfgw::FgwStatus::_empty;

int zce::zdp::zds_pack(zce_byte* buf, int size, const zfgw::FgwStatus& _t,
                       zds_context_t* ctx, bool has_prefix) {
    int len = 0, ret = 0;
    zce_uint64 _sp = 0;
    if (!zdp::is_empty_member(_t.running))       _sp |= 1ull << 0;
    if (!zdp::is_empty_member(_t.role))          _sp |= 1ull << 1;
    if (!zdp::is_empty_member(_t.session_count)) _sp |= 1ull << 2;
    if (!_t.links.empty())                       _sp |= 1ull << 3;
    if (!_t.sessions.empty())                    _sp |= 1ull << 4;

    len = zds_pack_struct_header(buf, size, _sp, ctx, has_prefix);
    CHECKLEN_MOVEBUF_ADDRET_DECSIZE;

    if (_sp & (1ull << 0)) { len = zds_pack_builtin(buf, size, _t.running, ctx);       CHECKLEN_MOVEBUF_ADDRET_DECSIZE; }
    if (_sp & (1ull << 1)) { len = zds_pack_builtin(buf, size, _t.role, ctx);          CHECKLEN_MOVEBUF_ADDRET_DECSIZE; }
    if (_sp & (1ull << 2)) { len = zds_pack_builtin(buf, size, _t.session_count, ctx); CHECKLEN_MOVEBUF_ADDRET_DECSIZE; }
    if (_sp & (1ull << 3)) { len = zds_pack_array  (buf, size, _t.links, ctx);         CHECKLEN_MOVEBUF_ADDRET_DECSIZE; }
    if (_sp & (1ull << 4)) { len = zds_pack_array  (buf, size, _t.sessions, ctx);      CHECKLEN_MOVEBUF_ADDRET_DECSIZE; }
    return ret;
}

int zce::zdp::zds_unpack(zfgw::FgwStatus& _t, const zce_byte* buf, int size,
                         zds_context_t* ctx, bool has_prefix) {
    int len = 0, ret = 0;
    zce_uint64 _sp = 0;
    len = zds_unpack_struct_header(_sp, buf, size, ctx, has_prefix);
    CHECKLEN_MOVEBUF_ADDRET_DECSIZE;

    if (_sp & (1ull << 0)) { len = zds_unpack_builtin(_t.running, buf, size, ctx);       CHECKLEN_MOVEBUF_ADDRET_DECSIZE; } else _t.running = 0;
    if (_sp & (1ull << 1)) { len = zds_unpack_builtin(_t.role, buf, size, ctx);          CHECKLEN_MOVEBUF_ADDRET_DECSIZE; } else _t.role = 0;
    if (_sp & (1ull << 2)) { len = zds_unpack_builtin(_t.session_count, buf, size, ctx); CHECKLEN_MOVEBUF_ADDRET_DECSIZE; } else _t.session_count = 0;
    if (_sp & (1ull << 3)) { len = zds_unpack_array  (_t.links, buf, size, ctx);         CHECKLEN_MOVEBUF_ADDRET_DECSIZE; } else _t.links.clear();
    if (_sp & (1ull << 4)) { len = zds_unpack_array  (_t.sessions, buf, size, ctx);      CHECKLEN_MOVEBUF_ADDRET_DECSIZE; } else _t.sessions.clear();

    _sp >>= 5;
    while (_sp) {
        if (_sp & 1) { len = zds_unpack_skip(buf, size, ctx); CHECKLEN_MOVEBUF_ADDRET_DECSIZE; }
        _sp >>= 1;
    }
    return ret;
}

// ── FgwEmpty / FgwStartRequest / FgwStartResult / etc ────────────────────────
zfgw::FgwEmpty zfgw::FgwEmpty::_empty;

int zce::zdp::zds_pack(zce_byte* buf, int size, const zfgw::FgwEmpty& _t,
                       zds_context_t* ctx, bool has_prefix) {
    int len = 0, ret = 0;
    zce_uint64 _sp = 0;
    if (!zdp::is_empty_member(_t._placeholder)) _sp |= 1ull << 0;
    len = zds_pack_struct_header(buf, size, _sp, ctx, has_prefix);
    CHECKLEN_MOVEBUF_ADDRET_DECSIZE;
    if (_sp & (1ull << 0)) { len = zds_pack_builtin(buf, size, _t._placeholder, ctx); CHECKLEN_MOVEBUF_ADDRET_DECSIZE; }
    return ret;
}

int zce::zdp::zds_unpack(zfgw::FgwEmpty& _t, const zce_byte* buf, int size,
                         zds_context_t* ctx, bool has_prefix) {
    int len = 0, ret = 0;
    zce_uint64 _sp = 0;
    len = zds_unpack_struct_header(_sp, buf, size, ctx, has_prefix);
    CHECKLEN_MOVEBUF_ADDRET_DECSIZE;
    if (_sp & (1ull << 0)) { len = zds_unpack_builtin(_t._placeholder, buf, size, ctx); CHECKLEN_MOVEBUF_ADDRET_DECSIZE; } else _t._placeholder = 0;
    _sp >>= 1;
    while (_sp) { if (_sp & 1) { len = zds_unpack_skip(buf, size, ctx); CHECKLEN_MOVEBUF_ADDRET_DECSIZE; } _sp >>= 1; }
    return ret;
}

zfgw::FgwStartRequest zfgw::FgwStartRequest::_empty;

int zce::zdp::zds_pack(zce_byte* buf, int size, const zfgw::FgwStartRequest& _t,
                       zds_context_t* ctx, bool has_prefix) {
    int len = 0, ret = 0;
    zce_uint64 _sp = 0;
    if (!zdp::is_empty_member(_t.force_restart)) _sp |= 1ull << 0;
    len = zds_pack_struct_header(buf, size, _sp, ctx, has_prefix);
    CHECKLEN_MOVEBUF_ADDRET_DECSIZE;
    if (_sp & (1ull << 0)) { len = zds_pack_builtin(buf, size, _t.force_restart, ctx); CHECKLEN_MOVEBUF_ADDRET_DECSIZE; }
    return ret;
}

int zce::zdp::zds_unpack(zfgw::FgwStartRequest& _t, const zce_byte* buf, int size,
                         zds_context_t* ctx, bool has_prefix) {
    int len = 0, ret = 0;
    zce_uint64 _sp = 0;
    len = zds_unpack_struct_header(_sp, buf, size, ctx, has_prefix);
    CHECKLEN_MOVEBUF_ADDRET_DECSIZE;
    if (_sp & (1ull << 0)) { len = zds_unpack_builtin(_t.force_restart, buf, size, ctx); CHECKLEN_MOVEBUF_ADDRET_DECSIZE; } else _t.force_restart = 0;
    _sp >>= 1;
    while (_sp) { if (_sp & 1) { len = zds_unpack_skip(buf, size, ctx); CHECKLEN_MOVEBUF_ADDRET_DECSIZE; } _sp >>= 1; }
    return ret;
}

zfgw::FgwStartResult zfgw::FgwStartResult::_empty;

int zce::zdp::zds_pack(zce_byte* buf, int size, const zfgw::FgwStartResult& _t,
                       zds_context_t* ctx, bool has_prefix) {
    int len = 0, ret = 0;
    zce_uint64 _sp = 0;
    if (!zdp::is_empty_member(_t.errcode)) _sp |= 1ull << 0;
    if (!zdp::is_empty_member(_t.message)) _sp |= 1ull << 1;
    len = zds_pack_struct_header(buf, size, _sp, ctx, has_prefix);
    CHECKLEN_MOVEBUF_ADDRET_DECSIZE;
    if (_sp & (1ull << 0)) { len = zds_pack_builtin(buf, size, _t.errcode, ctx); CHECKLEN_MOVEBUF_ADDRET_DECSIZE; }
    if (_sp & (1ull << 1)) { len = zds_pack_builtin(buf, size, _t.message, ctx); CHECKLEN_MOVEBUF_ADDRET_DECSIZE; }
    return ret;
}

int zce::zdp::zds_unpack(zfgw::FgwStartResult& _t, const zce_byte* buf, int size,
                         zds_context_t* ctx, bool has_prefix) {
    int len = 0, ret = 0;
    zce_uint64 _sp = 0;
    len = zds_unpack_struct_header(_sp, buf, size, ctx, has_prefix);
    CHECKLEN_MOVEBUF_ADDRET_DECSIZE;
    if (_sp & (1ull << 0)) { len = zds_unpack_builtin(_t.errcode, buf, size, ctx); CHECKLEN_MOVEBUF_ADDRET_DECSIZE; } else _t.errcode = 0;
    if (_sp & (1ull << 1)) { len = zds_unpack_builtin(_t.message, buf, size, ctx); CHECKLEN_MOVEBUF_ADDRET_DECSIZE; } else _t.message = {};
    _sp >>= 2;
    while (_sp) { if (_sp & 1) { len = zds_unpack_skip(buf, size, ctx); CHECKLEN_MOVEBUF_ADDRET_DECSIZE; } _sp >>= 1; }
    return ret;
}

zfgw::FgwAddChannelRequest zfgw::FgwAddChannelRequest::_empty;

int zce::zdp::zds_pack(zce_byte* buf, int size, const zfgw::FgwAddChannelRequest& _t,
                       zds_context_t* ctx, bool has_prefix) {
    int len = 0, ret = 0;
    zce_uint64 _sp = 0;
    if (!(_t.channel == zfgw::FgwChannelConfig::_empty)) _sp |= 1ull << 0;
    len = zds_pack_struct_header(buf, size, _sp, ctx, has_prefix);
    CHECKLEN_MOVEBUF_ADDRET_DECSIZE;
    if (_sp & (1ull << 0)) { len = zds_pack(buf, size, _t.channel, ctx, true); CHECKLEN_MOVEBUF_ADDRET_DECSIZE; }
    return ret;
}

int zce::zdp::zds_unpack(zfgw::FgwAddChannelRequest& _t, const zce_byte* buf, int size,
                         zds_context_t* ctx, bool has_prefix) {
    int len = 0, ret = 0;
    zce_uint64 _sp = 0;
    len = zds_unpack_struct_header(_sp, buf, size, ctx, has_prefix);
    CHECKLEN_MOVEBUF_ADDRET_DECSIZE;
    if (_sp & (1ull << 0)) { len = zds_unpack(_t.channel, buf, size, ctx, true); CHECKLEN_MOVEBUF_ADDRET_DECSIZE; } else _t.channel = {};
    _sp >>= 1;
    while (_sp) { if (_sp & 1) { len = zds_unpack_skip(buf, size, ctx); CHECKLEN_MOVEBUF_ADDRET_DECSIZE; } _sp >>= 1; }
    return ret;
}

zfgw::FgwRemoveChannelRequest zfgw::FgwRemoveChannelRequest::_empty;

int zce::zdp::zds_pack(zce_byte* buf, int size, const zfgw::FgwRemoveChannelRequest& _t,
                       zds_context_t* ctx, bool has_prefix) {
    int len = 0, ret = 0;
    zce_uint64 _sp = 0;
    if (!zdp::is_empty_member(_t.channel_id)) _sp |= 1ull << 0;
    len = zds_pack_struct_header(buf, size, _sp, ctx, has_prefix);
    CHECKLEN_MOVEBUF_ADDRET_DECSIZE;
    if (_sp & (1ull << 0)) { len = zds_pack_builtin(buf, size, _t.channel_id, ctx); CHECKLEN_MOVEBUF_ADDRET_DECSIZE; }
    return ret;
}

int zce::zdp::zds_unpack(zfgw::FgwRemoveChannelRequest& _t, const zce_byte* buf, int size,
                         zds_context_t* ctx, bool has_prefix) {
    int len = 0, ret = 0;
    zce_uint64 _sp = 0;
    len = zds_unpack_struct_header(_sp, buf, size, ctx, has_prefix);
    CHECKLEN_MOVEBUF_ADDRET_DECSIZE;
    if (_sp & (1ull << 0)) { len = zds_unpack_builtin(_t.channel_id, buf, size, ctx); CHECKLEN_MOVEBUF_ADDRET_DECSIZE; } else _t.channel_id = 0;
    _sp >>= 1;
    while (_sp) { if (_sp & 1) { len = zds_unpack_skip(buf, size, ctx); CHECKLEN_MOVEBUF_ADDRET_DECSIZE; } _sp >>= 1; }
    return ret;
}

zfgw::FgwSetConfigRequest zfgw::FgwSetConfigRequest::_empty;

int zce::zdp::zds_pack(zce_byte* buf, int size, const zfgw::FgwSetConfigRequest& _t,
                       zds_context_t* ctx, bool has_prefix) {
    int len = 0, ret = 0;
    zce_uint64 _sp = 0;
    if (!(_t.config == zfgw::FgwConfig::_empty)) _sp |= 1ull << 0;
    len = zds_pack_struct_header(buf, size, _sp, ctx, has_prefix);
    CHECKLEN_MOVEBUF_ADDRET_DECSIZE;
    if (_sp & (1ull << 0)) { len = zds_pack(buf, size, _t.config, ctx, true); CHECKLEN_MOVEBUF_ADDRET_DECSIZE; }
    return ret;
}

int zce::zdp::zds_unpack(zfgw::FgwSetConfigRequest& _t, const zce_byte* buf, int size,
                         zds_context_t* ctx, bool has_prefix) {
    int len = 0, ret = 0;
    zce_uint64 _sp = 0;
    len = zds_unpack_struct_header(_sp, buf, size, ctx, has_prefix);
    CHECKLEN_MOVEBUF_ADDRET_DECSIZE;
    if (_sp & (1ull << 0)) { len = zds_unpack(_t.config, buf, size, ctx, true); CHECKLEN_MOVEBUF_ADDRET_DECSIZE; } else _t.config = {};
    _sp >>= 1;
    while (_sp) { if (_sp & 1) { len = zds_unpack_skip(buf, size, ctx); CHECKLEN_MOVEBUF_ADDRET_DECSIZE; } _sp >>= 1; }
    return ret;
}
