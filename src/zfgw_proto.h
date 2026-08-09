// ***************************************************************
//  zfgw_proto
//  ---------------------------------------------------------------
//  This file follows the layout produced by zGen for .ptl files.
//  It is intentionally hand-maintained so the libfgw build does
//  not require the zgen tool to be present on every workstation.
//
//  Yongming Wang(wangym@gmail.com)
//  Copyright (C) 2026 - All Rights Reserved
// *****************************************************************

#pragma once

#include <zce/zce_config.h>
#include <zce/zce_object.h>
#include <zce/zce_types.h>
#include <zce/zce_dblock.h>
#include <zce/zce_any.h>

namespace zfgw {

struct FgwEndpoint {
    static FgwEndpoint _empty;
    bool operator==(const FgwEndpoint& _t) const noexcept {
        if (host != _t.host) return false;
        if (port != _t.port) return false;
        return true;
    }

    zce_astring host;
    zce_uint16  port = 0;
};

struct FgwChannelConfig {
    static FgwChannelConfig _empty;
    bool operator==(const FgwChannelConfig& _t) const noexcept {
        if (channel_id != _t.channel_id) return false;
        if (kind != _t.kind) return false;
        if (!(remote == _t.remote)) return false;
        if (priority != _t.priority) return false;
        return true;
    }

    zce_uint32   channel_id = 0;
    zce_byte     kind = 0;            /* 0 = TCP, 1 = UTP */
    FgwEndpoint  remote;
    zce_uint32   priority = 100;
};

struct FgwConfig {
    static FgwConfig _empty;
    bool operator==(const FgwConfig& _t) const noexcept {
        if (role != _t.role) return false;
        if (ingress_id != _t.ingress_id) return false;
        if (inport_listen_port != _t.inport_listen_port) return false;
        if (outport_listen_host != _t.outport_listen_host) return false;
        if (outport_listen_port != _t.outport_listen_port) return false;
        if (egress_bind_ip != _t.egress_bind_ip) return false;
        if (channels != _t.channels) return false;
        if (segment_size != _t.segment_size) return false;
        if (recv_window != _t.recv_window) return false;
        if (heartbeat_interval != _t.heartbeat_interval) return false;
        if (link_timeout != _t.link_timeout) return false;
        if (reconnect_max != _t.reconnect_max) return false;
        if (multipath_mode != _t.multipath_mode) return false;
        if (enable_crc != _t.enable_crc) return false;
        return true;
    }

    zce_byte     role = 0;
    zce_uint32   ingress_id = 0   /* unique per Inport VM when sharing links */;
    zce_uint16   inport_listen_port = 1080;
    zce_astring  outport_listen_host;
    zce_uint16   outport_listen_port = 0;
    zce_astring  egress_bind_ip;
    std::vector<FgwChannelConfig> channels;
    zce_uint16   segment_size = 1200;
    zce_uint16   recv_window = 1024;
    zce_uint16   heartbeat_interval = 5;
    zce_uint16   link_timeout = 15;
    zce_uint16   reconnect_max = 60;
    zce_byte     multipath_mode = 2;  /* 0 = best, 1 = all, 2 = weighted */
    zce_byte     enable_crc = 1;
};

struct FgwLinkQuality {
    static FgwLinkQuality _empty;
    bool operator==(const FgwLinkQuality& _t) const noexcept {
        if (channel_id != _t.channel_id) return false;
        if (connected != _t.connected) return false;
        if (rtt_ms != _t.rtt_ms) return false;
        if (loss_rate != _t.loss_rate) return false;
        if (bytes_sent != _t.bytes_sent) return false;
        if (bytes_recv != _t.bytes_recv) return false;
        if (send_segments != _t.send_segments) return false;
        if (recv_segments != _t.recv_segments) return false;
        if (rexmit_segments != _t.rexmit_segments) return false;
        return true;
    }

    zce_uint32 channel_id = 0;
    zce_byte   connected = 0;
    zce_uint32 rtt_ms = 0;
    zce_double loss_rate = 0.0;
    zce_uint64 bytes_sent = 0;
    zce_uint64 bytes_recv = 0;
    zce_uint32 send_segments = 0;
    zce_uint32 recv_segments = 0;
    zce_uint32 rexmit_segments = 0;
};

struct FgwSessionStat {
    static FgwSessionStat _empty;
    bool operator==(const FgwSessionStat& _t) const noexcept {
        if (session_id != _t.session_id) return false;
        if (ingress_id != _t.ingress_id) return false;
        if (client_addr != _t.client_addr) return false;
        if (target_addr != _t.target_addr) return false;
        if (bytes_sent != _t.bytes_sent) return false;
        if (bytes_recv != _t.bytes_recv) return false;
        if (open_timestamp != _t.open_timestamp) return false;
        return true;
    }

    zce_uint32  session_id = 0;
    zce_uint32  ingress_id = 0;
    zce_astring client_addr;
    zce_astring target_addr;
    zce_uint64  bytes_sent = 0;
    zce_uint64  bytes_recv = 0;
    zce_uint32  open_timestamp = 0;
};

struct FgwStatus {
    static FgwStatus _empty;
    bool operator==(const FgwStatus& _t) const noexcept {
        if (running != _t.running) return false;
        if (role != _t.role) return false;
        if (session_count != _t.session_count) return false;
        if (links != _t.links) return false;
        if (sessions != _t.sessions) return false;
        return true;
    }

    zce_byte   running = 0;
    zce_byte   role = 0;
    zce_uint32 session_count = 0;
    std::vector<FgwLinkQuality> links;
    std::vector<FgwSessionStat> sessions;
};

struct FgwEmpty {
    static FgwEmpty _empty;
    bool operator==(const FgwEmpty& _t) const noexcept {
        if (_placeholder != _t._placeholder) return false;
        return true;
    }
    zce_byte _placeholder = 0;
};

struct FgwStartRequest {
    static FgwStartRequest _empty;
    bool operator==(const FgwStartRequest& _t) const noexcept {
        if (force_restart != _t.force_restart) return false;
        return true;
    }
    zce_byte force_restart = 0;
};

struct FgwStartResult {
    static FgwStartResult _empty;
    bool operator==(const FgwStartResult& _t) const noexcept {
        if (errcode != _t.errcode) return false;
        if (message != _t.message) return false;
        return true;
    }
    zce_int32   errcode = 0;
    zce_astring message;
};

struct FgwAddChannelRequest {
    static FgwAddChannelRequest _empty;
    bool operator==(const FgwAddChannelRequest& _t) const noexcept {
        if (!(channel == _t.channel)) return false;
        return true;
    }
    FgwChannelConfig channel;
};

struct FgwRemoveChannelRequest {
    static FgwRemoveChannelRequest _empty;
    bool operator==(const FgwRemoveChannelRequest& _t) const noexcept {
        if (channel_id != _t.channel_id) return false;
        return true;
    }
    zce_uint32 channel_id = 0;
};

struct FgwSetConfigRequest {
    static FgwSetConfigRequest _empty;
    bool operator==(const FgwSetConfigRequest& _t) const noexcept {
        if (!(config == _t.config)) return false;
        return true;
    }
    FgwConfig config;
};

}  // namespace zfgw
