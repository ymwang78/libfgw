// ***************************************************************
//  zfgw.h  — public constants, error codes, init hook.
//  Yongming Wang(wangym@gmail.com)
//  Copyright (C) 2026 - All Rights Reserved
// ***************************************************************
#pragma once

#include "zfgw_proto.h"

namespace zce {
struct AppOptions;
}

namespace zfgw {
/// Pass to `DataStream::{sendSessionData,closeSession}(…, ingress_key)` to use
/// `FgwConfig.ingress_id` as the session map key / wire ingress (Inport side).
inline constexpr zce_uint32 kFgwWireIngressUseLocal = 0xFFFFFFFFu;

/// Wire protocol version advertised in FgwHello.proto_version. The receiver
/// rejects a handshake whose version it does not understand.
inline constexpr zce_uint16 kFgwProtoVersion = 2;
}  // namespace zfgw

/// libfgw module-level error codes (range 0x83040000 - 0x8304FFFF).
enum ZFGW_ERRCODE {
    ZFGW_ERRCODE_OK = 0x03040000,

    ZFGW_ERRCODE_BASE = 0x83040000,
    ZFGW_ERRCODE_EXCEPTION,
    ZFGW_ERRCODE_MEMORY,
    ZFGW_ERRCODE_BUSY,               ///< already running / invalid state
    ZFGW_ERRCODE_NOTRUNNING,
    ZFGW_ERRCODE_INVALIDCONFIG,
    ZFGW_ERRCODE_INVALIDROLE,
    ZFGW_ERRCODE_LISTENFAIL,
    ZFGW_ERRCODE_NOCHANNEL,          ///< all link channels are down
    ZFGW_ERRCODE_CHANNELFAIL,        ///< channel connect failed
    ZFGW_ERRCODE_BADSEGMENT,         ///< magic/CRC/length wrong
    ZFGW_ERRCODE_WINDOWFULL,         ///< receive window full
    ZFGW_ERRCODE_SOCKS5REJECT,       ///< SOCKS5 server replied error
    ZFGW_ERRCODE_SOCKS5BADVERSION,
    ZFGW_ERRCODE_SOCKS5BADCMD,
    ZFGW_ERRCODE_SOCKS5AUTHFAIL,
    ZFGW_ERRCODE_TARGETUNREACH,
    ZFGW_ERRCODE_PATHFILEWRITE,
    ZFGW_ERRCODE_PATHFILEREAD,
    ZFGW_ERRCODE_CHANNELEXISTS,
    ZFGW_ERRCODE_CHANNELNOTFOUND,
};

/// Role id used in FgwConfig.role
enum ZFGW_ROLE {
    ZFGW_ROLE_INPORT  = 0,  ///< local entry, accepts SOCKS5 clients
    ZFGW_ROLE_OUTPORT = 1,  ///< remote egress, terminates SOCKS5 on DataStream
};

/// Link kind used in FgwChannelConfig.kind
enum ZFGW_CHANNEL_KIND {
    ZFGW_CHANNEL_TCP = 0,
    ZFGW_CHANNEL_UTP = 1,
};

/// Multipath send strategy selector.
enum ZFGW_MULTIPATH_MODE {
    ZFGW_MULTIPATH_BEST     = 0,  ///< only send on lowest-RTT channel
    ZFGW_MULTIPATH_ALL      = 1,  ///< duplicate every segment to all channels
    ZFGW_MULTIPATH_WEIGHTED = 2,  ///< weighted by 1/(rtt*(1+loss))
};

/// Project / config filename helpers (mirrors libmpc behaviour).
extern std::string getFgwVMRelativeFilePath(const std::string& host_dir, const std::string& vmname,
                                            const std::string& vmpath);

/// Host-integration entry point (called from the hostvm plugin loader).
extern int zfgw_init(const struct zce::AppOptions* start_t);

extern int zfgw_save_config(const zfgw::FgwConfig& config, const std::string& host_dir,
                            const std::string& vmname, const std::string& vmpath);

extern int zfgw_load_config(zfgw::FgwConfig& config, const std::string& host_dir,
                            const std::string& vmname, const std::string& vmpath);
