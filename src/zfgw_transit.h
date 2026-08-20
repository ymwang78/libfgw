// ***************************************************************
//  zfgw_transit.h  — stateless middle-hop forwarder.
//
//  A Transit sits between Inports and Outports. It never terminates
//  SOCKS5, never runs a DataStream, and holds no per-session state.
//  It simply frames the segment stream on each link and forwards each
//  whole segment (CRC untouched) to the correct peer:
//    - segments from an Inport  → routed by the header's outport_id
//    - segments from an Outport → routed by the header's ingress_id
//  The two routing tables are learned from the FgwHello handshake and
//  from the segment headers themselves.
//
//  Yongming Wang(wangym@gmail.com)
//  Copyright (C) 2026 - All Rights Reserved
// ***************************************************************
#pragma once

#include "zfgw_channel.h"
#include "zfgw_segment.h"
#include <zce/zce_handler.h>
#include <zce/zce_reactor.h>
#include <zce/zce_sync.h>
#include <zce/zce_task_queue.h>
#include <map>
#include <vector>

namespace zfgw {

/// TransitService — routes raw segments between Inport-facing (accepted) links
/// and Outport-facing (dialed) links, statelessly per session.
class TransitService : public zce::Object {
  public:
    TransitService(const zce::SmartPtr<zce::Reactor>& reactor, zce::TaskQueue* sync_queue,
                   const FgwConfig& config);
    ~TransitService() override;

    /// Dial the configured Outport links (config.channels: channel_id == the
    /// outport_id it serves) and start accepting inbound Inport links on
    /// (outport_listen_host, outport_listen_port).
    int  start();
    void stop();

  private:
    // Per-link framing state.
    struct Link {
        FgwChannelPtr         chan;
        bool                  outbound = false;  // true = to an Outport
        std::vector<zce_byte> buffer;            // partial-segment reassembly
    };

    zce::SmartPtr<IFgwChannel> makeInboundChannel();   // accepted from an Inport
    void dialOutports();                               // config.channels -> Outports

    void onLinkBytes(IFgwChannel* ch, const zce_byte* buf, zce_uint32 len);
    void onLinkClosed(IFgwChannel* ch);

    FgwChannelPtr findOutbound(zce_uint32 outport_id);
    FgwChannelPtr findInbound(zce_uint32 ingress_id);

    zce::SmartPtr<zce::Reactor>          reactor_;
    zce::TaskQueue*                      sync_queue_ = nullptr;
    FgwConfig                            config_;

    mutable zce::Mutex                   lock_;
    zce::SmartPtr<zce::Acceptor>         acceptor_;
    zce_uint32                           accepted_id_seq_ = 0x80000000u;

    std::map<IFgwChannel*, Link>         links_;
    std::map<zce_uint32, FgwChannelPtr>  outbound_by_outport_;   // outport_id -> link
    std::map<zce_uint32, FgwChannelPtr>  inbound_by_ingress_;    // ingress_id -> link
};

}  // namespace zfgw
