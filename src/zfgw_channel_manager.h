// ***************************************************************
//  zfgw_channel_manager.h  — pool of link channels + link selector.
//  Yongming Wang(wangym@gmail.com)
//  Copyright (C) 2026 - All Rights Reserved
// ***************************************************************
#pragma once

#include "zfgw_channel.h"
#include <zce/zce_handler.h>
#include <zce/zce_sync.h>
#include <zce/zce_reactor.h>
#include <zce/zce_task_queue.h>
#include <zce/zce_timer.h>
#include <functional>
#include <map>
#include <memory>
#include <vector>

struct struct_utp_context;

namespace zfgw {

class DataStream;

// ─── UDP + libutp context holder ────────────────────────────────────────────
//
// Owns a single utp_context and the zce::Udp socket that feeds packets
// into it.  Shared by every FgwUtpChannel on the same reactor.
// Exposed in this header so the channel manager can construct one per
// bind address.
class FgwUtpContextHolder : public zce::Object {
  public:
    FgwUtpContextHolder(const zce::SmartPtr<zce::Reactor>& reactor, zce::TaskQueue* sync_queue);
    ~FgwUtpContextHolder() override;

    int start(const std::string& bind_ip, zce_uint16 bind_port);
    void stop();

    int connectChannel(FgwUtpChannel* channel, const FgwEndpoint& remote);
    void closeChannel(FgwUtpChannel* channel);

    struct struct_utp_context* context() const { return utp_ctx_; }
    const zce::SmartPtr<zce::Reactor>& reactor() const { return reactor_; }

  private:
    class UdpHandler;

    zce::SmartPtr<zce::Reactor> reactor_;
    zce::TaskQueue*             sync_queue_ = nullptr;
    zce::SmartPtr<UdpHandler>   udp_ptr_;
    struct struct_utp_context*  utp_ctx_ = nullptr;
    zce::SmartPtr<zce::Timer>   tick_timer_;
    mutable zce::Mutex          lock_;

    void onTick();
};

// ─── Link selector ──────────────────────────────────────────────────────────
//
// Computes, for each outbound segment, the set of channels that should
// transmit it.  The DataStream calls select() every time it wants to
// publish a segment.
class LinkSelector {
  public:
    LinkSelector() = default;

    void setMode(ZFGW_MULTIPATH_MODE mode) { mode_ = mode; }

    /// Returns the list of channel-ids (ordered by descending weight)
    /// that should receive the next outgoing segment.  The caller
    /// should transmit on each one in order, but may stop early when
    /// it believes one copy is sufficient.
    std::vector<zce_uint32> select(const std::vector<FgwChannelPtr>& pool) const;

  private:
    ZFGW_MULTIPATH_MODE mode_ = ZFGW_MULTIPATH_WEIGHTED;
};

// ─── Channel manager ────────────────────────────────────────────────────────
class ChannelManager : public zce::Object {
  public:
    using ChannelOpenedCb = std::function<void(ChannelManager*, IFgwChannel*)>;
    using ChannelClosedCb = std::function<void(ChannelManager*, IFgwChannel*)>;

    /// sync_queue: reactor timer / delayed work is marshalled to this queue (HostVM Machine).
    ChannelManager(const zce::SmartPtr<zce::Reactor>& reactor, zce::TaskQueue* sync_queue);
    ~ChannelManager() override;

    void setCallbacks(ChannelOpenedCb opened, ChannelClosedCb closed) {
        on_opened_ = std::move(opened);
        on_closed_ = std::move(closed);
    }

    int start(const FgwConfig& cfg);
    void stop();

    int addChannel(const FgwChannelConfig& cfg);
    int removeChannel(zce_uint32 channel_id);

    /// Start accepting inbound link connections (Transit / Outport side).
    /// Each accepted socket becomes an inbound FgwTcpChannel whose bytes are
    /// fanned out to the registered DataStream(s). Idempotent-guarded.
    int  startLinkListener(const std::string& bind_ip, zce_uint16 bind_port);
    void stopLinkListener();

    /// Get (or create) the shared UTP context bound to (bind_ip, bind_port).
    /// For clients we usually bind to the wildcard address with an
    /// ephemeral port.
    zce::SmartPtr<FgwUtpContextHolder> sharedUtpContext(const std::string& bind_ip = "",
                                                        zce_uint16 bind_port = 0);

    std::vector<FgwChannelPtr> liveChannels() const;

    FgwChannelPtr findChannel(zce_uint32 channel_id) const;

    LinkSelector& selector() { return selector_; }

    void collectLinkQualities(std::vector<FgwLinkQuality>& out) const;

    /// Register a DataStream to receive **all** inbound bytes from every live
    /// channel (fan-out).  Safe to call multiple times with distinct streams.
    void addBytesSubscriber(DataStream* stream);
    void removeBytesSubscriber(DataStream* stream);

  private:
    void dispatchChannelBytes(IFgwChannel* ch, const zce_byte* buf, zce_uint32 len);
    void wireChannelRecv(IFgwChannel* chan);

    void onChannelConnected(IFgwChannel* ch, int errcode);
    void onChannelClosed(IFgwChannel* ch);
    void scheduleReconnect(zce_uint32 channel_id, FgwEndpoint remote, zce_uint32 kind,
                           zce_uint32 priority, unsigned backoff_seconds);

    /// Build an inbound (accepted) TCP channel, register it, and return it so
    /// its handler can be handed to the zce::Acceptor factory.
    zce::SmartPtr<IFgwChannel> makeAcceptedChannel();
    /// Send the FgwHello handshake on a freshly-connected dialed channel.
    void sendHello(IFgwChannel* ch);

    zce::SmartPtr<zce::Reactor>                    reactor_;
    zce::TaskQueue*                                sync_queue_ = nullptr;
    mutable zce::Mutex                             lock_;
    std::map<zce_uint32, FgwChannelPtr>            channels_;
    zce::SmartPtr<FgwUtpContextHolder>             shared_utp_;
    zce::SmartPtr<zce::Timer>                      heartbeat_timer_;
    zce::SmartPtr<zce::Acceptor>                   link_acceptor_;
    /// Id space for accepted channels (high bit set) so they never collide
    /// with operator-configured (dialed) channel ids.
    zce_uint32                                     accepted_id_seq_ = 0x80000000u;
    LinkSelector                                   selector_;
    FgwConfig                                      config_{};

    ChannelOpenedCb  on_opened_;
    ChannelClosedCb  on_closed_;

    std::vector<DataStream*> bytes_subscribers_;
};

}  // namespace zfgw
