// ***************************************************************
//  zfgw_channel.h  — link-channel abstraction used by DataStream.
//
//  Each IFgwChannel represents a single long-lived point-to-point
//  transport (TCP socket, UTP socket, …).  Channels expose an
//  ordered-byte-stream API to the DataStream layer; DataStream is
//  responsible for framing individual segments on top of it.
//
//  Yongming Wang(wangym@gmail.com)
//  Copyright (C) 2026 - All Rights Reserved
// ***************************************************************
#pragma once

#include <zce/zce_handler.h>
#include <zce/zce_sync.h>
#include <zce/zce_object.h>
#include <zce/zce_reactor.h>
#include <zce/zce_types.h>
#include <atomic>
#include <functional>
#include <string>
#include "zfgw_proto.h"
#include "zfgw.h"

// Bridge the build-system option to the internal guard used throughout the
// UTP code paths. CMake's FGW_ENABLE_UTP defines FGW_USE_LIBUTP=1; the sources
// below are written against ZFGW_WITH_LIBUTP. Keep the two names in sync here
// so enabling the option actually compiles the libutp integration in.
#if defined(FGW_USE_LIBUTP) && (FGW_USE_LIBUTP)
#    define ZFGW_WITH_LIBUTP 1
#else
#    define ZFGW_WITH_LIBUTP 0
#endif

struct struct_utp_context;
struct UTPSocket;

namespace zfgw {

class DataStream;

/// Observed link quality.  DataStream uses these values in its
/// weighted link-selection algorithm.
struct LinkQuality {
    zce_uint32 rtt_ms = 0;
    double     loss_rate = 0.0;
    zce_uint64 bytes_sent = 0;
    zce_uint64 bytes_recv = 0;
    zce_uint32 send_segments = 0;
    zce_uint32 recv_segments = 0;
    zce_uint32 rexmit_segments = 0;
    zce_uint32 last_rx_tick = 0;
};

/// Interface implemented by every link-channel type.
///
/// A channel is owned by a DataStream (SmartPtr).  Bytes delivered
/// via onRecvBytes() must be the exact bytes the peer wrote; the
/// channel must NOT lose, reorder, or duplicate bytes within its
/// single stream.  (Cross-channel reordering is fine — DataStream
/// handles that via the SegmentHeader seq_num.)
class IFgwChannel : virtual public zce::Object {
    friend class DataStream;

  public:
    using ConnectedCallback = std::function<void(IFgwChannel*, int errcode)>;
    using ClosedCallback    = std::function<void(IFgwChannel*)>;
    using BytesCallback     = std::function<void(IFgwChannel*, const zce_byte*, zce_uint32)>;

  protected:
    zce_uint32        channel_id_ = 0;
    ZFGW_CHANNEL_KIND kind_ = ZFGW_CHANNEL_TCP;
    zce_uint32        priority_ = 100;
    bool              connected_ = false;
    /// True for channels created by an inbound listener (accepted) rather
    /// than dialed; accepted channels are not auto-reconnected and do not
    /// send the initiating Hello (the dialing peer does).
    bool              accepted_ = false;
    /// Set by the ChannelManager heartbeat check when the link has gone silent
    /// (TCP still open but no bytes for link_timeout). A stalled link is kept
    /// but excluded from live selection until traffic resumes. Atomic: written
    /// by the heartbeat/recv paths, read by selection on other threads.
    std::atomic<bool> stalled_{false};
    /// Peer identity learned from the FgwHello handshake (0 until received).
    zce_uint32        peer_channel_id_ = 0;
    zce_uint32        peer_ingress_id_ = 0;
    zce_uint32        peer_outport_id_ = 0;
    LinkQuality       quality_;
    mutable zce::Mutex quality_lock_;

    ConnectedCallback on_connected_;
    ClosedCallback    on_closed_;
    BytesCallback     on_bytes_;

  public:
    IFgwChannel(zce_uint32 id, ZFGW_CHANNEL_KIND kind, zce_uint32 priority = 100)
        : channel_id_(id), kind_(kind), priority_(priority) {}

    ~IFgwChannel() override = default;

    zce_uint32         channelId()  const { return channel_id_; }
    ZFGW_CHANNEL_KIND  kind()       const { return kind_; }
    zce_uint32         priority()   const { return priority_; }
    bool               isConnected() const { return connected_; }

    /// Liveness: connected AND not marked stalled by the heartbeat check.
    /// Selection uses this so a silent (GFW-stalled) link is skipped.
    bool               isLive()     const { return connected_ && !stalled_; }
    bool               isStalled()  const { return stalled_; }
    void               markStalled(bool v) { stalled_ = v; }
    zce_uint32         lastRxTick() const {
        zce::Guard<zce::Mutex> g(quality_lock_);
        return quality_.last_rx_tick;
    }

    bool               isAccepted() const { return accepted_; }
    void               markAccepted()     { accepted_ = true; }

    /// Peer identity, as reported in the FgwHello handshake.
    zce_uint32         peerChannelId() const { return peer_channel_id_; }
    zce_uint32         peerIngressId() const { return peer_ingress_id_; }
    zce_uint32         peerOutportId() const { return peer_outport_id_; }
    void setPeerIdentity(zce_uint32 peer_channel, zce_uint32 peer_ingress,
                         zce_uint32 peer_outport) {
        peer_channel_id_ = peer_channel;
        peer_ingress_id_ = peer_ingress;
        peer_outport_id_ = peer_outport;
    }

    LinkQuality quality() const {
        zce::Guard<zce::Mutex> g(quality_lock_);
        return quality_;
    }

    void setConnectedCallback(ConnectedCallback cb) { on_connected_ = std::move(cb); }
    void setClosedCallback(ClosedCallback cb)       { on_closed_ = std::move(cb); }
    void setBytesCallback(BytesCallback cb)         { on_bytes_ = std::move(cb); }

    /// Initiate (or restart) the underlying transport connection.
    virtual int connect(const FgwEndpoint& remote) = 0;

    /// Close the channel; idempotent.
    virtual void close() = 0;

    /// Enqueue a buffer for transmission.  Returns bytes accepted, or
    /// a negative ZFGW_ERRCODE on error.  The buffer is copied into
    /// the transport's internal queue, so the caller may free it
    /// immediately after return.
    virtual int sendBytes(const zce_byte* buf, zce_uint32 len) = 0;

    /// Effective MSS for segment sizing heuristics.  Default returns
    /// 1200 which works for every sane MTU.
    virtual int effectiveMss() const { return 1200; }

  protected:
    void markConnected(bool v) {
        connected_ = v;
        if (v) {
            // Baseline the receive clock at connect time so a link that comes
            // up but is blackholed (never receives) still stalls after a full
            // link_timeout, instead of being treated as forever-fresh.
            zce::Guard<zce::Mutex> g(quality_lock_);
            quality_.last_rx_tick = (zce_uint32)zce_tick();
        }
        if (on_connected_) on_connected_(this, v ? 0 : ZFGW_ERRCODE_CHANNELFAIL);
    }
    void markClosed() {
        connected_ = false;
        if (on_closed_) on_closed_(this);
    }
    void addSentBytes(zce_uint32 n) {
        zce::Guard<zce::Mutex> g(quality_lock_);
        quality_.bytes_sent += n;
    }
    void addRecvBytes(zce_uint32 n) {
        // Traffic resumed: clear any stall immediately so the channel rejoins
        // live selection before the next heartbeat tick — otherwise a
        // synchronous reply to this very frame could hit NOCHANNEL.
        stalled_.store(false, std::memory_order_relaxed);
        zce::Guard<zce::Mutex> g(quality_lock_);
        quality_.bytes_recv += n;
        quality_.last_rx_tick = (zce_uint32)zce_tick();
    }
    void noteSegmentSent()    { zce::Guard<zce::Mutex> g(quality_lock_); ++quality_.send_segments; }
    void noteSegmentRecv()    { zce::Guard<zce::Mutex> g(quality_lock_); ++quality_.recv_segments; }
    void noteSegmentRexmit()  { zce::Guard<zce::Mutex> g(quality_lock_); ++quality_.rexmit_segments; }
    void setRttMs(zce_uint32 v) {
        zce::Guard<zce::Mutex> g(quality_lock_);
        quality_.rtt_ms = v;
    }
};

using FgwChannelPtr = zce::SmartPtr<IFgwChannel>;

/// TCP-backed link channel.  Delegates socket I/O to zce::Tcp +
/// zce::Connector.  `zce::Tcp` already handles asynchronous write
/// queues and prioritised send, so this class is mostly a bridge
/// that forwards callbacks into the `IFgwChannel` interface.
class FgwTcpChannel : public IFgwChannel {
  public:
    FgwTcpChannel(const zce::SmartPtr<zce::Reactor>& reactor, zce_uint32 id,
                  zce_uint32 priority = 100);

    ~FgwTcpChannel() override;

    int  connect(const FgwEndpoint& remote) override;
    void close() override;
    int  sendBytes(const zce_byte* buf, zce_uint32 len) override;

    /// Wrap an already-accepted zce::Tcp (used by OutportService when a
    /// remote Inport dials in).
    int adoptAcceptedTcp(const zce::SmartPtr<zce::Tcp>& tcp_ptr);

    /// Create the zce::Tcp handler for an inbound (accepted) channel and
    /// return it for a zce::Acceptor factory to adopt. The channel keeps a
    /// SmartPtr to the handler; bytes read on it flow through the normal
    /// BytesCallback path. Marks the channel accepted.
    zce::Tcp* createAcceptedHandler();

  private:
    class TcpHandler;  // zce::Tcp subclass that fans out bytes

    zce::SmartPtr<zce::Reactor>     reactor_;
    zce::SmartPtr<zce::Tcp>         tcp_ptr_;
    zce::SmartPtr<zce::Connector>   connector_;
    FgwEndpoint                     remote_{};
    mutable zce::Mutex              lock_;
};

/// UTP-backed link channel — UDP + libutp congestion control.
///
/// A single utp_context is created per reactor and cached inside the
/// ChannelManager (see zfgw_channel_manager.h).  Each FgwUtpChannel
/// owns a single utp_socket that belongs to that context.  The actual
/// UDP socket (zce::Udp) is shared by all UTP channels that talk to
/// the same local bind address.
class FgwUtpContextHolder;       // fwd

class FgwUtpChannel : public IFgwChannel {
  public:
    FgwUtpChannel(const zce::SmartPtr<FgwUtpContextHolder>& ctx, zce_uint32 id,
                  zce_uint32 priority = 100);

    ~FgwUtpChannel() override;

    int  connect(const FgwEndpoint& remote) override;
    void close() override;
    int  sendBytes(const zce_byte* buf, zce_uint32 len) override;

    /// Invoked by the context holder when libutp delivers bytes for
    /// this socket.  Public so FgwUtpContextHolder can call it.
    void deliverBytes(const zce_byte* buf, zce_uint32 len);

    /// Invoked when libutp signals CONNECT / WRITABLE / EOF / ERROR.
    void onUtpState(int state);
    void onUtpError(int error_code);

    struct UTPSocket* utpSocket() const { return utp_socket_; }

  private:
    zce::SmartPtr<FgwUtpContextHolder> ctx_holder_;
    struct UTPSocket*                  utp_socket_ = nullptr;
    FgwEndpoint                        remote_{};
    mutable zce::Mutex                 lock_;
};

}  // namespace zfgw
