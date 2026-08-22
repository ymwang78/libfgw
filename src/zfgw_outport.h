// ***************************************************************
//  zfgw_outport.h  — remote (server-side) SOCKS5 terminator.
//
//  The OutportService consumes logical sessions that arrive over the
//  DataStream from a peer Inport.  Every session is expected to carry
//  a standard SOCKS5 handshake followed by the actual payload; the
//  Outport speaks SOCKS5 itself and, on successful negotiation, dials
//  out to the requested target with a zce::Tcp.  Egress stability comes
//  from session affinity — a session is pinned to one Outport for its
//  lifetime — while the source address itself is chosen by the host's
//  routing table.
//
//  Yongming Wang(wangym@gmail.com)
//  Copyright (C) 2026 - All Rights Reserved
// ***************************************************************
#pragma once

#include "zfgw_datastream.h"
#include <zce/zce_handler.h>
#include <zce/zce_sync.h>
#include <map>
#include <string>
#include <utility>
#include <vector>

namespace zfgw {

class OutportService;

/// One SOCKS5 negotiation + relay per remote session.
class FgwRelaySession : public ISessionHandler, virtual public zce::Object {
  public:
    enum State { ST_AWAIT_METHODS, ST_AWAIT_REQUEST, ST_CONNECTING, ST_RELAYING, ST_CLOSED };

    FgwRelaySession(OutportService* outport, const DataStreamPtr& stream,
                    zce_uint32 peer_ingress, zce_uint32 session_id);
    ~FgwRelaySession() override;

    zce_uint32 sessionId() const { return session_id_; }
    zce_uint32 peerIngressId() const { return peer_ingress_id_; }

    // ISessionHandler API — bytes coming in from the peer Inport.
    void onSessionOpen(DataStream* stream, zce_uint32 session_id) override;
    void onSessionData(DataStream* stream, zce_uint32 session_id,
                       const zce_byte* buf, zce_uint32 len) override;
    void onSessionClose(DataStream* stream, zce_uint32 session_id) override;

    // Called by the outbound TCP handler.
    void onRemoteOpen();
    void onRemoteBytes(const zce_byte* buf, zce_uint32 len);
    void onRemoteClose();

    void close();

    const std::string& targetAddr() const { return target_addr_; }
    zce_uint64 bytesSent() const { return bytes_sent_; }
    zce_uint64 bytesRecv() const { return bytes_recv_; }
    zce_uint32 openTimestamp() const { return open_timestamp_; }

  private:
    void feedRx(const zce_byte* buf, zce_uint32 len);
    int  tryParseMethods();
    int  tryParseRequest();
    void writeReply(zce_byte rep);
    void startConnect(const std::string& host, zce_uint16 port);
    void sendToPeer(const zce_byte* buf, zce_uint32 len);

    OutportService*            outport_;
    DataStreamPtr              stream_;
    zce_uint32                 peer_ingress_id_ = 0;
    zce_uint32                 session_id_;
    State                      state_ = ST_AWAIT_METHODS;
    std::vector<zce_byte>      rx_buffer_;
    zce::SmartPtr<zce::Tcp>    remote_tcp_;
    zce::SmartPtr<zce::Connector> remote_conn_;
    std::string                target_addr_;
    zce_uint32                 open_timestamp_ = 0;
    zce_uint64                 bytes_sent_ = 0;
    zce_uint64                 bytes_recv_ = 0;
    mutable zce::Mutex         lock_;
};

using FgwRelaySessionPtr = zce::SmartPtr<FgwRelaySession>;

/// OutportService — owns the DataStream's server side + its per-session relays.
class OutportService : public zce::Object {
  public:
    OutportService(const zce::SmartPtr<zce::Reactor>& reactor, const DataStreamPtr& stream);
    ~OutportService() override;

    int start();
    void stop();

    using RelaySessionKey = std::pair<zce_uint32, zce_uint32>;  // (peer_ingress, session_id)

    /// Lookup or create the relay session for a given ingress + session id.
    FgwRelaySessionPtr ensureSession(zce_uint32 peer_ingress, zce_uint32 session_id);

    void removeSession(zce_uint32 peer_ingress, zce_uint32 session_id);

    std::vector<FgwRelaySessionPtr> snapshotSessions() const;

    const zce::SmartPtr<zce::Reactor>& reactor() const { return reactor_; }
    const DataStreamPtr& dataStream() const { return stream_; }

    /// Called by the server-side channels (accepted by a listener or
    /// connected from a client) to feed bytes into the DataStream.
    void onChannelBytes(IFgwChannel* ch, const zce_byte* buf, zce_uint32 len) {
        stream_->onChannelBytes(ch, buf, len);
    }

    /// A small integration hook: when the DataStream sees a previously
    /// unknown session id (Inport started without a prior openSession
    /// on the Outport), it installs a relay session automatically.
    /// The VM plumbs this via dataStreamHook().
    class UnknownSessionHook;
    void dataStreamHook();

  private:
    zce::SmartPtr<zce::Reactor>                 reactor_;
    DataStreamPtr                               stream_;
    mutable zce::Mutex                          lock_;
    std::map<RelaySessionKey, FgwRelaySessionPtr> sessions_;
};

}  // namespace zfgw
