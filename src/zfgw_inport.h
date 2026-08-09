// ***************************************************************
//  zfgw_inport.h  — local (client-side) TCP entry point.
//
//  InportService listens on a local TCP socket (typically 127.0.0.1:1080
//  for SOCKS5 compatibility).  Every accepted client becomes a FgwSession
//  that forwards raw bytes to the OutportService through the aggregated
//  DataStream.
//
//  The Inport is deliberately protocol-agnostic: it does NOT parse the
//  SOCKS5 handshake locally.  Instead it passes the entire TCP byte
//  stream through to the Outport (which runs a SOCKS5 server on top of
//  the DataStream).  This keeps the Inport stateless and lets callers
//  use any SOCKS5-speaking client as-is.
//
//  Yongming Wang(wangym@gmail.com)
//  Copyright (C) 2026 - All Rights Reserved
// ***************************************************************
#pragma once

#include "zfgw_datastream.h"
#include <zce/zce_handler.h>
#include <zce/zce_sync.h>
#include <map>

namespace zfgw {

class InportService;

/// Per-client session created for every TCP connection accepted by
/// the InportService.  Implements ISessionHandler so it can receive
/// peer-originated bytes from the DataStream and push them back to
/// the local client socket.
class FgwSession : public ISessionHandler, virtual public zce::Object {
  public:
    FgwSession(InportService* inport, const DataStreamPtr& stream, zce_uint32 session_id);
    ~FgwSession() override;

    zce_uint32 sessionId() const { return session_id_; }

    void bindClientTcp(const zce::SmartPtr<zce::Tcp>& tcp);

    /// Called by the Tcp subclass every time the client sends bytes.
    void onClientBytes(const zce_byte* buf, zce_uint32 len);

    /// Called by the Tcp subclass when the local client disconnects.
    void onClientClose();

    void close();

    // ISessionHandler API — bytes coming back from the Outport.
    void onSessionOpen(DataStream* stream, zce_uint32 session_id) override {}
    void onSessionData(DataStream* stream, zce_uint32 session_id,
                       const zce_byte* buf, zce_uint32 len) override;
    void onSessionClose(DataStream* stream, zce_uint32 session_id) override;

    zce_uint64 bytesSent() const { return bytes_sent_; }
    zce_uint64 bytesRecv() const { return bytes_recv_; }
    zce_uint32 openTimestamp() const { return open_timestamp_; }
    std::string clientAddr() const { return client_addr_; }

  private:
    InportService*              inport_;
    DataStreamPtr               stream_;
    zce_uint32                  session_id_;
    zce::SmartPtr<zce::Tcp>     client_tcp_;
    std::string                 client_addr_;
    zce_uint32                  open_timestamp_ = 0;
    zce_uint64                  bytes_sent_ = 0;
    zce_uint64                  bytes_recv_ = 0;
    bool                        closed_ = false;
    mutable zce::Mutex          lock_;
};

using FgwSessionPtr = zce::SmartPtr<FgwSession>;

/// InportService — acceptor + session registry.
class InportService : public zce::Object {
  public:
    InportService(const zce::SmartPtr<zce::Reactor>& reactor, const DataStreamPtr& stream);
    ~InportService() override;

    int start(const std::string& listen_ip, zce_uint16 listen_port);
    void stop();

    FgwSessionPtr registerSession(const FgwSessionPtr& session);
    void removeSession(zce_uint32 session_id);

    std::vector<FgwSessionPtr> snapshotSessions() const;

    const DataStreamPtr& dataStream() const { return stream_; }

    void onChannelBytes(IFgwChannel* ch, const zce_byte* buf, zce_uint32 len) {
        stream_->onChannelBytes(ch, buf, len);
    }

  private:
    class InportTcp;            // zce::Tcp subclass tied to a FgwSession

    zce_uint32 allocateSessionId();

    zce::SmartPtr<zce::Reactor>          reactor_;
    DataStreamPtr                        stream_;
    zce::SmartPtr<zce::Acceptor>         acceptor_;
    mutable zce::Mutex                   lock_;
    std::map<zce_uint32, FgwSessionPtr>  sessions_;
    zce_uint32                           next_session_id_ = 1;
};

}  // namespace zfgw
