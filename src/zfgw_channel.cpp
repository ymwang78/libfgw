// ***************************************************************
//  zfgw_channel.cpp
//  Yongming Wang(wangym@gmail.com)
//  Copyright (C) 2026 - All Rights Reserved
// ***************************************************************
#include "zfgw_channel.h"
#include "zfgw_channel_manager.h"
#include "zfgw.h"

#include <zce/zce_log.h>
#include <zce/zce_timer.h>
#include <zce/zce_mbpool.h>
#include <cstring>

#if ZFGW_WITH_LIBUTP
#    include "utp.h"
#endif

namespace zfgw {

// ─── FgwTcpChannel::TcpHandler ───────────────────────────────────────────────
//
// Subclass of zce::Tcp.  We override the four virtuals that matter:
//   on_open, on_close, on_read_data, on_written.
// Incoming bytes are forwarded to the owner channel via a raw pointer
// (protected by a lock on close).
class FgwTcpChannel::TcpHandler : public zce::Tcp {
  public:
    TcpHandler(const zce::SmartPtr<zce::Reactor>& reactor, FgwTcpChannel* owner)
        : zce::Tcp(reactor, 16), owner_(owner) {}

    ~TcpHandler() override = default;

    void clearOwner() { owner_ = nullptr; }

    void on_open(bool passive, const zce_sockaddr_t& remote) override {
        zce::Tcp::on_open(passive, remote);  // starts read loop
        if (owner_) {
            owner_->markConnected(true);
        }
    }

    void on_close() override {
        zce::Tcp::on_close();
        if (owner_) {
            owner_->markClosed();
        }
    }

    void on_read_data(zce_byte* buf, zce_uint32 len) override {
        if (owner_) {
            owner_->addRecvBytes(len);
            if (owner_->on_bytes_) {
                owner_->on_bytes_(owner_, buf, len);
            }
        }
    }

    void on_written(void* req, int status) override {
        // Let zce::Tcp process its send queue first.
        zce::Tcp::on_written(req, status);
    }

  private:
    FgwTcpChannel* owner_;
};

// ─── FgwTcpChannel ───────────────────────────────────────────────────────────
FgwTcpChannel::FgwTcpChannel(const zce::SmartPtr<zce::Reactor>& reactor, zce_uint32 id,
                             zce_uint32 priority)
    : IFgwChannel(id, ZFGW_CHANNEL_TCP, priority), reactor_(reactor) {}

FgwTcpChannel::~FgwTcpChannel() {
    close();
}

int FgwTcpChannel::connect(const FgwEndpoint& remote) {
    zce::Guard<zce::Mutex> g(lock_);
    close();
    remote_ = remote;

    tcp_ptr_ = zce::SmartPtr<zce::Tcp>(new TcpHandler(reactor_, this));
    connector_ = new zce::Connector(tcp_ptr_, remote.host, remote.port, /*timeout*/ 10);
    int ret = connector_->start_connect();
    if (ret < 0) {
        ZCE_ERROR((ZLOG_ERROR, "fgw: tcp connect %s:%u failed, ret=0x%x",
                   remote.host.c_str(), (unsigned)remote.port, ret));
    }
    return ret;
}

void FgwTcpChannel::close() {
    zce::Guard<zce::Mutex> g(lock_);
    if (tcp_ptr_) {
        if (auto* th = dynamic_cast<TcpHandler*>(tcp_ptr_.get())) {
            th->clearOwner();
        }
        tcp_ptr_->close();
        tcp_ptr_ = nullptr;
    }
    connector_ = nullptr;
    connected_ = false;
}

int FgwTcpChannel::sendBytes(const zce_byte* buf, zce_uint32 len) {
    if (!tcp_ptr_) return ZFGW_ERRCODE_NOTRUNNING;
    if (!connected_) return ZFGW_ERRCODE_NOCHANNEL;
    if (len == 0) return 0;

    zce::RefBlock dblock;
    ZCE_MBACQUIRE(dblock, (int)len);
    if ((int)dblock.space() < (int)len) return ZFGW_ERRCODE_MEMORY;
    std::memcpy(dblock.wr_ptr_cow(), buf, len);
    dblock.wr_ptr((int)len);

    int ret = tcp_ptr_->write(dblock, zce::IStream::ERV_ISTREAM_DEFAULT);
    if (ret >= 0) {
        addSentBytes(len);
    }
    return ret < 0 ? ret : (int)len;
}

int FgwTcpChannel::adoptAcceptedTcp(const zce::SmartPtr<zce::Tcp>& /*tcp_ptr*/) {
    // Accepted channels use a distinct TcpHandler path — the Acceptor
    // constructs a TcpHandler directly via its factory lambda, so this
    // overload is reserved for future wrapping of pre-existing sockets.
    return ZFGW_ERRCODE_INVALIDCONFIG;
}

// ─── FgwUtpChannel ───────────────────────────────────────────────────────────
//
// The libutp integration is kept behind ZFGW_WITH_LIBUTP so this library
// still builds on platforms where libutp has not been vendored in.  When
// the flag is off the channel is a no-op stub that fails to connect and
// answers "not supported" to every send.
//
// The actual callback plumbing is described in detail in
// libfgw/docs/design_fgw.md §3.2.1.
FgwUtpChannel::FgwUtpChannel(const zce::SmartPtr<FgwUtpContextHolder>& ctx, zce_uint32 id,
                             zce_uint32 priority)
    : IFgwChannel(id, ZFGW_CHANNEL_UTP, priority), ctx_holder_(ctx) {}

FgwUtpChannel::~FgwUtpChannel() {
    close();
}

int FgwUtpChannel::connect(const FgwEndpoint& remote) {
    zce::Guard<zce::Mutex> g(lock_);
    remote_ = remote;
#if ZFGW_WITH_LIBUTP
    if (!ctx_holder_) return ZFGW_ERRCODE_INVALIDCONFIG;
    return ctx_holder_->connectChannel(this, remote);
#else
    (void)remote;
    ZCE_ERROR((ZLOG_ERROR, "fgw: libutp not compiled in, channel %u cannot connect",
               channel_id_));
    return ZFGW_ERRCODE_INVALIDCONFIG;
#endif
}

void FgwUtpChannel::close() {
    zce::Guard<zce::Mutex> g(lock_);
#if ZFGW_WITH_LIBUTP
    if (ctx_holder_ && utp_socket_) {
        ctx_holder_->closeChannel(this);
    }
#endif
    utp_socket_ = nullptr;
    connected_ = false;
}

int FgwUtpChannel::sendBytes(const zce_byte* buf, zce_uint32 len) {
#if ZFGW_WITH_LIBUTP
    if (!utp_socket_) return ZFGW_ERRCODE_NOCHANNEL;
    if (len == 0) return 0;
    size_t sent = utp_write(utp_socket_, const_cast<zce_byte*>(buf), len);
    if (sent == 0) return ZFGW_ERRCODE_BUSY;
    addSentBytes((zce_uint32)sent);
    return (int)sent;
#else
    (void)buf; (void)len;
    return ZFGW_ERRCODE_INVALIDCONFIG;
#endif
}

void FgwUtpChannel::deliverBytes(const zce_byte* buf, zce_uint32 len) {
    addRecvBytes(len);
    if (on_bytes_) on_bytes_(this, buf, len);
}

void FgwUtpChannel::onUtpState(int state) {
#if ZFGW_WITH_LIBUTP
    switch (state) {
        case UTP_STATE_CONNECT:
        case UTP_STATE_WRITABLE:
            markConnected(true);
            break;
        case UTP_STATE_EOF:
        case UTP_STATE_DESTROYING:
            markClosed();
            break;
    }
#else
    (void)state;
#endif
}

void FgwUtpChannel::onUtpError(int /*error_code*/) {
    markClosed();
}

}  // namespace zfgw
