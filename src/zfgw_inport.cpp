// ***************************************************************
//  zfgw_inport.cpp
//  Yongming Wang(wangym@gmail.com)
//  Copyright (C) 2026 - All Rights Reserved
// ***************************************************************
#include "zfgw_inport.h"
#include "zfgw.h"
#include <zce/zce_log.h>
#include <zce/zce_api.h>
#include <zce/zce_mbpool.h>
#include <cstring>
#include <ctime>

namespace zfgw {

// ─── FgwSession ─────────────────────────────────────────────────────────────
FgwSession::FgwSession(InportService* inport, const DataStreamPtr& stream,
                       zce_uint32 session_id)
    : inport_(inport), stream_(stream), session_id_(session_id) {
    open_timestamp_ = (zce_uint32)std::time(nullptr);
}

FgwSession::~FgwSession() = default;

void FgwSession::bindClientTcp(const zce::SmartPtr<zce::Tcp>& tcp) {
    zce::Guard<zce::Mutex> g(lock_);
    client_tcp_ = tcp;
    if (tcp) {
        char addr_buf[128] = {0};
        const auto& ra = tcp->remote_addr();
        zce_inet_ntoa(addr_buf, (unsigned)sizeof(addr_buf), &ra, false);
        client_addr_ = addr_buf;
    }
}

void FgwSession::onClientBytes(const zce_byte* buf, zce_uint32 len) {
    if (closed_) return;
    bytes_recv_ += len;
    stream_->sendSessionData(session_id_, buf, len);
}

void FgwSession::onClientClose() {
    close();
}

void FgwSession::close() {
    {
        zce::Guard<zce::Mutex> g(lock_);
        if (closed_) return;
        closed_ = true;
    }
    if (stream_) {
        stream_->closeSession(session_id_);
    }
    if (client_tcp_) {
        client_tcp_->close();
        client_tcp_ = nullptr;
    }
    if (inport_) {
        inport_->removeSession(session_id_);
    }
}

void FgwSession::onSessionData(DataStream* /*stream*/, zce_uint32 /*session_id*/,
                               const zce_byte* buf, zce_uint32 len) {
    if (!client_tcp_ || closed_) return;

    zce::RefBlock dblock;
    ZCE_MBACQUIRE(dblock, (int)len);
    if ((int)dblock.space() < (int)len) return;
    std::memcpy(dblock.wr_ptr_cow(), buf, len);
    dblock.wr_ptr((int)len);
    client_tcp_->write(dblock, zce::IStream::ERV_ISTREAM_DEFAULT);
    bytes_sent_ += len;
}

void FgwSession::onSessionClose(DataStream* /*stream*/, zce_uint32 /*session_id*/) {
    close();
}

// ─── InportService::InportTcp ───────────────────────────────────────────────
class InportService::InportTcp : public zce::Tcp {
  public:
    InportTcp(const zce::SmartPtr<zce::Reactor>& reactor, InportService* inport)
        : zce::Tcp(reactor, 16), inport_(inport) {}

    void attachSession(const FgwSessionPtr& session) {
        session_ = session;
        session_->bindClientTcp(zce::SmartPtr<zce::Tcp>(this));
    }

    void on_open(bool passive, const zce_sockaddr_t& remote) override {
        zce::Tcp::on_open(passive, remote);
    }

    void on_read_data(zce_byte* buf, zce_uint32 len) override {
        if (session_) session_->onClientBytes(buf, len);
    }

    void on_close() override {
        zce::Tcp::on_close();
        if (session_) {
            session_->onClientClose();
            session_ = nullptr;
        }
    }

  private:
    InportService* inport_;
    FgwSessionPtr  session_;
};

// ─── InportService ──────────────────────────────────────────────────────────
InportService::InportService(const zce::SmartPtr<zce::Reactor>& reactor,
                             const DataStreamPtr& stream)
    : reactor_(reactor), stream_(stream) {}

InportService::~InportService() {
    stop();
}

zce_uint32 InportService::allocateSessionId() {
    zce::Guard<zce::Mutex> g(lock_);
    return next_session_id_++;
}

int InportService::start(const std::string& listen_ip, zce_uint16 listen_port) {
    zce::Guard<zce::Mutex> g(lock_);
    if (acceptor_) return ZFGW_ERRCODE_BUSY;

    acceptor_ = new zce::Acceptor(reactor_, [this]() -> zce::Tcp* {
        // Factory invoked by zce::Acceptor for each new connection.
        auto tcp = new InportTcp(reactor_, this);
        zce_uint32 sid = allocateSessionId();
        FgwSessionPtr session(new FgwSession(this, stream_, sid));
        tcp->attachSession(session);
        sessions_[sid] = session;
        stream_->openSession(sid, session.get());
        return tcp;
    });

    int ret = acceptor_->listen(listen_ip.empty() ? "0.0.0.0" : listen_ip.c_str(), listen_port);
    if (ret < 0) {
        ZCE_ERROR((ZLOG_ERROR, "fgw: inport listen %s:%u failed 0x%x",
                   listen_ip.c_str(), (unsigned)listen_port, ret));
        acceptor_ = nullptr;
        return ZFGW_ERRCODE_LISTENFAIL;
    }
    return 0;
}

void InportService::stop() {
    zce::SmartPtr<zce::Acceptor> acc;
    std::map<zce_uint32, FgwSessionPtr> copy;
    {
        zce::Guard<zce::Mutex> g(lock_);
        acc.swap(acceptor_);
        copy.swap(sessions_);
    }
    if (acc) acc->close();
    for (auto& kv : copy) {
        if (kv.second) kv.second->close();
    }
}

FgwSessionPtr InportService::registerSession(const FgwSessionPtr& session) {
    zce::Guard<zce::Mutex> g(lock_);
    sessions_[session->sessionId()] = session;
    return session;
}

void InportService::removeSession(zce_uint32 session_id) {
    zce::Guard<zce::Mutex> g(lock_);
    sessions_.erase(session_id);
}

std::vector<FgwSessionPtr> InportService::snapshotSessions() const {
    zce::Guard<zce::Mutex> g(lock_);
    std::vector<FgwSessionPtr> out;
    out.reserve(sessions_.size());
    for (const auto& kv : sessions_) {
        if (kv.second) out.push_back(kv.second);
    }
    return out;
}

}  // namespace zfgw
