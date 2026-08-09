// ***************************************************************
//  zfgw_outport.cpp
//  Yongming Wang(wangym@gmail.com)
//  Copyright (C) 2026 - All Rights Reserved
// ***************************************************************
#include "zfgw_outport.h"
#include "zfgw.h"
#include <zce/zce_log.h>
#include <zce/zce_api.h>
#include <zce/zce_mbpool.h>
#include <cstring>
#include <ctime>

namespace zfgw {

namespace {
// SOCKS5 constants.
constexpr zce_byte SOCKS5_VER             = 0x05;
constexpr zce_byte SOCKS5_CMD_CONNECT     = 0x01;
constexpr zce_byte SOCKS5_ATYP_IPV4       = 0x01;
constexpr zce_byte SOCKS5_ATYP_DOMAIN     = 0x03;
constexpr zce_byte SOCKS5_ATYP_IPV6       = 0x04;
constexpr zce_byte SOCKS5_REP_OK          = 0x00;
constexpr zce_byte SOCKS5_REP_GENERAL     = 0x01;
constexpr zce_byte SOCKS5_REP_NETUNREACH  = 0x03;
constexpr zce_byte SOCKS5_REP_HOSTUNREACH = 0x04;
constexpr zce_byte SOCKS5_REP_CMDNOTSUP   = 0x07;
}  // namespace

// ─── Outbound TCP handler (connects to SOCKS5 target) ────────────────────────
class OutboundTcp : public zce::Tcp {
  public:
    OutboundTcp(const zce::SmartPtr<zce::Reactor>& reactor, FgwRelaySession* owner)
        : zce::Tcp(reactor, 16), owner_(owner) {}

    void clearOwner() { owner_ = nullptr; }

    void on_open(bool passive, const zce_sockaddr_t& remote) override {
        zce::Tcp::on_open(passive, remote);
        if (owner_) owner_->onRemoteOpen();
    }

    void on_read_data(zce_byte* buf, zce_uint32 len) override {
        if (owner_) owner_->onRemoteBytes(buf, len);
    }

    void on_close() override {
        zce::Tcp::on_close();
        if (owner_) owner_->onRemoteClose();
    }

  private:
    FgwRelaySession* owner_;
};

// ─── FgwRelaySession ─────────────────────────────────────────────────────────
FgwRelaySession::FgwRelaySession(OutportService* outport, const DataStreamPtr& stream,
                                   zce_uint32 peer_ingress, zce_uint32 session_id)
    : outport_(outport), stream_(stream), peer_ingress_id_(peer_ingress), session_id_(session_id) {
    open_timestamp_ = (zce_uint32)std::time(nullptr);
}

FgwRelaySession::~FgwRelaySession() = default;

void FgwRelaySession::onSessionOpen(DataStream* /*stream*/, zce_uint32 /*session_id*/) {
    // Nothing to do until the peer sends SOCKS5 handshake bytes.
}

void FgwRelaySession::onSessionData(DataStream* /*stream*/, zce_uint32 /*session_id*/,
                                    const zce_byte* buf, zce_uint32 len) {
    feedRx(buf, len);
}

void FgwRelaySession::onSessionClose(DataStream* /*stream*/, zce_uint32 /*session_id*/) {
    close();
}

void FgwRelaySession::onRemoteOpen() {
    writeReply(SOCKS5_REP_OK);
    {
        zce::Guard<zce::Mutex> g(lock_);
        state_ = ST_RELAYING;
    }

    // Flush any bytes that arrived while we were still connecting.
    std::vector<zce_byte> leftover;
    {
        zce::Guard<zce::Mutex> g(lock_);
        leftover.swap(rx_buffer_);
    }
    if (!leftover.empty() && remote_tcp_) {
        zce::RefBlock dblock;
        ZCE_MBACQUIRE(dblock, (int)leftover.size());
        if ((int)dblock.space() >= (int)leftover.size()) {
            std::memcpy(dblock.wr_ptr_cow(), leftover.data(), leftover.size());
            dblock.wr_ptr((int)leftover.size());
            remote_tcp_->write(dblock, zce::IStream::ERV_ISTREAM_DEFAULT);
        }
    }
}

void FgwRelaySession::onRemoteBytes(const zce_byte* buf, zce_uint32 len) {
    bytes_recv_ += len;
    sendToPeer(buf, len);
}

void FgwRelaySession::onRemoteClose() {
    close();
}

void FgwRelaySession::close() {
    {
        zce::Guard<zce::Mutex> g(lock_);
        if (state_ == ST_CLOSED) return;
        state_ = ST_CLOSED;
    }
    if (remote_tcp_) {
        if (auto raw = dynamic_cast<OutboundTcp*>(remote_tcp_.get())) {
            raw->clearOwner();
        }
        remote_tcp_->close();
        remote_tcp_ = nullptr;
    }
    if (stream_) {
        stream_->closeSession(session_id_, peer_ingress_id_);
    }
    if (outport_) {
        outport_->removeSession(peer_ingress_id_, session_id_);
    }
}

void FgwRelaySession::feedRx(const zce_byte* buf, zce_uint32 len) {
    State st;
    {
        zce::Guard<zce::Mutex> g(lock_);
        rx_buffer_.insert(rx_buffer_.end(), buf, buf + len);
        bytes_recv_ += 0;  // counted separately when relaying
        st = state_;
    }

    while (true) {
        if (st == ST_AWAIT_METHODS) {
            int ret = tryParseMethods();
            if (ret <= 0) break;
        } else if (st == ST_AWAIT_REQUEST) {
            int ret = tryParseRequest();
            if (ret <= 0) break;
        } else if (st == ST_RELAYING) {
            std::vector<zce_byte> data;
            {
                zce::Guard<zce::Mutex> g(lock_);
                data.swap(rx_buffer_);
            }
            if (!data.empty() && remote_tcp_) {
                zce::RefBlock dblock;
                ZCE_MBACQUIRE(dblock, (int)data.size());
                if ((int)dblock.space() >= (int)data.size()) {
                    std::memcpy(dblock.wr_ptr_cow(), data.data(), data.size());
                    dblock.wr_ptr((int)data.size());
                    remote_tcp_->write(dblock, zce::IStream::ERV_ISTREAM_DEFAULT);
                    bytes_sent_ += data.size();
                }
            }
            break;
        } else {
            break;
        }
        zce::Guard<zce::Mutex> g(lock_);
        st = state_;
    }
}

int FgwRelaySession::tryParseMethods() {
    zce::Guard<zce::Mutex> g(lock_);
    if (rx_buffer_.size() < 2) return 0;
    if (rx_buffer_[0] != SOCKS5_VER) {
        ZCE_ERROR((ZLOG_WARNI, "fgw: session %u bad SOCKS version %u", session_id_,
                   (unsigned)rx_buffer_[0]));
        close();
        return -1;
    }
    zce_byte nmethods = rx_buffer_[1];
    if (rx_buffer_.size() < (size_t)(2 + nmethods)) return 0;

    rx_buffer_.erase(rx_buffer_.begin(), rx_buffer_.begin() + 2 + nmethods);

    zce_byte reply[2] = {SOCKS5_VER, 0x00 /* NO AUTH */};
    sendToPeer(reply, 2);
    state_ = ST_AWAIT_REQUEST;
    return 2 + nmethods;
}

int FgwRelaySession::tryParseRequest() {
    std::string host;
    zce_uint16 port = 0;
    size_t      need = 0;
    int         action = 0;   // 0 = need more; 1 = connect; 2 = cmd-not-sup; 3 = bad version

    {
        zce::Guard<zce::Mutex> g(lock_);
        if (rx_buffer_.size() < 5) return 0;  // need atyp to know length
        if (rx_buffer_[0] != SOCKS5_VER) { action = 3; }
        else {
            zce_byte cmd  = rx_buffer_[1];
            zce_byte atyp = rx_buffer_[3];
            if (atyp == SOCKS5_ATYP_IPV4) {
                need = 4 + 4 + 2;
                if (rx_buffer_.size() < need) return 0;
                char ip[32];
                std::snprintf(ip, sizeof(ip), "%u.%u.%u.%u",
                              (unsigned)rx_buffer_[4], (unsigned)rx_buffer_[5],
                              (unsigned)rx_buffer_[6], (unsigned)rx_buffer_[7]);
                host = ip;
                port = (zce_uint16)((rx_buffer_[8] << 8) | rx_buffer_[9]);
            } else if (atyp == SOCKS5_ATYP_DOMAIN) {
                if (rx_buffer_.size() < 5) return 0;
                zce_byte dlen = rx_buffer_[4];
                need = 4 + 1 + dlen + 2;
                if (rx_buffer_.size() < need) return 0;
                host.assign((const char*)&rx_buffer_[5], dlen);
                port = (zce_uint16)((rx_buffer_[5 + dlen] << 8) | rx_buffer_[5 + dlen + 1]);
            } else if (atyp == SOCKS5_ATYP_IPV6) {
                need = 4 + 16 + 2;
                if (rx_buffer_.size() < need) return 0;
                char ip[64] = {0};
                char* p = ip;
                for (int i = 0; i < 16; i += 2) {
                    if (i) *p++ = ':';
                    std::snprintf(p, 8, "%02x%02x",
                                  rx_buffer_[4 + i], rx_buffer_[4 + i + 1]);
                    p += 4;
                }
                host = ip;
                port = (zce_uint16)((rx_buffer_[4 + 16] << 8) | rx_buffer_[4 + 17]);
            } else {
                action = 2;
            }

            if (action == 0) {
                rx_buffer_.erase(rx_buffer_.begin(), rx_buffer_.begin() + need);
                if (cmd != SOCKS5_CMD_CONNECT) {
                    action = 2;
                } else {
                    target_addr_ = host + ":" + std::to_string(port);
                    state_ = ST_CONNECTING;
                    action = 1;
                }
            }
        }
    }

    if (action == 1) {
        ZCE_DEBUG((ZLOG_TRACE, "fgw: session %u -> %s", session_id_, target_addr_.c_str()));
        startConnect(host, port);
        return (int)need;
    }
    if (action == 2) {
        writeReply(SOCKS5_REP_CMDNOTSUP);
        close();
        return -1;
    }
    if (action == 3) {
        close();
        return -1;
    }
    return 0;
}

void FgwRelaySession::writeReply(zce_byte rep) {
    zce_byte reply[10] = {SOCKS5_VER, rep, 0x00, SOCKS5_ATYP_IPV4, 0, 0, 0, 0, 0, 0};
    sendToPeer(reply, sizeof(reply));
}

void FgwRelaySession::startConnect(const std::string& host, zce_uint16 port) {
    auto tcp = new OutboundTcp(outport_->reactor(), this);
    remote_tcp_ = tcp;
    remote_conn_ = new zce::Connector(remote_tcp_, host, port, 10);
    int ret = remote_conn_->start_connect();
    if (ret < 0) {
        ZCE_ERROR((ZLOG_ERROR, "fgw: session %u dial %s failed 0x%x",
                   session_id_, target_addr_.c_str(), ret));
        writeReply(SOCKS5_REP_HOSTUNREACH);
        close();
    }
    // Fixed-egress bind (egress_bind_ip) requires a libuv-level
    // uv_tcp_bind call before uv_tcp_connect which the current
    // zce::Tcp API does not yet expose.  The intended contract is
    // documented in libfgw/docs/design_fgw.md §3.2.2.
}

void FgwRelaySession::sendToPeer(const zce_byte* buf, zce_uint32 len) {
    if (stream_) stream_->sendSessionData(session_id_, buf, len, peer_ingress_id_);
}

// ─── OutportService ──────────────────────────────────────────────────────────
OutportService::OutportService(const zce::SmartPtr<zce::Reactor>& reactor,
                               const DataStreamPtr& stream,
                               const std::string& egress_bind_ip)
    : reactor_(reactor), stream_(stream), egress_bind_ip_(egress_bind_ip) {}

OutportService::~OutportService() {
    stop();
}

int OutportService::start() {
    dataStreamHook();
    return 0;
}

void OutportService::stop() {
    std::map<RelaySessionKey, FgwRelaySessionPtr> copy;
    {
        zce::Guard<zce::Mutex> g(lock_);
        copy.swap(sessions_);
    }
    for (auto& kv : copy) {
        if (kv.second) kv.second->close();
    }
}

FgwRelaySessionPtr OutportService::ensureSession(zce_uint32 peer_ingress, zce_uint32 session_id) {
    zce::Guard<zce::Mutex> g(lock_);
    RelaySessionKey key(peer_ingress, session_id);
    auto it = sessions_.find(key);
    if (it != sessions_.end()) return it->second;
    FgwRelaySessionPtr s(new FgwRelaySession(this, stream_, peer_ingress, session_id));
    sessions_[key] = s;
    return s;
}

void OutportService::removeSession(zce_uint32 peer_ingress, zce_uint32 session_id) {
    zce::Guard<zce::Mutex> g(lock_);
    sessions_.erase(RelaySessionKey(peer_ingress, session_id));
}

std::vector<FgwRelaySessionPtr> OutportService::snapshotSessions() const {
    zce::Guard<zce::Mutex> g(lock_);
    std::vector<FgwRelaySessionPtr> out;
    out.reserve(sessions_.size());
    for (const auto& kv : sessions_) {
        if (kv.second) out.push_back(kv.second);
    }
    return out;
}

void OutportService::dataStreamHook() {
    stream_->setUnknownSessionCallback([this](zce_uint32 peer_ingress,
                                              zce_uint32 session_id) -> ISessionHandler* {
        FgwRelaySessionPtr s = ensureSession(peer_ingress, session_id);
        return s.get();
    });
}

}  // namespace zfgw
