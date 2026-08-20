// ***************************************************************
//  zfgw_channel_manager.cpp
//  Yongming Wang(wangym@gmail.com)
//  Copyright (C) 2026 - All Rights Reserved
// ***************************************************************
#include "zfgw_channel_manager.h"
#include "zfgw_datastream.h"
#include "zfgw_segment.h"
#include "zfgw_pack.h"
#include "zfgw.h"
#include <zce/zce_log.h>
#include <zce/zce_task_queue.h>
#include <zce/zce_timer.h>
#include <algorithm>
#include <cstring>

#if ZFGW_WITH_LIBUTP
#    include "utp.h"
#endif

namespace zfgw {

// ─── FgwUtpContextHolder::UdpHandler ─────────────────────────────────────────
class FgwUtpContextHolder::UdpHandler : public zce::Udp {
  public:
    UdpHandler(const zce::SmartPtr<zce::Reactor>& reactor, FgwUtpContextHolder* owner)
        : zce::Udp(reactor), owner_(owner) {}

    void on_read_data(zce_byte* buf, zce_uint32 len, const zce_sockaddr_t* from) override {
#if ZFGW_WITH_LIBUTP
        if (owner_ && owner_->context() && from) {
            utp_process_udp(owner_->context(), buf, len, &from->sa, sizeof(zce_sockaddr_t));
        }
#else
        (void)buf; (void)len; (void)from;
#endif
    }

    void clearOwner() { owner_ = nullptr; }

  private:
    FgwUtpContextHolder* owner_;
};

// ─── FgwUtpContextHolder ─────────────────────────────────────────────────────
FgwUtpContextHolder::FgwUtpContextHolder(const zce::SmartPtr<zce::Reactor>& reactor,
                                         zce::TaskQueue* sync_queue)
    : reactor_(reactor), sync_queue_(sync_queue) {}

FgwUtpContextHolder::~FgwUtpContextHolder() {
    stop();
}

int FgwUtpContextHolder::start(const std::string& bind_ip, zce_uint16 bind_port) {
    zce::Guard<zce::Mutex> g(lock_);

    udp_ptr_ = new UdpHandler(reactor_, this);
    int ret = udp_ptr_->listen(bind_ip.empty() ? "0.0.0.0" : bind_ip.c_str(), bind_port);
    if (ret < 0) {
        ZCE_ERROR((ZLOG_ERROR, "fgw: utp udp listen %s:%u failed, ret=0x%x",
                   bind_ip.c_str(), (unsigned)bind_port, ret));
        udp_ptr_ = nullptr;
        return ZFGW_ERRCODE_LISTENFAIL;
    }

#if ZFGW_WITH_LIBUTP
    utp_ctx_ = utp_init(2);
    if (!utp_ctx_) {
        udp_ptr_->close();
        udp_ptr_ = nullptr;
        return ZFGW_ERRCODE_MEMORY;
    }
    // The actual callback wiring is omitted here to keep the bootstrap
    // terse.  FgwSession / ChannelManager register the required libutp
    // callbacks on this context once the holder is started.
#endif

    zce::SmartPtr<FgwUtpContextHolder> self(this);
    tick_timer_ = reactor_->scheduleTimer(zce::SmartPtr<zce::TaskQueue>(sync_queue_), 50, true,
                                          [self](zce::Timer*) { self->onTick(); });
    return 0;
}

void FgwUtpContextHolder::stop() {
    zce::Guard<zce::Mutex> g(lock_);
    if (tick_timer_) {
        tick_timer_->cancel();
        tick_timer_ = nullptr;
    }
#if ZFGW_WITH_LIBUTP
    if (utp_ctx_) {
        utp_destroy(utp_ctx_);
        utp_ctx_ = nullptr;
    }
#endif
    if (udp_ptr_) {
        udp_ptr_->clearOwner();
        udp_ptr_->close();
        udp_ptr_ = nullptr;
    }
}

void FgwUtpContextHolder::onTick() {
#if ZFGW_WITH_LIBUTP
    if (utp_ctx_) {
        utp_check_timeouts(utp_ctx_);
    }
#endif
}

int FgwUtpContextHolder::connectChannel(FgwUtpChannel* /*channel*/,
                                        const FgwEndpoint& /*remote*/) {
    // Detailed libutp socket wiring is intentionally omitted from this
    // initial framework — it requires project-specific callback tables
    // that the hosting VM sets up once libutp is actually vendored in.
    return ZFGW_ERRCODE_INVALIDCONFIG;
}

void FgwUtpContextHolder::closeChannel(FgwUtpChannel* /*channel*/) {
    // Same note as connectChannel() above.
}

// ─── LinkSelector ────────────────────────────────────────────────────────────
std::vector<zce_uint32> LinkSelector::select(const std::vector<FgwChannelPtr>& pool) const {
    std::vector<zce_uint32> out;
    if (pool.empty()) return out;

    struct Entry {
        zce_uint32 id;
        double     weight;
    };
    std::vector<Entry> ranked;
    ranked.reserve(pool.size());

    for (const auto& ch : pool) {
        if (!ch || !ch->isLive()) continue;
        auto q = ch->quality();
        double rtt = (q.rtt_ms == 0) ? 50.0 : (double)q.rtt_ms;
        double loss = q.loss_rate;
        if (loss < 0.0) loss = 0.0;
        if (loss > 0.99) loss = 0.99;
        double weight = (double)ch->priority() / (rtt * (1.0 + 10.0 * loss));
        ranked.push_back({ch->channelId(), weight});
    }

    std::sort(ranked.begin(), ranked.end(),
              [](const Entry& a, const Entry& b) { return a.weight > b.weight; });

    switch (mode_) {
        case ZFGW_MULTIPATH_BEST:
            if (!ranked.empty()) out.push_back(ranked.front().id);
            break;
        case ZFGW_MULTIPATH_ALL:
            for (const auto& e : ranked) out.push_back(e.id);
            break;
        case ZFGW_MULTIPATH_WEIGHTED:
        default: {
            // Explore/exploit dual-send: the primary (exploit) copy plus one
            // rotating probe (explore). Two copies bound the redundancy at 2x
            // while a stalled primary can't block delivery.
            if (ranked.empty()) break;

            // Primary: the fed-back recommended link if it is still live,
            // otherwise the best-weight link.
            size_t primary_idx = 0;
            const zce_uint32 want = primary_channel_id_.load();
            if (want != 0) {
                for (size_t i = 0; i < ranked.size(); ++i) {
                    if (ranked[i].id == want) { primary_idx = i; break; }
                }
            }
            out.push_back(ranked[primary_idx].id);

            // Probe: one channel from the rest, rotating each call so every
            // alternate link is exercised over time.
            if (ranked.size() > 1) {
                const size_t others = ranked.size() - 1;
                const size_t r = probe_rotation_.fetch_add(1, std::memory_order_relaxed) % others;
                const size_t idx = (r < primary_idx) ? r : r + 1;  // skip primary
                out.push_back(ranked[idx].id);
            }
            break;
        }
    }
    return out;
}

// ─── ChannelManager ──────────────────────────────────────────────────────────
ChannelManager::ChannelManager(const zce::SmartPtr<zce::Reactor>& reactor,
                               zce::TaskQueue* sync_queue)
    : reactor_(reactor), sync_queue_(sync_queue) {}

ChannelManager::~ChannelManager() {
    stop();
}

int ChannelManager::start(const FgwConfig& cfg) {
    zce::Guard<zce::Mutex> g(lock_);
    config_ = cfg;
    selector_.setMode((ZFGW_MULTIPATH_MODE)cfg.multipath_mode);

    for (const auto& ccfg : cfg.channels) {
        int ret = addChannel(ccfg);
        if (ret < 0) {
            ZCE_ERROR((ZLOG_ERROR, "fgw: addChannel %u failed, ret=0x%x", ccfg.channel_id, ret));
        }
    }
    startHeartbeat();
    return 0;
}

void ChannelManager::stop() {
    stopLinkListener();
    zce::Guard<zce::Mutex> g(lock_);
    if (heartbeat_timer_) {
        heartbeat_timer_->cancel();
        heartbeat_timer_ = nullptr;
    }
    bytes_subscribers_.clear();
    for (auto& kv : channels_) {
        if (kv.second) kv.second->close();
    }
    channels_.clear();
    if (shared_utp_) {
        shared_utp_->stop();
        shared_utp_ = nullptr;
    }
}

// ─── Inbound link listener (Transit / Outport accept side) ───────────────────
int ChannelManager::startLinkListener(const std::string& bind_ip, zce_uint16 bind_port) {
    {
        zce::Guard<zce::Mutex> g(lock_);
        if (link_acceptor_) return ZFGW_ERRCODE_BUSY;
    }

    // The factory runs in the reactor thread for each accepted connection.
    zce::SmartPtr<zce::Acceptor> acc(new zce::Acceptor(reactor_, [this]() -> zce::Tcp* {
        zce::SmartPtr<IFgwChannel> chan = makeAcceptedChannel();
        if (!chan) return nullptr;
        return static_cast<FgwTcpChannel*>(chan.get())->createAcceptedHandler();
    }));

    int ret = acc->listen(bind_ip.empty() ? "0.0.0.0" : bind_ip.c_str(), bind_port);
    if (ret < 0) {
        ZCE_ERROR((ZLOG_ERROR, "fgw: link listener %s:%u failed 0x%x",
                   bind_ip.c_str(), (unsigned)bind_port, ret));
        return ZFGW_ERRCODE_LISTENFAIL;
    }

    zce::Guard<zce::Mutex> g(lock_);
    link_acceptor_ = acc;
    ZCE_DEBUG((ZLOG_INFOR, "fgw: link listener up on %s:%u",
               bind_ip.empty() ? "0.0.0.0" : bind_ip.c_str(), (unsigned)bind_port));
    return 0;
}

void ChannelManager::stopLinkListener() {
    zce::SmartPtr<zce::Acceptor> acc;
    {
        zce::Guard<zce::Mutex> g(lock_);
        acc.swap(link_acceptor_);
    }
    if (acc) acc->close();
}

zce::SmartPtr<IFgwChannel> ChannelManager::makeAcceptedChannel() {
    zce::Guard<zce::Mutex> g(lock_);
    zce_uint32 id = accepted_id_seq_++;
    zce::SmartPtr<IFgwChannel> chan(new FgwTcpChannel(reactor_, id, /*priority*/ 100));
    chan->markAccepted();
    chan->setConnectedCallback([this](IFgwChannel* ch, int errcode) {
        onChannelConnected(ch, errcode);
    });
    chan->setClosedCallback([this](IFgwChannel* ch) {
        onChannelClosed(ch);
    });
    wireChannelRecv(chan.get());
    channels_[id] = chan;
    return chan;
}

void ChannelManager::sendHello(IFgwChannel* ch) {
    if (!ch) return;
    FgwHello hello;
    hello.proto_version = kFgwProtoVersion;
    hello.role       = config_.role;
    hello.ingress_id = config_.ingress_id;
    hello.outport_id = 0;  // per-segment routing carries the egress selector
    hello.channel_id = ch->channelId();

    zce_byte payload[64];
    int plen = zce::zdp::zds_pack(payload, (int)sizeof(payload), hello, nullptr, true);
    if (plen < 0) {
        ZCE_ERROR((ZLOG_WARNI, "fgw: hello pack failed on channel %u", ch->channelId()));
        return;
    }

    FgwSegmentHeader hdr;
    hdr.flags       = FgwSegmentHeader::FLAG_HELLO;
    hdr.payload_len = (zce_uint16)plen;
    hdr.session_id  = 0;
    hdr.seq_num     = 0;
    hdr.ingress_id  = config_.ingress_id;

    zce_byte frame[128];
    int wrote = fgwSegmentEncode(frame, (int)sizeof(frame), hdr, payload, plen);
    if (wrote < 0) return;
    ch->sendBytes(frame, (zce_uint32)wrote);
}

// ─── Heartbeat / stall detection ─────────────────────────────────────────────
void ChannelManager::startHeartbeat() {
    // Guard against a double start() — cancel any timer we already hold.
    if (heartbeat_timer_) {
        heartbeat_timer_->cancel();
        heartbeat_timer_ = nullptr;
    }
    unsigned interval = config_.heartbeat_interval > 0 ? (unsigned)config_.heartbeat_interval : 5;
    zce::SmartPtr<ChannelManager> self(this);
    heartbeat_timer_ = reactor_->scheduleTimer(zce::SmartPtr<zce::TaskQueue>(sync_queue_),
                                               (int)interval * 1000, /*repeat*/ true,
                                               [self](zce::Timer*) { self->onHeartbeatTick(); });
}

void ChannelManager::sendHeartbeat(IFgwChannel* ch) {
    if (!ch) return;
    FgwSegmentHeader hdr;
    hdr.flags       = FgwSegmentHeader::FLAG_HEARTBEAT;
    hdr.payload_len = 0;
    hdr.session_id  = 0;
    hdr.seq_num     = 0;
    hdr.ingress_id  = config_.ingress_id;

    zce_byte frame[64];
    int wrote = fgwSegmentEncode(frame, (int)sizeof(frame), hdr, nullptr, 0);
    if (wrote < 0) return;
    ch->sendBytes(frame, (zce_uint32)wrote);
}

void ChannelManager::onHeartbeatTick() {
    std::vector<FgwChannelPtr> chans;
    unsigned link_timeout_ms;
    {
        zce::Guard<zce::Mutex> g(lock_);
        for (auto& kv : channels_) if (kv.second) chans.push_back(kv.second);
        link_timeout_ms = (config_.link_timeout > 0 ? (unsigned)config_.link_timeout : 15) * 1000;
    }

    for (auto& ch : chans) {
        if (!ch->isConnected()) continue;
        sendHeartbeat(ch.get());
        // Stall detection: TCP still open but silent past link_timeout. Sampling
        // `now`, reading last_rx_tick, and writing stalled_ all happen inside
        // evaluateStall() under the receive lock, so a concurrent addRecvBytes()
        // recovery can neither be overwritten nor make the delta underflow.
        const bool was_stalled = ch->isStalled();
        ch->evaluateStall(link_timeout_ms);
        if (ch->isStalled() != was_stalled) {
            ZCE_DEBUG((ZLOG_INFOR, "fgw: channel %u %s", ch->channelId(),
                       ch->isStalled() ? "STALLED (no rx)" : "recovered"));
        }
    }
}

int ChannelManager::addChannel(const FgwChannelConfig& cfg) {
    zce::Guard<zce::Mutex> g(lock_);
    if (channels_.count(cfg.channel_id)) {
        return ZFGW_ERRCODE_CHANNELEXISTS;
    }

    FgwChannelPtr chan;
    if (cfg.kind == ZFGW_CHANNEL_TCP) {
        chan = new FgwTcpChannel(reactor_, cfg.channel_id, cfg.priority);
    } else if (cfg.kind == ZFGW_CHANNEL_UTP) {
        auto ctx = sharedUtpContext();
        chan = new FgwUtpChannel(ctx, cfg.channel_id, cfg.priority);
    } else {
        return ZFGW_ERRCODE_INVALIDCONFIG;
    }

    chan->setConnectedCallback([this](IFgwChannel* ch, int errcode) {
        onChannelConnected(ch, errcode);
    });
    chan->setClosedCallback([this](IFgwChannel* ch) {
        onChannelClosed(ch);
    });
    wireChannelRecv(chan.get());
    channels_[cfg.channel_id] = chan;
    int ret = chan->connect(cfg.remote);
    if (ret < 0) {
        ZCE_ERROR((ZLOG_ERROR, "fgw: channel %u connect %s:%u failed, ret=0x%x",
                   cfg.channel_id, cfg.remote.host.c_str(), (unsigned)cfg.remote.port, ret));
    }
    return ret;
}

int ChannelManager::removeChannel(zce_uint32 channel_id) {
    zce::Guard<zce::Mutex> g(lock_);
    auto it = channels_.find(channel_id);
    if (it == channels_.end()) return ZFGW_ERRCODE_CHANNELNOTFOUND;
    if (it->second) it->second->close();
    channels_.erase(it);
    return 0;
}

zce::SmartPtr<FgwUtpContextHolder> ChannelManager::sharedUtpContext(const std::string& bind_ip,
                                                                    zce_uint16 bind_port) {
    if (!shared_utp_) {
        shared_utp_ = new FgwUtpContextHolder(reactor_, sync_queue_);
        int ret = shared_utp_->start(bind_ip, bind_port);
        if (ret < 0) {
            shared_utp_ = nullptr;
        }
    }
    return shared_utp_;
}

std::vector<FgwChannelPtr> ChannelManager::liveChannels() const {
    zce::Guard<zce::Mutex> g(lock_);
    std::vector<FgwChannelPtr> out;
    out.reserve(channels_.size());
    for (const auto& kv : channels_) {
        if (kv.second && kv.second->isLive()) {
            out.push_back(kv.second);
        }
    }
    return out;
}

FgwChannelPtr ChannelManager::findChannel(zce_uint32 channel_id) const {
    zce::Guard<zce::Mutex> g(lock_);
    auto it = channels_.find(channel_id);
    return it == channels_.end() ? FgwChannelPtr() : it->second;
}

void ChannelManager::addBytesSubscriber(DataStream* stream) {
    if (!stream) return;
    zce::Guard<zce::Mutex> g(lock_);
    auto it = std::find(bytes_subscribers_.begin(), bytes_subscribers_.end(), stream);
    if (it != bytes_subscribers_.end()) return;
    bytes_subscribers_.push_back(stream);
}

void ChannelManager::removeBytesSubscriber(DataStream* stream) {
    if (!stream) return;
    zce::Guard<zce::Mutex> g(lock_);
    bytes_subscribers_.erase(
        std::remove(bytes_subscribers_.begin(), bytes_subscribers_.end(), stream),
        bytes_subscribers_.end());
}

void ChannelManager::dispatchChannelBytes(IFgwChannel* ch, const zce_byte* buf, zce_uint32 len) {
    std::vector<DataStream*> copy;
    {
        zce::Guard<zce::Mutex> g(lock_);
        copy = bytes_subscribers_;
    }
    for (auto* ds : copy) {
        if (ds) ds->onChannelBytes(ch, buf, len);
    }
}

void ChannelManager::wireChannelRecv(IFgwChannel* chan) {
    if (!chan) return;
    chan->setBytesCallback([this](IFgwChannel* ch, const zce_byte* buf, zce_uint32 len) {
        dispatchChannelBytes(ch, buf, len);
    });
}

void ChannelManager::collectLinkQualities(std::vector<FgwLinkQuality>& out) const {
    zce::Guard<zce::Mutex> g(lock_);
    out.clear();
    out.reserve(channels_.size());
    for (const auto& kv : channels_) {
        if (!kv.second) continue;
        auto q = kv.second->quality();
        FgwLinkQuality lq;
        lq.channel_id      = kv.second->channelId();
        lq.connected       = kv.second->isConnected() ? 1 : 0;
        lq.rtt_ms          = q.rtt_ms;
        lq.loss_rate       = q.loss_rate;
        lq.bytes_sent      = q.bytes_sent;
        lq.bytes_recv      = q.bytes_recv;
        lq.send_segments   = q.send_segments;
        lq.recv_segments   = q.recv_segments;
        lq.rexmit_segments = q.rexmit_segments;
        out.push_back(lq);
    }
}

void ChannelManager::onChannelConnected(IFgwChannel* ch, int errcode) {
    if (errcode >= 0) {
        ZCE_DEBUG((ZLOG_TRACE, "fgw: channel %u connected", ch->channelId()));
        if (on_opened_) on_opened_(this, ch);
        // The dialing side announces itself first; accepted channels wait for
        // the peer's Hello instead of sending their own.
        if (!ch->isAccepted()) sendHello(ch);
    } else {
        ZCE_ERROR((ZLOG_ERROR, "fgw: channel %u connect failed 0x%x", ch->channelId(), errcode));
    }
}

void ChannelManager::onChannelClosed(IFgwChannel* ch) {
    ZCE_DEBUG((ZLOG_TRACE, "fgw: channel %u closed", ch->channelId()));
    if (on_closed_) on_closed_(this, ch);

    zce_uint32 id = ch->channelId();

    // Accepted (inbound) channels are never redialed — the remote peer will
    // reconnect. Remove them, but defer to the next loop turn so we do not
    // free the channel while still unwinding its own on_close callback.
    if (ch->isAccepted()) {
        zce::SmartPtr<ChannelManager> self(this);
        reactor_->scheduleTimer(zce::SmartPtr<zce::TaskQueue>(sync_queue_), 1, false,
                                [self, id](zce::Timer*) { self->removeChannel(id); });
        return;
    }

    FgwEndpoint remote{};
    zce_uint32  kind = ZFGW_CHANNEL_TCP, priority = 100;
    {
        zce::Guard<zce::Mutex> g(lock_);
        auto it = std::find_if(config_.channels.begin(), config_.channels.end(),
                               [&](const FgwChannelConfig& c) { return c.channel_id == id; });
        if (it != config_.channels.end()) {
            remote = it->remote;
            kind = it->kind;
            priority = it->priority;
        }
    }
    scheduleReconnect(id, remote, kind, priority,
                      config_.reconnect_max > 0 ? (unsigned)config_.reconnect_max : 30);
}

void ChannelManager::scheduleReconnect(zce_uint32 channel_id, FgwEndpoint remote,
                                       zce_uint32 kind, zce_uint32 priority,
                                       unsigned backoff_seconds) {
    zce::SmartPtr<ChannelManager> self(this);
    reactor_->scheduleTimer(zce::SmartPtr<zce::TaskQueue>(sync_queue_),
                            (int)backoff_seconds * 1000, false, [=](zce::Timer*) {
        FgwChannelConfig cfg;
        cfg.channel_id = channel_id;
        cfg.kind       = (zce_byte)kind;
        cfg.remote     = remote;
        cfg.priority   = priority;
        self->removeChannel(channel_id);
        self->addChannel(cfg);
    });
}

}  // namespace zfgw
