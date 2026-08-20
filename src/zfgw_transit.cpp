// ***************************************************************
//  zfgw_transit.cpp
//  Yongming Wang(wangym@gmail.com)
//  Copyright (C) 2026 - All Rights Reserved
// ***************************************************************
#include "zfgw_transit.h"
#include "zfgw_pack.h"
#include "zfgw.h"

#include <zce/zce_log.h>
#include <zce/zce_timer.h>

namespace zfgw {

TransitService::TransitService(const zce::SmartPtr<zce::Reactor>& reactor,
                               zce::TaskQueue* sync_queue, const FgwConfig& config)
    : reactor_(reactor), sync_queue_(sync_queue), config_(config) {}

TransitService::~TransitService() {
    stop();
}

int TransitService::start() {
    dialOutports();

    // Accept inbound links from Inports on the configured listen endpoint.
    if (config_.outport_listen_port != 0) {
        zce::SmartPtr<zce::Acceptor> acc(new zce::Acceptor(reactor_, [this]() -> zce::Tcp* {
            zce::SmartPtr<IFgwChannel> chan = makeInboundChannel();
            if (!chan) return nullptr;
            return static_cast<FgwTcpChannel*>(chan.get())->createAcceptedHandler();
        }));
        const std::string& ip = config_.outport_listen_host;
        int ret = acc->listen(ip.empty() ? "0.0.0.0" : ip.c_str(), config_.outport_listen_port);
        if (ret < 0) {
            ZCE_ERROR((ZLOG_ERROR, "fgw transit: listen %s:%u failed 0x%x", ip.c_str(),
                       (unsigned)config_.outport_listen_port, ret));
            return ZFGW_ERRCODE_LISTENFAIL;
        }
        zce::Guard<zce::Mutex> g(lock_);
        acceptor_ = acc;
        ZCE_DEBUG((ZLOG_INFOR, "fgw transit: listening for inports on %s:%u",
                   ip.empty() ? "0.0.0.0" : ip.c_str(), (unsigned)config_.outport_listen_port));
    }
    return 0;
}

void TransitService::stop() {
    zce::SmartPtr<zce::Acceptor> acc;
    std::map<IFgwChannel*, Link> links;
    {
        zce::Guard<zce::Mutex> g(lock_);
        acc.swap(acceptor_);
        links.swap(links_);
        outbound_by_outport_.clear();
        inbound_by_ingress_.clear();
    }
    if (acc) acc->close();
    for (auto& kv : links) {
        if (kv.second.chan) kv.second.chan->close();
    }
}

void TransitService::dialOutports() {
    for (const auto& ccfg : config_.channels) {
        dialOneOutport(ccfg);
    }
}

void TransitService::dialOneOutport(const FgwChannelConfig& ccfg) {
    zce::SmartPtr<IFgwChannel> chan(new FgwTcpChannel(reactor_, ccfg.channel_id, ccfg.priority));
    IFgwChannel* raw = chan.get();
    chan->setBytesCallback([this](IFgwChannel* ch, const zce_byte* b, zce_uint32 n) {
        onLinkBytes(ch, b, n);
    });
    chan->setClosedCallback([this](IFgwChannel* ch) { onLinkClosed(ch); });
    {
        zce::Guard<zce::Mutex> g(lock_);
        // Drop any stale channel we still hold for this outport_id (e.g. a prior
        // dial that failed synchronously). Explicit close() does not fire the
        // closed callback, so this cannot recurse into onLinkClosed().
        auto oit = outbound_by_outport_.find(ccfg.channel_id);
        if (oit != outbound_by_outport_.end() && oit->second) {
            links_.erase(oit->second.get());
        }
        Link L;
        L.chan     = chan;
        L.outbound = true;
        links_[raw] = std::move(L);
        // channel_id is the outport_id this link serves.
        outbound_by_outport_[ccfg.channel_id] = chan;
    }
    int ret = chan->connect(ccfg.remote);
    if (ret < 0) {
        ZCE_ERROR((ZLOG_ERROR, "fgw transit: dial outport %u (%s:%u) failed 0x%x, will retry",
                   ccfg.channel_id, ccfg.remote.host.c_str(), (unsigned)ccfg.remote.port, ret));
        scheduleOutportRedial(ccfg.channel_id);
    }
}

void TransitService::scheduleOutportRedial(zce_uint32 outport_id) {
    const unsigned backoff = config_.reconnect_max > 0 ? (unsigned)config_.reconnect_max : 30;
    zce::SmartPtr<TransitService> self(this);
    reactor_->scheduleTimer(zce::SmartPtr<zce::TaskQueue>(sync_queue_), (int)backoff * 1000, false,
                            [self, outport_id](zce::Timer*) {
        FgwChannelConfig cfg;
        bool found = false;
        {
            zce::Guard<zce::Mutex> g(self->lock_);
            for (const auto& c : self->config_.channels) {
                if (c.channel_id == outport_id) { cfg = c; found = true; break; }
            }
        }
        if (found) {
            ZCE_DEBUG((ZLOG_INFOR, "fgw transit: re-dialing outport %u", outport_id));
            self->dialOneOutport(cfg);
        }
    });
}

zce::SmartPtr<IFgwChannel> TransitService::makeInboundChannel() {
    zce::Guard<zce::Mutex> g(lock_);
    zce_uint32 id = accepted_id_seq_++;
    zce::SmartPtr<IFgwChannel> chan(new FgwTcpChannel(reactor_, id, 100));
    chan->markAccepted();
    IFgwChannel* raw = chan.get();
    chan->setBytesCallback([this](IFgwChannel* ch, const zce_byte* b, zce_uint32 n) {
        onLinkBytes(ch, b, n);
    });
    chan->setClosedCallback([this](IFgwChannel* ch) { onLinkClosed(ch); });
    Link L;
    L.chan     = chan;
    L.outbound = false;
    links_[raw] = std::move(L);
    return chan;
}

FgwChannelPtr TransitService::findOutbound(zce_uint32 outport_id) {
    auto it = outbound_by_outport_.find(outport_id);
    return it == outbound_by_outport_.end() ? FgwChannelPtr() : it->second;
}

FgwChannelPtr TransitService::findInbound(zce_uint32 ingress_id) {
    auto it = inbound_by_ingress_.find(ingress_id);
    return it == inbound_by_ingress_.end() ? FgwChannelPtr() : it->second;
}

void TransitService::onLinkBytes(IFgwChannel* ch, const zce_byte* buf, zce_uint32 len) {
    zce::Guard<zce::Mutex> g(lock_);
    auto lit = links_.find(ch);
    if (lit == links_.end()) return;
    Link& L = lit->second;
    L.buffer.insert(L.buffer.end(), buf, buf + len);

    size_t pos = 0;
    while (true) {
        const size_t remaining = L.buffer.size() - pos;
        if (remaining < (size_t)FgwSegmentHeader::LEGACY_HEADER_SIZE) break;

        FgwSegmentHeader hdr;
        int hr = fgwSegmentPeekHeader(L.buffer.data() + pos, (int)remaining, hdr);
        if (hr == ZFGW_ERRCODE_BADSEGMENT) {
            ZCE_ERROR((ZLOG_WARNI, "fgw transit: bad segment on link %u, dropping buffer",
                       ch->channelId()));
            L.buffer.clear();
            return;
        }
        if (hr == 0) break;  // need more bytes
        const int total = hr + (int)hdr.payload_len;
        if ((size_t)total > remaining) break;

        const zce_byte* frame = L.buffer.data() + pos;

        if (hdr.isHello()) {
            // A hello is a hop-local handshake; learn identity from an inbound
            // (Inport) hello and do NOT forward it.
            if (!L.outbound) {
                FgwHello hello;
                if (zce::zdp::zds_unpack(hello, frame + hr, (int)hdr.payload_len, nullptr, true) >= 0 &&
                    hello.proto_version == kFgwProtoVersion) {
                    inbound_by_ingress_[hello.ingress_id] = L.chan;
                }
            }
        } else if (hdr.isHeartbeat()) {
            // Hop-local keepalive: consume, never forward end-to-end.
        } else if (!L.outbound) {
            // From an Inport: learn its ingress and forward by outport_id.
            inbound_by_ingress_[hdr.ingress_id] = L.chan;
            FgwChannelPtr out = findOutbound(hdr.outport_id);
            if (out) {
                out->sendBytes(frame, (zce_uint32)total);
            } else {
                ZCE_ERROR((ZLOG_WARNI, "fgw transit: no outport %u for session %u",
                           hdr.outport_id, hdr.session_id));
            }
        } else {
            // From an Outport: forward reverse traffic by ingress_id.
            FgwChannelPtr in = findInbound(hdr.ingress_id);
            if (in) {
                in->sendBytes(frame, (zce_uint32)total);
            } else {
                ZCE_ERROR((ZLOG_WARNI, "fgw transit: no inport for ingress %u", hdr.ingress_id));
            }
        }

        pos += (size_t)total;
    }

    if (pos > 0) {
        L.buffer.erase(L.buffer.begin(), L.buffer.begin() + (ptrdiff_t)pos);
    }
}

void TransitService::onLinkClosed(IFgwChannel* ch) {
    // Defer removal to the next loop turn so we do not free the channel while
    // still unwinding its own on_close callback.
    zce::SmartPtr<TransitService> self(this);
    reactor_->scheduleTimer(zce::SmartPtr<zce::TaskQueue>(sync_queue_), 1, false,
                            [self, ch](zce::Timer*) {
        bool       was_outbound = false;
        bool       have_outport = false;
        zce_uint32 outport_id   = 0;
        {
            zce::Guard<zce::Mutex> g(self->lock_);
            auto lit = self->links_.find(ch);
            if (lit == self->links_.end()) return;
            was_outbound = lit->second.outbound;
            // Drop it from whichever routing table references it, capturing the
            // outport_id of an outbound link so we can re-dial it.
            for (auto it = self->outbound_by_outport_.begin();
                 it != self->outbound_by_outport_.end();) {
                if (it->second.get() == ch) {
                    outport_id = it->first;
                    have_outport = true;
                    it = self->outbound_by_outport_.erase(it);
                } else {
                    ++it;
                }
            }
            for (auto it = self->inbound_by_ingress_.begin();
                 it != self->inbound_by_ingress_.end();) {
                if (it->second.get() == ch) it = self->inbound_by_ingress_.erase(it);
                else ++it;
            }
            self->links_.erase(lit);
        }
        // An Outport link dropped — schedule a backoff re-dial so the route
        // heals without operator intervention. Inbound (Inport) links are not
        // re-dialed: the Inport reconnects on its own.
        if (was_outbound && have_outport) self->scheduleOutportRedial(outport_id);
    });
}

}  // namespace zfgw
