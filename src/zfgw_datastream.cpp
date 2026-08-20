// ***************************************************************
//  zfgw_datastream.cpp
//  Yongming Wang(wangym@gmail.com)
//  Copyright (C) 2026 - All Rights Reserved
// ***************************************************************
#include "zfgw_datastream.h"
#include "zfgw_pack.h"
#include "zfgw.h"
#include <zce/zce_log.h>
#include <zce/zce_mbpool.h>
#include <algorithm>
#include <cstring>

namespace zfgw {

DataStream::DataStream(const zce::SmartPtr<ChannelManager>& manager, zce_uint32 local_ingress_id,
                       zce_uint16 segment_size, zce_uint16 recv_window, bool verify_crc,
                       zce_uint32 route_outport_id)
    : manager_(manager),
      local_ingress_id_(local_ingress_id),
      segment_size_(segment_size),
      recv_window_(recv_window),
      verify_crc_(verify_crc),
      route_outport_id_(route_outport_id) {
    if (segment_size_ == 0 || segment_size_ > FgwSegmentHeader::MAX_PAYLOAD) {
        segment_size_ = 1200;
    }
    if (recv_window_ == 0) recv_window_ = 1024;
    if (manager_) {
        manager_->addBytesSubscriber(this);
    }
}

DataStream::~DataStream() {
    if (manager_) {
        manager_->removeBytesSubscriber(this);
    }
}

int DataStream::openSession(zce_uint32 session_id, ISessionHandler* handler) {
    zce::Guard<zce::Mutex> g(lock_);
    SessionKey key(local_ingress_id_, session_id);
    if (sessions_.count(key)) {
        return ZFGW_ERRCODE_BUSY;
    }
    SessionState st;
    st.session_id      = session_id;
    st.wire_ingress_id = local_ingress_id_;
    st.handler         = handler;
    st.opened          = true;
    sessions_[key]     = std::move(st);

    sendSegment(local_ingress_id_, session_id, 0, FgwSegmentHeader::FLAG_SYN, nullptr, 0);
    return 0;
}

void DataStream::closeSession(zce_uint32 session_id, zce_uint32 ingress_key) {
    zce::Guard<zce::Mutex> g(lock_);
    const zce_uint32 ing = resolveIngressKey(ingress_key);
    SessionKey key(ing, session_id);
    auto it = sessions_.find(key);
    if (it == sessions_.end()) return;

    if (!it->second.closed) {
        sendSegment(it->second.wire_ingress_id, session_id, it->second.next_send_seq++,
                    FgwSegmentHeader::FLAG_FIN, nullptr, 0);
        it->second.closed = true;
    }
    sessions_.erase(it);
}

int DataStream::sendSessionData(zce_uint32 session_id, const zce_byte* buf, zce_uint32 len,
                                zce_uint32 ingress_key) {
    zce::Guard<zce::Mutex> g(lock_);
    const zce_uint32 ing = resolveIngressKey(ingress_key);
    SessionKey key(ing, session_id);
    auto it = sessions_.find(key);
    if (it == sessions_.end() || it->second.closed) {
        return ZFGW_ERRCODE_NOCHANNEL;
    }

    zce_uint32 written = 0;
    while (written < len) {
        zce_uint32 chunk = len - written;
        if (chunk > segment_size_) chunk = segment_size_;
        int ret = sendSegment(it->second.wire_ingress_id, session_id, it->second.next_send_seq++,
                              FgwSegmentHeader::FLAG_DATA, buf + written, chunk);
        if (ret < 0) return ret;
        written += chunk;
    }
    return (int)written;
}

int DataStream::sendSegment(zce_uint32 wire_ingress, zce_uint32 session_id, zce_uint32 seq,
                            zce_uint8 flags, const zce_byte* payload, zce_uint32 payload_len) {
    FgwSegmentHeader hdr;
    hdr.flags       = flags;
    hdr.payload_len = (zce_uint16)payload_len;
    hdr.session_id  = session_id;
    hdr.seq_num     = seq;
    hdr.ingress_id  = wire_ingress;
    if (route_outport_id_ != 0) {
        hdr.flags      = (zce_uint8)(hdr.flags | FgwSegmentHeader::FLAG_HDR_ROUTE);
        hdr.outport_id = route_outport_id_;
    }

    const int hdr_sz = (route_outport_id_ != 0) ? FgwSegmentHeader::ROUTED_HEADER_SIZE
                                                : FgwSegmentHeader::HEADER_SIZE;
    const int total = hdr_sz + (int)payload_len;
    zce::RefBlock dblock;
    ZCE_MBACQUIRE(dblock, total);
    if ((int)dblock.space() < total) return ZFGW_ERRCODE_MEMORY;

    int wrote = fgwSegmentEncode(dblock.wr_ptr_cow(), (int)dblock.space(), hdr,
                                 payload, (int)payload_len);
    if (wrote < 0) return wrote;
    dblock.wr_ptr(wrote);

    auto live = manager_->liveChannels();
    auto ids  = manager_->selector().select(live);
    if (ids.empty()) {
        return ZFGW_ERRCODE_NOCHANNEL;
    }

    int best = 0;
    for (size_t i = 0; i < ids.size(); ++i) {
        auto ch = std::find_if(live.begin(), live.end(),
                               [&](const FgwChannelPtr& p) { return p && p->channelId() == ids[i]; });
        if (ch == live.end() || !*ch) continue;
        int ret = (*ch)->sendBytes(dblock.rd_ptr(), wrote);
        if (ret > 0 && i == 0) best = ret;
    }
    return best > 0 ? best : (int)payload_len;
}

void DataStream::onChannelBytes(IFgwChannel* ch, const zce_byte* buf, zce_uint32 len) {
    zce::Guard<zce::Mutex> g(lock_);
    auto& scratch = scratch_[ch];
    scratch.buffer.insert(scratch.buffer.end(), buf, buf + len);

    size_t pos = 0;
    while (true) {
        const size_t remaining = scratch.buffer.size() - pos;
        if (remaining < (size_t)FgwSegmentHeader::LEGACY_HEADER_SIZE) break;

        FgwSegmentHeader hdr;
        int hr = fgwSegmentPeekHeader(scratch.buffer.data() + pos, (int)remaining, hdr);
        if (hr == ZFGW_ERRCODE_BADSEGMENT) {
            ZCE_ERROR((ZLOG_WARNI, "fgw: channel %u bad segment, dropping buffer",
                       ch->channelId()));
            scratch.buffer.clear();
            return;
        }
        if (hr == 0) break;

        const int total = hr + (int)hdr.payload_len;
        if ((size_t)total > remaining) break;

        const zce_byte* frame = scratch.buffer.data() + pos;
        if (verify_crc_) {
            if (!fgwSegmentVerifyCrc(frame, hdr, hr)) {
                ZCE_ERROR((ZLOG_WARNI, "fgw: channel %u CRC mismatch on seg %u/%u",
                           ch->channelId(), hdr.session_id, hdr.seq_num));
                pos += (size_t)total;
                continue;
            }
        }
        ch->noteSegmentRecv();

        // Per-link handshake: learn the peer's identity for this channel and
        // stop. Hello is not a session segment and must bypass dedup (all of a
        // peer's channels share ingress/session/seq = X/0/0 and would collide).
        if (hdr.isHello()) {
            FgwHello hello;
            if (zce::zdp::zds_unpack(hello, frame + hr, (int)hdr.payload_len,
                                     nullptr, true) < 0) {
                ZCE_ERROR((ZLOG_WARNI, "fgw: channel %u bad hello payload", ch->channelId()));
            } else if (hello.proto_version != kFgwProtoVersion) {
                // Incompatible peer: do not apply identity — the link cannot be
                // trusted to speak our framing/routing. Leave it unidentified.
                ZCE_ERROR((ZLOG_WARNI,
                           "fgw: channel %u hello proto_version %u unsupported (want %u), ignoring",
                           ch->channelId(), (unsigned)hello.proto_version,
                           (unsigned)kFgwProtoVersion));
            } else {
                ch->setPeerIdentity(hello.channel_id, hello.ingress_id, hello.outport_id);
                ZCE_DEBUG((ZLOG_TRACE,
                           "fgw: channel %u hello peer(ch=%u ing=%u out=%u role=%u)",
                           ch->channelId(), hello.channel_id, hello.ingress_id,
                           hello.outport_id, (unsigned)hello.role));
            }
            pos += (size_t)total;
            continue;
        }

        if (hdr.isHeartbeat()) {
            // Keepalive: the channel's recv path already refreshed liveness.
            // Carries no session and must bypass dedup.
            pos += (size_t)total;
            continue;
        }

        const zce_uint32 rx_ingress = hdr.isHdrExt() ? hdr.ingress_id : 0;
        SessionKey skey(rx_ingress, hdr.session_id);

        DedupKey dk;
        dk.ingress = rx_ingress;
        dk.session = hdr.session_id;
        dk.seq     = hdr.seq_num;
        dk.kind    = hdr.isSyn() ? 1 : (hdr.isFin() ? 2 : 0);
        if (dedup_map_.count(dk)) {
            pos += (size_t)total;
            continue;
        }
        dedup_map_[dk] = true;
        dedup_queue_.push_back(dk);
        if (dedup_queue_.size() > kDedupRing) {
            dedup_map_.erase(dedup_queue_.front());
            dedup_queue_.pop_front();
        }

        auto it = sessions_.find(skey);

        if (hdr.isSyn()) {
            if (it == sessions_.end()) {
                SessionState st;
                st.session_id      = hdr.session_id;
                st.wire_ingress_id = rx_ingress;
                if (unknown_cb_) {
                    st.handler = unknown_cb_(rx_ingress, hdr.session_id);
                }
                sessions_[skey] = std::move(st);
                it = sessions_.find(skey);
            }
            if (it != sessions_.end() && it->second.handler) {
                it->second.handler->onSessionOpen(this, hdr.session_id);
            }
        }
        if (hdr.isFin()) {
            if (it != sessions_.end()) {
                ISessionHandler* h = it->second.handler;
                if (h) {
                    // The handler's onSessionClose() may re-enter closeSession()
                    // and erase this very entry, invalidating `it`. Re-find after
                    // the callback before erasing to avoid a double-erase crash.
                    h->onSessionClose(this, hdr.session_id);
                    it = sessions_.find(skey);
                }
                if (it != sessions_.end()) sessions_.erase(it);
            }
            pos += (size_t)total;
            continue;
        }
        if (hdr.isData() && hdr.payload_len > 0) {
            if (it == sessions_.end()) {
                SessionState st;
                st.session_id      = hdr.session_id;
                st.wire_ingress_id = rx_ingress;
                if (unknown_cb_) {
                    st.handler = unknown_cb_(rx_ingress, hdr.session_id);
                }
                sessions_[skey] = std::move(st);
                it = sessions_.find(skey);
            }
            {
                zce::RefBlock payload;
                ZCE_MBACQUIRE(payload, (int)hdr.payload_len);
                if ((int)payload.space() >= (int)hdr.payload_len) {
                    std::memcpy(payload.wr_ptr_cow(), frame + hr, hdr.payload_len);
                    payload.wr_ptr(hdr.payload_len);
                    it->second.rx_buffer.emplace(hdr.seq_num, std::move(payload));
                }
                if (it->second.rx_buffer.size() > recv_window_) {
                    ZCE_ERROR((ZLOG_WARNI, "fgw: session %u rx window overflow",
                               hdr.session_id));
                }
                deliverInOrder(it->second);
            }
        }
        pos += (size_t)total;
    }

    if (pos > 0) {
        scratch.buffer.erase(scratch.buffer.begin(), scratch.buffer.begin() + (ptrdiff_t)pos);
    }
}

void DataStream::deliverInOrder(SessionState& state) {
    if (!state.handler) return;
    // Collect the contiguous in-order run BEFORE invoking the handler: a
    // handler callback may re-enter and erase this session (e.g. an Outport
    // relay closing on a SOCKS5 error), which would leave `state` dangling for
    // the next iteration. Save what we need, then deliver from locals only.
    ISessionHandler* handler = state.handler;
    const zce_uint32 sid = state.session_id;
    std::vector<zce::RefBlock> ready;
    while (!state.rx_buffer.empty()) {
        auto it = state.rx_buffer.begin();
        if (it->first != state.expected_rx_seq) break;
        ready.push_back(std::move(it->second));
        state.rx_buffer.erase(it);
        ++state.expected_rx_seq;
    }
    for (auto& blk : ready) {
        if (blk.length() > 0) {
            handler->onSessionData(this, sid, blk.rd_ptr(), (zce_uint32)blk.length());
        }
    }
}

}  // namespace zfgw
