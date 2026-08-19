// ***************************************************************
//  zfgw_datastream.h  — multipath aggregated transport.
//
//  A DataStream instance multiplexes many logical sessions across
//  the (possibly-many) channels owned by a ChannelManager.  Each
//  session receives an ordered, deduplicated byte-stream in a form
//  compatible with zce::IStream callbacks so that higher layers
//  (InportService / OutportService) can treat it as an ordinary
//  duplex pipe.
//
//  Sessions are keyed by **(ingress_id, session_id)** on the wire so
//  multiple Inport VMs can share one ChannelManager / link bundle.
//
//  Yongming Wang(wangym@gmail.com)
//  Copyright (C) 2026 - All Rights Reserved
// ***************************************************************
#pragma once

#include "zfgw_channel_manager.h"
#include "zfgw_segment.h"
#include "zfgw.h"
#include <zce/zce_sync.h>
#include <functional>
#include <map>
#include <deque>
#include <vector>
#include <utility>

namespace zfgw {

class DataStream;

/// Interface that the DataStream invokes for each session's payload.
/// One instance per logical session (sessionid).  Both Inport and
/// Outport sides implement this to receive application-layer bytes.
class ISessionHandler {
  public:
    virtual ~ISessionHandler() = default;

    /// Called when the peer sends a SYN for this session.
    virtual void onSessionOpen(DataStream* stream, zce_uint32 session_id) {}

    /// Ordered, deduplicated payload bytes for the session.
    virtual void onSessionData(DataStream* stream, zce_uint32 session_id,
                               const zce_byte* buf, zce_uint32 len) = 0;

    /// Peer closed the session.
    virtual void onSessionClose(DataStream* stream, zce_uint32 session_id) = 0;
};

struct SessionState {
    zce_uint32 session_id      = 0;
    /// Inport-side ingress id carried on the wire for this session.
    zce_uint32 wire_ingress_id = 0;
    zce_uint32 next_send_seq   = 0;
    zce_uint32 expected_rx_seq = 0;
    std::map<zce_uint32, zce::RefBlock> rx_buffer;  // seq -> payload
    ISessionHandler* handler   = nullptr;
    bool opened                = false;
    bool closed                = false;
};

/// DataStream — the heart of libfgw.
class DataStream : public zce::Object {
  public:
    DataStream(const zce::SmartPtr<ChannelManager>& manager, zce_uint32 local_ingress_id,
               zce_uint16 segment_size = 1200, zce_uint16 recv_window = 1024,
               bool verify_crc = true);
    ~DataStream() override;

    /// Register a locally-initiated session (Inport).  Sends SYN with
    /// `local_ingress_id` in the segment header.
    int openSession(zce_uint32 session_id, ISessionHandler* handler);

    /// Close a session and send FIN.  `ingress_key` defaults to
    /// kFgwWireIngressUseLocal (Inport); Outport must pass the peer
    /// ingress id explicitly.
    void closeSession(zce_uint32 session_id,
                      zce_uint32 ingress_key = kFgwWireIngressUseLocal);

    /// Emit application bytes.  `ingress_key` selects the session map
    /// bucket (see closeSession).
    int sendSessionData(zce_uint32 session_id, const zce_byte* buf, zce_uint32 len,
                        zce_uint32 ingress_key = kFgwWireIngressUseLocal);

    /// Invoked when raw bytes arrive on a link channel.
    void onChannelBytes(IFgwChannel* ch, const zce_byte* buf, zce_uint32 len);

    /// Unknown session hook — must return a handler and/or nullptr.
    using UnknownSessionCb =
        std::function<ISessionHandler*(zce_uint32 peer_ingress, zce_uint32 session_id)>;
    void setUnknownSessionCallback(UnknownSessionCb cb) { unknown_cb_ = std::move(cb); }

    zce_uint16 segmentSize() const { return segment_size_; }
    zce_uint16 recvWindow()  const { return recv_window_; }
    zce_uint32 localIngressId() const { return local_ingress_id_; }

    zce::SmartPtr<ChannelManager> manager() const { return manager_; }

  private:
    using SessionKey = std::pair<zce_uint32, zce_uint32>;  // (ingress, session_id)

    struct DedupKey {
        zce_uint32 ingress = 0;
        zce_uint32 session = 0;
        zce_uint32 seq     = 0;
        /// Segment class: 0 = DATA, 1 = SYN, 2 = FIN. Keeps control markers in a
        /// separate dedup space from DATA so that a SYN and the first DATA
        /// segment (both legitimately seq 0) are not conflated — otherwise the
        /// first payload segment is dropped as a false duplicate of the SYN.
        zce_uint8  kind    = 0;
        bool operator<(const DedupKey& o) const {
            if (ingress != o.ingress) return ingress < o.ingress;
            if (session != o.session) return session < o.session;
            if (seq != o.seq) return seq < o.seq;
            return kind < o.kind;
        }
    };

    zce_uint32 resolveIngressKey(zce_uint32 key_arg) const {
        return key_arg == kFgwWireIngressUseLocal ? local_ingress_id_ : key_arg;
    }

    int sendSegment(zce_uint32 wire_ingress, zce_uint32 session_id, zce_uint32 seq,
                    zce_uint8 flags, const zce_byte* payload, zce_uint32 payload_len);

    void deliverInOrder(SessionState& state);

    struct ChannelScratch {
        std::vector<zce_byte> buffer;
    };

    zce::SmartPtr<ChannelManager>             manager_;
    zce_uint32                                 local_ingress_id_ = 0;
    zce_uint16                                 segment_size_;
    zce_uint16                                 recv_window_;
    bool                                       verify_crc_ = true;

    mutable zce::Mutex                         lock_;
    std::map<SessionKey, SessionState>         sessions_;
    std::map<IFgwChannel*, ChannelScratch>     scratch_;

    static constexpr size_t kDedupRing = 4096;
    std::deque<DedupKey>                       dedup_queue_;
    std::map<DedupKey, bool>                   dedup_map_;

    UnknownSessionCb unknown_cb_;
};

using DataStreamPtr = zce::SmartPtr<DataStream>;

}  // namespace zfgw
