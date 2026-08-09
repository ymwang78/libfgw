// ***************************************************************
//  zfgw_vm.cpp
//  Yongming Wang(wangym@gmail.com)
//  Copyright (C) 2026 - All Rights Reserved
// ***************************************************************
#include "zfgw_inc.h"
#include "zfgw_vm.h"
#include "zfgw.h"

#include <zce/zce_log.h>
#include <zce/zce_api.h>
#include <zce/zce_reactor.h>

namespace zfgw {

ZfgwMachine::ZfgwMachine(const std::string& name,
                         const zce::SmartPtr<zce::zvm::VirtualMachineStub>& stub_ptr)
    : Machine(name, stub_ptr) {}

ZfgwMachine::~ZfgwMachine() {
    doTeardown();
}

int ZfgwMachine::start() {
    // Attempt to load a persisted configuration; missing file is a
    // soft error — the caller can set one via mlfSetConfig later.
    const auto& stub = stub_ptr();
    if (stub) {
        int ret = zfgw_load_config(config_, stub->hostDir(), vm_name_, /*vmpath*/ "");
        if (ret < 0) {
            ZCE_DEBUG((ZLOG_INFOR, "ZfgwMachine '%s': no persisted config (ret=%d), "
                                   "waiting for mlfSetConfig.",
                       vm_name_.c_str(), ret));
            return 0;
        }
    }
    return doBringup();
}

void ZfgwMachine::stop() {
    doTeardown();
}

int ZfgwMachine::doBringup() {
    zce::Guard<zce::Mutex> g(lock_);
    if (running_) return 0;

    zce::SmartPtr<zce::Reactor> reactor;
    if (stub_ptr() && stub_ptr()->getReactorPtr()) {
        reactor = stub_ptr()->getReactorPtr();
    } else {
        reactor = zce::SmartPtr<zce::Reactor>(zce::ReactorSigt::instance());
    }
    if (!reactor) {
        ZCE_ERROR((ZLOG_ERROR, "ZfgwMachine '%s': Reactor not available", vm_name_.c_str()));
        return ZFGW_ERRCODE_NOTRUNNING;
    }

    zce::TaskQueue* sync_q = static_cast<zce::TaskQueue*>(this);
    manager_ = new ChannelManager(reactor, sync_q);
    int ret = manager_->start(config_);
    if (ret < 0) return ret;

    stream_ = new DataStream(manager_, config_.ingress_id, config_.segment_size, config_.recv_window,
                             config_.enable_crc != 0);

    if (config_.role == ZFGW_ROLE_INPORT) {
        inport_ = new InportService(reactor, stream_);
        std::string ip = "127.0.0.1";
        ret = inport_->start(ip, config_.inport_listen_port);
        if (ret < 0) {
            doTeardown();
            return ret;
        }
    } else if (config_.role == ZFGW_ROLE_OUTPORT) {
        outport_ = new OutportService(reactor, stream_, config_.egress_bind_ip);
        ret = outport_->start();
        if (ret < 0) {
            doTeardown();
            return ret;
        }
    } else {
        doTeardown();
        return ZFGW_ERRCODE_INVALIDROLE;
    }

    running_ = true;
    ZCE_DEBUG((ZLOG_INFOR, "ZfgwMachine '%s' started as role=%u",
               vm_name_.c_str(), (unsigned)config_.role));
    return 0;
}

void ZfgwMachine::doTeardown() {
    zce::Guard<zce::Mutex> g(lock_);
    if (inport_)  { inport_->stop();  inport_  = nullptr; }
    if (outport_) { outport_->stop(); outport_ = nullptr; }
    stream_ = nullptr;
    if (manager_) { manager_->stop(); manager_ = nullptr; }
    running_ = false;
}

// ─── RPC dispatch ────────────────────────────────────────────────────────────
int ZfgwMachine::call_dblock(zce_int64 /*objid*/, const std::string& method,
                             zce::RefBlock& dblock, int /*mstimeout*/,
                             const response_cb& response) {
    int ret = 0;
    do {
        FGW_RPC_CALL_1(mlfGetStatus,     FgwEmpty);
        FGW_RPC_CALL_1(mlfStart,         FgwStartRequest);
        FGW_RPC_CALL_1(mlfStop,          FgwEmpty);
        FGW_RPC_CALL_1(mlfSetConfig,     FgwSetConfigRequest);
        FGW_RPC_CALL_1(mlfAddChannel,    FgwAddChannelRequest);
        FGW_RPC_CALL_1(mlfRemoveChannel, FgwRemoveChannelRequest);

        ZCE_ERROR((ZLOG_WARNI, "ZfgwMachine '%s': unknown method %s",
                   vm_name_.c_str(), method.c_str()));
        response(ZFGW_ERRCODE_INVALIDCONFIG, zce::RefBlock());
        return 0;
    } while (0);
    return ret;
}

// ─── Individual RPC handlers ─────────────────────────────────────────────────
int ZfgwMachine::mlfGetStatus(FgwEmpty /*req*/, const response_cb& resp) {
    FgwStatus st;
    {
        zce::Guard<zce::Mutex> g(lock_);
        st.running = running_ ? 1 : 0;
        st.role    = config_.role;
        if (manager_) manager_->collectLinkQualities(st.links);

        if (inport_) {
            for (const auto& s : inport_->snapshotSessions()) {
                FgwSessionStat ss;
                ss.session_id     = s->sessionId();
                ss.ingress_id     = config_.ingress_id;
                ss.client_addr    = s->clientAddr();
                ss.target_addr    = "";
                ss.bytes_sent     = s->bytesSent();
                ss.bytes_recv     = s->bytesRecv();
                ss.open_timestamp = s->openTimestamp();
                st.sessions.push_back(std::move(ss));
            }
        } else if (outport_) {
            for (const auto& s : outport_->snapshotSessions()) {
                FgwSessionStat ss;
                ss.session_id     = s->sessionId();
                ss.ingress_id     = s->peerIngressId();
                ss.client_addr    = "";
                ss.target_addr    = s->targetAddr();
                ss.bytes_sent     = s->bytesSent();
                ss.bytes_recv     = s->bytesRecv();
                ss.open_timestamp = s->openTimestamp();
                st.sessions.push_back(std::move(ss));
            }
        }
        st.session_count = (zce_uint32)st.sessions.size();
    }
    return sendResponse(resp, st);
}

int ZfgwMachine::mlfStart(FgwStartRequest req, const response_cb& resp) {
    if (running_ && !req.force_restart) {
        FgwStartResult r;
        r.errcode = ZFGW_ERRCODE_BUSY;
        r.message = "already running";
        return sendResponse(resp, r);
    }
    if (running_ && req.force_restart) doTeardown();
    int ret = doBringup();

    FgwStartResult r;
    r.errcode = ret;
    r.message = ret < 0 ? "failed" : "ok";
    return sendResponse(resp, r);
}

int ZfgwMachine::mlfStop(FgwEmpty /*req*/, const response_cb& resp) {
    doTeardown();
    FgwStartResult r;
    r.errcode = 0;
    r.message = "stopped";
    return sendResponse(resp, r);
}

int ZfgwMachine::mlfSetConfig(FgwSetConfigRequest req, const response_cb& resp) {
    {
        zce::Guard<zce::Mutex> g(lock_);
        config_ = req.config;
    }
    const auto& stub = stub_ptr();
    if (stub) {
        int wret = zfgw_save_config(config_, stub->hostDir(), vm_name_, "");
        if (wret < 0) {
            ZCE_ERROR((ZLOG_WARNI, "ZfgwMachine '%s': save config failed ret=0x%x",
                       vm_name_.c_str(), wret));
        }
    }
    FgwStartResult r;
    r.errcode = 0;
    r.message = "config updated";
    return sendResponse(resp, r);
}

int ZfgwMachine::mlfAddChannel(FgwAddChannelRequest req, const response_cb& resp) {
    int ret = ZFGW_ERRCODE_NOTRUNNING;
    {
        zce::Guard<zce::Mutex> g(lock_);
        if (manager_) ret = manager_->addChannel(req.channel);
        if (ret >= 0) {
            config_.channels.push_back(req.channel);
        }
    }
    FgwStartResult r;
    r.errcode = ret;
    r.message = ret < 0 ? "failed" : "ok";
    return sendResponse(resp, r);
}

int ZfgwMachine::mlfRemoveChannel(FgwRemoveChannelRequest req, const response_cb& resp) {
    int ret = ZFGW_ERRCODE_NOTRUNNING;
    {
        zce::Guard<zce::Mutex> g(lock_);
        if (manager_) ret = manager_->removeChannel(req.channel_id);
        if (ret >= 0) {
            for (auto it = config_.channels.begin(); it != config_.channels.end();) {
                if (it->channel_id == req.channel_id) it = config_.channels.erase(it);
                else ++it;
            }
        }
    }
    FgwStartResult r;
    r.errcode = ret;
    r.message = ret < 0 ? "failed" : "ok";
    return sendResponse(resp, r);
}

}  // namespace zfgw
