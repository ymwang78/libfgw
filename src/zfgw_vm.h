// ***************************************************************
//  zfgw_vm.h  — HostVM (zce::zvm::Machine) integration.
//
//  ZfgwMachine is the glue layer between the libfgw data-plane
//  components (ChannelManager / DataStream / Inport / Outport) and
//  the ZCE virtual-machine hosting framework.  Each VM instance is
//  either an Inport (client) or Outport (server) role; the role and
//  topology are provided through the persisted FgwConfig.
//
//  Yongming Wang(wangym@gmail.com)
//  Copyright (C) 2026 - All Rights Reserved
// ***************************************************************
#pragma once

#include <zce/zvm.h>
#include <zce/zce_sync.h>
#include "zfgw_channel_manager.h"
#include "zfgw_datastream.h"
#include "zfgw_inport.h"
#include "zfgw_outport.h"
#include "zfgw_transit.h"
#include "zfgw_proto.h"

namespace zfgw {

using response_cb = zce::zvm::VirtualMachineStub::response_cb;

/// ZCE virtual machine of type "fgw".
///
/// Lifecycle:
///   * Constructed by the VirtualMachineRegister factory.
///   * ZfgwMachine::start() reads the persisted FgwConfig from the
///     VM directory and, based on role, brings up the matching
///     Inport / Outport service plus the ChannelManager.
///   * RPC methods (mlfGetStatus, mlfStart, mlfStop, mlfSetConfig,
///     mlfAddChannel, mlfRemoveChannel) are dispatched through
///     call_dblock() using the FGW_RPC_CALL_* macros.
class ZfgwMachine : public zce::zvm::Machine {
  public:
    ZfgwMachine(const std::string& name,
                const zce::SmartPtr<zce::zvm::VirtualMachineStub>& stub_ptr);
    ~ZfgwMachine() override;

    int  start() override;
    void stop() override;

    int  call_dblock(zce_int64 objid, const std::string& method, zce::RefBlock& dblock,
                     int mstimeout,
                     const zce::zvm::VirtualMachineStub::response_cb& response) override;

    const FgwConfig& config() const { return config_; }

  private:
    // ── RPC handlers ────────────────────────────────────────────────────────
    int mlfGetStatus   (FgwEmpty req, const response_cb& resp);
    int mlfStart       (FgwStartRequest req, const response_cb& resp);
    int mlfStop        (FgwEmpty req, const response_cb& resp);
    int mlfSetConfig   (FgwSetConfigRequest req, const response_cb& resp);
    int mlfAddChannel  (FgwAddChannelRequest req, const response_cb& resp);
    int mlfRemoveChannel(FgwRemoveChannelRequest req, const response_cb& resp);

    int  doBringup();
    void doTeardown();

    mutable zce::Mutex                          lock_;
    FgwConfig                                   config_;
    zce::SmartPtr<ChannelManager>               manager_;
    zce::SmartPtr<DataStream>                   stream_;
    zce::SmartPtr<InportService>                inport_;
    zce::SmartPtr<OutportService>               outport_;
    zce::SmartPtr<TransitService>               transit_;
    bool                                        running_ = false;
};

using ZfgwMachinePtr = zce::SmartPtr<ZfgwMachine>;

}  // namespace zfgw
