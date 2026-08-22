// ***************************************************************
//  zfgw.cpp  — module bootstrap + config persistence helpers.
//  Yongming Wang(wangym@gmail.com)
//  Copyright (C) 2026 - All Rights Reserved
// ***************************************************************
#include "zfgw.h"
#include "zfgw_vm.h"
#include "zfgw_inc.h"

#include <zce/zvm.h>
#include <zce/zce_log.h>
#include <zce/zce_filesystem.h>
#include <zce/zce_mbpool.h>
#include <cstdio>
#include <cstring>

#include <zce/zvm_module.h>

namespace zfgw {

// ─── Config file path helpers ────────────────────────────────────────────────

std::string fgwConfigFilePath(const std::string& host_dir, const std::string& vmname,
                              const std::string& vmpath) {
    std::string dir = host_dir;
    if (!dir.empty() && dir.back() != '/' && dir.back() != '\\') dir += '/';
    if (!vmpath.empty()) {
        dir += vmpath;
        if (dir.back() != '/' && dir.back() != '\\') dir += '/';
    }
    dir += vmname;
    dir += "/config.zds";
    return dir;
}

}  // namespace zfgw

// ─── Public C-style helpers (declared in zfgw.h) ─────────────────────────────

std::string getFgwVMRelativeFilePath(const std::string& host_dir, const std::string& vmname,
                                     const std::string& vmpath) {
    return zfgw::fgwConfigFilePath(host_dir, vmname, vmpath);
}

int zfgw_save_config(const zfgw::FgwConfig& config, const std::string& host_dir,
                     const std::string& vmname, const std::string& vmpath) {
    std::string path = zfgw::fgwConfigFilePath(host_dir, vmname, vmpath);

    // Stamp the schema version here — the single choke point for persisting a
    // config — so no caller can write a file a future loader would misread.
    zfgw::FgwConfig stamped = config;
    stamped.config_version = zfgw::kFgwConfigVersion;

    int need = zce::zdp::zds_pack(nullptr, 0, stamped, nullptr, true);
    if (need < 0) return need;

    zce::RefBlock dblock;
    ZCE_MBACQUIRE(dblock, need);
    if ((int)dblock.space() < need) return ZFGW_ERRCODE_MEMORY;
    int wrote = zce::zdp::zds_pack(dblock.wr_ptr_cow(), (int)dblock.space(), stamped,
                                   nullptr, true);
    if (wrote < 0) return wrote;
    dblock.wr_ptr(wrote);

    FILE* fp = std::fopen(path.c_str(), "wb");
    if (!fp) return ZFGW_ERRCODE_PATHFILEWRITE;
    size_t n = std::fwrite(dblock.rd_ptr(), 1, (size_t)wrote, fp);
    std::fclose(fp);
    return n == (size_t)wrote ? 0 : ZFGW_ERRCODE_PATHFILEWRITE;
}

int zfgw_load_config(zfgw::FgwConfig& config, const std::string& host_dir,
                     const std::string& vmname, const std::string& vmpath) {
    std::string path = zfgw::fgwConfigFilePath(host_dir, vmname, vmpath);

    FILE* fp = std::fopen(path.c_str(), "rb");
    if (!fp) return ZFGW_ERRCODE_PATHFILEREAD;

    std::fseek(fp, 0, SEEK_END);
    long len = std::ftell(fp);
    std::fseek(fp, 0, SEEK_SET);
    if (len <= 0) {
        std::fclose(fp);
        return ZFGW_ERRCODE_PATHFILEREAD;
    }
    zce::RefBlock dblock;
    ZCE_MBACQUIRE(dblock, (int)len);
    if ((int)dblock.space() < (int)len) {
        std::fclose(fp);
        return ZFGW_ERRCODE_MEMORY;
    }
    size_t n = std::fread(dblock.wr_ptr_cow(), 1, (size_t)len, fp);
    std::fclose(fp);
    if (n != (size_t)len) return ZFGW_ERRCODE_PATHFILEREAD;
    dblock.wr_ptr((int)n);

    zfgw::FgwConfig decoded;
    int consumed = zce::zdp::zds_unpack(decoded, dblock.rd_ptr(), (int)dblock.length(),
                                        nullptr, true);
    if (consumed < 0) return consumed;

    // Reject a file written under a different field numbering. A pre-v0.3.0
    // config carries no version (0) and its bits 5+ denote other fields — some
    // of which share a type with what sits there now, so it would decode
    // "successfully" with silently shifted values. Refuse instead of guessing.
    if (!zfgw::fgwConfigVersionOk(decoded.config_version)) {
        ZCE_ERROR((ZLOG_ERROR,
                   "fgw: %s has config_version 0x%08X, expected 0x%08X — "
                   "incompatible layout, regenerate the config",
                   path.c_str(), decoded.config_version, zfgw::kFgwConfigVersion));
        return ZFGW_ERRCODE_INVALIDCONFIG;
    }

    config = std::move(decoded);
    return 0;
}

// ─── VM registration ────────────────────────────────────────────────────────

static zce::zvm::VirtualMachineRegister _zfgw_register(
    "fgw",
    [](const zdp_base::zvm_t& vm,
       const zce::SmartPtr<zce::zvm::VirtualMachineStub>& stub_ptr,
       zce::RefBlock& /*args*/) -> zce::SmartPtr<zce::zvm::Machine> {
        return zce::SmartPtr<zce::zvm::Machine>(new zfgw::ZfgwMachine(vm.vmname, stub_ptr));
    });

// ─── Optional module-level init hook (mirrors zmpc_init) ─────────────────────

int zfgw_init(const struct zce::AppOptions* /*start_t*/) {
    // The VirtualMachineRegister static above handles VM registration;
    // this hook is kept for symmetry with libmpc so hosts that call
    // library-level init can still link against libfgw without
    // resorting to a bare symbol reference.
    ZCE_DEBUG((ZLOG_INFOR, "libfgw initialised"));
    return 0;
}
