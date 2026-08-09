// ***************************************************************
//  zfgw_inc.h  — internal-use macros shared by libfgw sources.
//  Yongming Wang(wangym@gmail.com)
//  Copyright (C) 2026 - All Rights Reserved
// ***************************************************************
#pragma once

#include <zce/zce_config.h>
#include <zce/zdp_base_pack.h>
#include <zce/zce_mbpool.h>
#include <zce/zds_schema.h>
#include <zce/zvm.h>
#include "zfgw_pack.h"

// ── Unpack single argument from dblock (mirrors zident_inc.h) ────────────────

#define FGW_UNPACK_ARG(arg)                                                         \
    unpack_ret = ::zfgw::zds_unpack_auto(arg, dblock);                              \
    if (unpack_ret < 0) {                                                           \
        ZCE_ERROR((ZLOG_ERROR, "unpack %s failed, ret=0x%x\n", #arg, unpack_ret));  \
        response(unpack_ret, zce::RefBlock());                                      \
        return unpack_ret;                                                          \
    }

// ── RPC dispatch helpers (handler style from zmpc_inc.h / zident_inc.h) ──────

#define FGW_RPC_CALL_0(FUNC)                \
    if (method == #FUNC) {                  \
        ret = FUNC(response);               \
        break;                              \
    }

#define FGW_RPC_CALL_1(FUNC, REQ)                     \
    if (method == #FUNC) {                            \
        REQ arg{};                                    \
        int unpack_ret = 0;                           \
        FGW_UNPACK_ARG(arg);                          \
        ret = FUNC(std::move(arg), response);         \
        break;                                        \
    }

namespace zfgw {

using response_cb = zce::zvm::VirtualMachineStub::response_cb;

template <class T>
inline int zds_unpack_auto(T& a, ::zce::RefBlock& dblock) {
    if constexpr (zce::zdp::is_builtin_type<std::decay_t<T>>()) {
        return zce::zdp::zds_unpack_builtin(a, dblock.rd_ptr(), (int)dblock.length(), nullptr);
    } else {
        return zce::zdp::zds_unpack(a, dblock.rd_ptr(), (int)dblock.length(), nullptr, true);
    }
}

}  // namespace zfgw
