# libfgw — Fixed-egress Gateway Library

`libfgw` (Fixed-egress GateWay) 是一个 C++ 静态库，负责在 HostVM（`zce::zvm`）
运行时中以 “虚拟机” 的形式提供稳定、快速、固定出口的多链路聚合代理通道。
它的定位与 `libident`、`libmpc` 相同 —— 作为一个可独立加载的 VM 模块被
HostVM 调度，通过 RPC 暴露控制面，通过普通 socket 提供数据面。

## 主要特性

- **双端角色**
  - **Inport**：本地入口，监听 TCP 127.0.0.1:1080，接受任意 SOCKS5 客户端。
  - **Outport**：远端出口，终结 SOCKS5 协议并以固定 egress IP 主动外联。
- **多链路聚合 DataStream**：UTP + TCP 任意组合，支持：
  - 加权最短路径的链路选择（RTT / 丢包 / 优先级）。
  - 重复包去重（按 `session_id + seq_num`）与接收窗口重排序。
  - CRC32 分段完整性校验（16 字节定长包头 + 负载）。
- **链路自愈**：心跳检测、指数退避重连。
- **配置持久化**：FgwConfig 通过 ZDS 序列化到 VM 目录下的 `config.zds`。
- **HostVM 集成**：`ZfgwMachine` 继承 `zce::zvm::Machine`，
  暴露 `mlfStart / mlfStop / mlfSetConfig / mlfAddChannel /
  mlfRemoveChannel / mlfGetStatus` 等 RPC 方法。

## 模块清单

| 文件 | 说明 |
| --- | --- |
| `src/zfgw.ptl` / `zfgw.bat` | ZDL 协议描述及重生成脚本（手写对应 `_proto.h` / `_pack.*`）。 |
| `src/zfgw_proto.h` | `FgwEndpoint / FgwConfig / FgwStatus …` 数据结构。 |
| `src/zfgw_pack.{h,cpp}` | ZDS `zds_pack` / `zds_unpack` 实现。 |
| `src/zfgw.{h,cpp}` | 错误码、角色枚举、`zfgw_save_config` / `zfgw_load_config`、VM 工厂注册。 |
| `src/zfgw_segment.{h,cpp}` | 分段封包、CRC32、字节序处理。 |
| `src/zfgw_channel.{h,cpp}` | `IFgwChannel` 抽象 + `FgwTcpChannel` / `FgwUtpChannel`（后者需 `FGW_USE_LIBUTP`）。 |
| `src/zfgw_channel_manager.{h,cpp}` | 通道池、`LinkSelector`、心跳/重连、共享 UTP 上下文。 |
| `src/zfgw_datastream.{h,cpp}` | 多链路聚合、session 复用、去重与重排序。 |
| `src/zfgw_inport.{h,cpp}` | 本地 TCP 入口与 `FgwSession`。 |
| `src/zfgw_outport.{h,cpp}` | 远端 SOCKS5 终结 + 对外连接的 `FgwRelaySession`。 |
| `src/zfgw_vm.{h,cpp}` | `ZfgwMachine`：生命周期、RPC 分发与状态聚合。 |
| `src/zfgw_inc.h` | RPC 参数解包与分发辅助宏。 |
| `gtest/*` | 包头/CRC、ZDS 打包的单元测试。 |

## 典型数据面流向

```
SOCKS5 Client
    │  (127.0.0.1:1080)
    ▼
Inport / FgwSession  ── raw bytes ──┐
                                    │
                        DataStream (session multiplex + CRC + seq)
                                    │
                 +------- LinkSelector (weighted) -------+
                 │                                        │
            FgwUtpChannel                          FgwTcpChannel
                 │                                        │
                 └──────────────── Outport 端重组 ────────┘
                                    │
                 FgwRelaySession（SOCKS5 server + zce::Connector）
                                    │
                                Internet
                                    │ （固定 egress IP）
                                    ▼
                                Target Host
```

## 配置范例（`FgwConfig`）

```text
role               = 0 (Inport) | 1 (Outport)
inport_listen_port = 1080
egress_bind_ip     = "1.2.3.4"          // Outport 固定出口
segment_size       = 1200
recv_window        = 1024
heartbeat_interval = 5                  // seconds
multipath_mode     = 0 best | 1 all | 2 weighted
channels           = [
    { channel_id: 1, kind: 0 (TCP), remote: a.example:5001, priority: 100 },
    { channel_id: 2, kind: 1 (UTP), remote: b.example:5002, priority: 80 },
]
```

## 构建

### 单独构建 libfgw

```bash
cmake -S libsrc/libfgw -B libsrc/libfgw/build -DCMAKE_BUILD_TYPE=Release
cmake --build libsrc/libfgw/build
```

选项：

- `FGW_ENABLE_UTP` — 打开后使用真正的 `libutp`，默认关闭（使用桩实现）。
- `FGW_BUILD_TESTS` — 默认开启，构建 `gtest/` 下的单元测试。

### 与 HostVM 集成

HostVM 启动后会通过 `VirtualMachineRegister` 查找 `"fgw"` 类型虚拟机并
实例化 `ZfgwMachine`。每个 VM 目录下会自动生成 `config.zds`，可以通过
`mlfSetConfig` 更新后热重启。

## RPC 方法

| method | request | response | 说明 |
| --- | --- | --- | --- |
| `mlfStart`        | `FgwStartRequest` | `FgwStartResult` | 按当前配置启动服务，支持 `force_restart`。 |
| `mlfStop`         | `FgwEmpty`        | `FgwStartResult` | 停止服务。 |
| `mlfSetConfig`    | `FgwSetConfigRequest` | `FgwStartResult` | 覆盖当前配置并落盘，需再次 `mlfStart` 生效。 |
| `mlfAddChannel`   | `FgwAddChannelRequest` | `FgwStartResult` | 运行时动态追加一条链路。 |
| `mlfRemoveChannel`| `FgwRemoveChannelRequest` | `FgwStartResult` | 动态摘除链路。 |
| `mlfGetStatus`    | `FgwEmpty`        | `FgwStatus` | 聚合链路状态与活跃 session 统计。 |

## 单元测试覆盖

- `test_zfgw_segment` —— CRC32 一致性、分段编码解码、错误包拒绝。
- `test_zfgw_pack`    —— `FgwEndpoint / FgwChannelConfig / FgwConfig / FgwStatus`
  的 ZDS 往返编码解码，验证字段级等价。

## 已知待办 / 扩展点

- `FgwUtpChannel` 目前为桩实现，开启 `FGW_USE_LIBUTP` 后需要补齐 `utp_init` /
  `utp_create_socket` / `utp_connect` / `utp_issue_deferred_acks` 的完整
  调用链。
- `egress_bind_ip` 在 `zfgw_outport.cpp` 内对应的 `zce::Tcp` 绑定逻辑当前依赖
  宿主 `uv_tcp_bind` — 需要在 `libzce` 中暴露 bind 接口或使用 `zce::Acceptor`
  的反向绑定能力。
- Outport listening（被动接收来自 Inport 的 UTP/TCP 链路）尚未在
  `InportService` / `OutportService` 中提供自动对称构建，目前由
  `FgwConfig.channels` 主动 dial。
