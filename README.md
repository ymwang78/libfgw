# libfgw — Fixed-egress Gateway Library

`libfgw` (Fixed-egress GateWay) 是一个 C++ 静态库，负责在 HostVM（`zce::zvm`）
运行时中以 “虚拟机” 的形式提供稳定、快速、固定出口的多链路聚合代理通道。
它的定位与 `libident`、`libmpc` 相同 —— 作为一个可独立加载的 VM 模块被
HostVM 调度，通过 RPC 暴露控制面，通过普通 socket 提供数据面。

## 主要特性

- **三种角色**
  - **Inport**：本地入口，监听 TCP 127.0.0.1:1080，接受任意 SOCKS5 客户端。
  - **Transit**：墙外中转，**无会话状态**：去程按报文头 `outport_id`、回程按
    `ingress_id` 查表转发整段（CRC 不动），出口链路断开后按退避重连。
  - **Outport**：远端出口，终结 SOCKS5 协议并向目标主动外联；出口稳定性来自会话粘性
    （会话终生固定在同一 Outport）。
- **多链路聚合 DataStream**：UTP + TCP 任意组合，支持：
  - **探索/利用双发**：一份主用（当前最快）+ 一份轮换探路，冗余固定 2x；
    主用链路由接收端竞速结果反馈决定。
  - 重复包去重（按 `ingress_id + session_id + seq_num + 控制/数据判别位`）
    与接收窗口重排序；窗外段直接丢弃，`rx_buffer` 受 `recv_window` 硬限。
  - CRC32 分段完整性校验，包头三种宽度：16（Legacy）/ 20（含 `ingress_id`）/
    24 字节（Routed，含 `outport_id`）。
- **抗干扰链路自愈**：逐链路心跳 + 卡死探活（TCP 仍连着但静默时判定 stalled 并
  移出选路）、指数退避重连；接收端按**到达顺序竞速**统计各链路胜率，经
  `FgwLinkFeedback`（ACK + 单调 epoch）反馈给发送端切换主用链路。
- **配置持久化**：FgwConfig 通过 ZDS 序列化到 VM 目录下的 `config.zds`，
  并带 `config_version` 模式校验，拒绝按旧字段编号写出的配置。
- **HostVM 集成**：`ZfgwMachine` 继承 `zce::zvm::Machine`，
  暴露 `mlfStart / mlfStop / mlfSetConfig / mlfAddChannel /
  mlfRemoveChannel / mlfGetStatus` 等 RPC 方法。

## 模块清单

| 文件 | 说明 |
| --- | --- |
| `src/zfgw.ptl` / `zfgw.bat` | ZDL 协议描述及重生成脚本；`_proto.h` / `_pack.*` 由 **zgen 生成**，勿手改。 |
| `src/zfgw_proto.h` | `FgwEndpoint / FgwConfig / FgwStatus …` 数据结构。 |
| `src/zfgw_pack.{h,cpp}` | ZDS `zds_pack` / `zds_unpack` 实现。 |
| `src/zfgw.{h,cpp}` | 错误码、角色枚举、`zfgw_save_config` / `zfgw_load_config`、VM 工厂注册。 |
| `src/zfgw_segment.{h,cpp}` | 分段封包、CRC32、字节序处理。 |
| `src/zfgw_channel.{h,cpp}` | `IFgwChannel` 抽象 + `FgwTcpChannel` / `FgwUtpChannel`（后者需 `FGW_USE_LIBUTP`）。 |
| `src/zfgw_channel_manager.{h,cpp}` | 通道池、`LinkSelector`、心跳/重连、共享 UTP 上下文。 |
| `src/zfgw_datastream.{h,cpp}` | 多链路聚合、session 复用、去重与重排序。 |
| `src/zfgw_inport.{h,cpp}` | 本地 TCP 入口与 `FgwSession`。 |
| `src/zfgw_outport.{h,cpp}` | 远端 SOCKS5 终结 + 对外连接的 `FgwRelaySession`。 |
| `src/zfgw_transit.{h,cpp}` | `TransitService`：无状态中转转发（`outport_id` / `ingress_id` 两张表）。 |
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
          +---- LinkSelector（主用 + 轮换探路，双发）----+
                 │                                        │
            FgwUtpChannel                          FgwTcpChannel
                 │                                        │
                 └───────── (可选) Transit 无状态转发 ─────┘
                          去程按 outport_id / 回程按 ingress_id
                                    │
                            Outport 端汇聚去重
                                    │
                 FgwRelaySession（SOCKS5 server + zce::Connector）
                                    │
                                Internet
                                    │ （宿主路由 / 会话粘性固定出口）
                                    ▼
                                Target Host
```

## 配置范例（`FgwConfig`）

```text
role               = 0 (Inport) | 1 (Outport) | 2 (Transit)
inport_listen_port = 1080               // Inport: SOCKS5 监听端口
outport_listen_host / outport_listen_port
                                        // Outport / Transit: 接受入站链路的监听地址
route_outport_id   = 0                  // Inport: 0=直连，非0=经 Transit 转发到该出口
segment_size       = 1200
recv_window        = 1024               // 接收窗口（段数），同时是 rx_buffer 上限
heartbeat_interval = 5                  // seconds
link_timeout       = 15                 // seconds，超时无收包即判定链路 stalled
multipath_mode     = 0 best | 1 all | 2 探索/利用双发（默认）
config_version                          // 由 zfgw_save_config 自动盖戳，勿手填
channels           = [
    { channel_id: 1, kind: 0 (TCP), remote: a.example:5001, priority: 100 },
    { channel_id: 2, kind: 1 (UTP), remote: b.example:5002, priority: 80 },
]
```

## 构建

### 单独构建 libfgw

```bash
cmake -S modules/fgw -B modules/fgw/build -DCMAKE_BUILD_TYPE=Release
cmake --build modules/fgw/build
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

- `test_zfgw_segment`（5）—— CRC32 一致性、分段编码解码、错误包拒绝、
  24 字节 Routed 包头往返。
- `test_zfgw_pack`（5）—— `FgwEndpoint / FgwChannelConfig / FgwConfig / FgwStatus`
  的 ZDS 往返编码解码；`config_version` 模式守卫。
- `test_zfgw_datastream`（11）—— 接收路径回归：SYN/DATA seq0 去重碰撞、
  重复段只交付一次、FIN 重入不崩溃、接收窗口上限、窗外段不污染去重表、
  竞速反馈按 epoch 生效与对端重启后重置、`LinkSelector` 探索/利用选路与并发安全、
  心跳卡死判定。

> 测试目标在 CMake 中以 Linux 版 `ZCE_LIB` 路径为条件生成，目前统一在 Linux 侧运行。

## 已知待办 / 扩展点

- `FgwUtpChannel` 目前为桩实现，开启 `FGW_USE_LIBUTP` 后需要补齐 `utp_init` /
  `utp_create_socket` / `utp_connect` / `utp_issue_deferred_acks` 的完整
  调用链。
- 出口源 IP **不由本库绑定**：`egress_bind_ip` 已移除。出口稳定性来自会话粘性
  （一个会话终生固定在同一个 Outport），源地址由宿主机路由表决定；需要在多个
  本机 IP 间选择的场景请用策略路由。
- **发送端流控与重传**：接收窗口目前只做上限保护（窗外段丢弃），既没有向对端
  施加背压（`WINDOW_FULL`），也不重传窗内缺口——缺口会让该会话停住。
  `FgwLinkFeedback.highest_seq` / `alive_bitmap` 已为此预留。
- **Transit 侧竞速裁决**：三层拓扑中被审查的是 Inport→Transit 这一跳，Transit 是
  该跳的接收方，但目前只转发、不裁决，竞速反馈仍由端到端的 Outport 产生。
- **大流量下的探路开销**：当前每段都双发（2x）。大流量场景可改为采样式探路
  （每 N 段探一次）把开销压回接近 1x。
