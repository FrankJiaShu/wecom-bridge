# wecom-bridge — 企业微信智能机器人 ↔ 本机 omp 长连接桥

企业微信智能机器人（智能机器人 API 模式·WebSocket 长连接）与本地 omp 编码助手之间的桥接服务。
微信里 @机器人 提问 → 本机 omp 处理 → 流式回复到微信。**无需公网 IP**，整条链路跑在你自己电脑上。

> 协议依据：[企业微信官方·智能机器人长连接](https://developer.work.weixin.qq.com/document/path/101463)
> 工程参数参考：Hermes Agent 的 wecom 适配器（`plugins/platforms/wecom/`）实测值

## 架构

```
企业微信 wss://openws.work.weixin.qq.com
        │  aibot_subscribe (BotID+Secret+device_id)
        │  30s 应用层 ping + 协议层 ping
        ▼
wecom-bridge.mjs（Node 常驻进程，127.0.0.1）
  ├─ 收：aibot_msg_callback / aibot_event_callback
  │     msgid 去重 → 群聊 @前缀剥离 → 长文本分片合并(0.6s/硬上限2s)
  │     → 写 inbox.jsonl → 交给大脑
  ├─ 大脑 omp-brain.mjs：每个聊天一个 `omp --mode rpc` 子进程
  │     --profile wecombot（MCP/配置与你的日常 omp 完全隔离）
  │     --no-context-files（不加载任何 AGENTS.md 提示词）
  │     --session-dir workspace/sessions/<chatid>（每聊天独立记忆）
  └─ 回：aibot_respond_msg 流式（text_delta 节流 1.2s 推送，finish=true 收尾）
        流窗死亡(846608/846604) → 降级 aibot_send_msg 主动推送
```

## 文件清单

```
wecom-bridge/
├─ src/                    后端
│  ├─ wecom-bridge.mjs        桥接主程序：WSS 长连接 + 收发协议 + HTTP API + 控制面板 + 会话清扫
│  └─ omp-brain.mjs           omp 大脑：每聊天一个 omp RPC 子进程，流式转发，群名/发送者上下文注入
├─ public/
│  └─ panel.html              控制面板前端（http://127.0.0.1:8765/）
├─ scripts/                 启停脚本
│  ├─ start-bridge.bat        启动（清停止标志 → 无窗口拉起守护）
│  ├─ stop-bridge.bat         停止（写 bridge.stop 标志 + 杀进程，守护见标志一并退出）
│  ├─ run-hidden.vbs          无窗口启动器（start-bridge.bat 调用）
│  └─ bridge-forever.cmd      守护循环：崩溃 5s 自动重启；端口被占不重复拉起
├─ logs/                   运行日志（gitignore）：bridge.log 超 5MB 轮转 .old、bridge-crash.log
├─ data/                   运行数据（gitignore）：inbox.jsonl 超 20MB 轮转 .old
├─ workspace/sessions/     每聊天 omp 会话存储（gitignore，含隐私勿外传）
├─ .env                    凭证配置（gitignore，勿提交；CLI 参数可覆盖，见下文）
├─ .env.example            配置模板
├─ bridge.stop             停止标志（运行时生成，守护消费后自删）
├─ package.json / package-lock.json
└─ README.md
```

## 快速开始

```bat
:: 启动（双击 scripts\start-bridge.bat 也一样）：完全无窗口后台运行，不残留任何黑框
cd /d D:\gdmp\wecom-bridge
scripts\start-bridge.bat   :: 显示"Started OK"即可；重复执行会提示已在运行

:: 停止（唯一停止方式；直接关窗口/任务管理器杀进程也行但不推荐）
scripts\stop-bridge.bat

:: 控制面板（浏览器打开）
start http://127.0.0.1:8765/

:: 看状态（另开一个终端）
curl http://127.0.0.1:8765/health
:: 期望: {"ws_open":true,"subscribed":true, ...}

:: 看机器人收到了什么
type data\inbox.jsonl
```

## 控制面板

浏览器打开 **http://127.0.0.1:8765/**（仅本机可访问）：

- **状态灯**：长连接 / 大脑实时状态 + 运行时长
- **模型 / 思考强度**：下拉切换（读自 `~/.omp/profiles/wecombot/agent/`），点"应用并热更新"写入 profile 并回收聊天子进程，下一条微信消息即用新配置
- **会话管理**：列出每个聊天的 omp 会话（大小/最后活跃/文本预览/活跃标记），勾选批量删除（清空该聊天的记忆）
- **消息流**：最近 100 条微信消息实时滚动
- **停止服务**：面板内直接下线机器人（面板由桥服务承载，停止后面板也随之失效）

验证方式：企业微信里给机器人发消息（单聊直接发；群聊要 @机器人）。

## 配置（CLI 参数 > .env > 默认）

配置三层优先级：**命令行参数 > .env 文件/环境变量 > 内置默认**。全部参数看 `node wecom-bridge.mjs --help`。

| 参数 / .env 键 | 说明 | 默认 |
|---|---|---|
| `--bot-id` / `WECOM_BOT_ID`，`--secret` / `WECOM_SECRET` | 管理后台→智能机器人→API模式→**长连接** 页面获取的凭证（长连接专用 Secret ≠ 回调模式的 Token/AESKey）。secret 放命令行会出现在进程列表，**推荐放 .env** | 必填 |
| `--port` / `BRIDGE_PORT` | 本地 HTTP API 端口（仅监听 127.0.0.1） | `8765` |
| `--brain` / `BRIDGE_BRAIN` | `omp` = 启用 omp 大脑；不设则退回回声模式 | `omp` |
| `--echo` / `BRIDGE_ECHO` | `1` = 回声测试（收到啥回 `echo: ...`）；大脑启用时不生效 | 关 |
| `--omp-args` / `OMP_ARGS` | 传给 omp 子进程的额外参数（见下方安全说明） | 空 |
| `--omp-cmd` / `OMP_CMD` | omp 可执行文件 | `omp`（PATH 查找） |
| `--omp-approval` / `OMP_APPROVAL_MODE` | 审批模式（会被 OMP_ARGS 覆盖，二选一即可） | `yolo` |
| `--welcome` / `WECOM_WELCOME_TEXT` | 进入单聊会话的欢迎语，留空不回复 | 空 |
| `--ws-url` / `WECOM_WEBSOCKET_URL` | WSS 地址 | 官方地址 |
| `--inbox` / `INBOX_FILE` | 收件箱 JSONL 路径 | `data/inbox.jsonl` |
| `--log-max` / `BRIDGE_LOG_MAX` | 日志轮转阈值（字节） | 5MB |
| `--session-max-days` / `SESSION_MAX_DAYS` | 会话闲置自动清理天数 | `14` |
| `--workspace` | omp 子进程工作目录 | `workspace/` |
| `--profile-dir` / `OMP_PROFILE_DIR` | bot 的 omp profile 目录 | `~/.omp/profiles/wecombot/agent` |

示例：临时换个端口跑一个测试实例（注意第二个实例会踢掉第一个的长连接）：

```
node wecom-bridge.mjs --port 8766 --brain omp
```

**改任何配置后必须重启**（stop 再 start）。

## omp 大脑的隔离设计（不影响你原有的 omp）

| 你原有的东西 | 隔离手段 |
|---|---|
| omp 会话列表 / 历史 | 子进程带独立 `--session-dir`，bot 会话完全不存在于你的列表里 |
| 用户级 MCP（openidea/dbx/mysql/reqable…） | 子进程用独立 profile `wecombot`（`--profile`），看不到默认配置的任何 MCP |
| 问数 AGENTS.md 等提示词 | `--no-context-files`，bot 是"普通 omp"，无任何域守卫约束 |
| 模型配置 | 只读复用同一份 `models.yml`（GLM），复制在 profile 内；额度共享 |
| 文件系统 | 子进程 cwd 固定在 `workspace/` |

- 每个聊天（群 chatid / 单聊 userid）一个 omp 子进程，**追问有记忆**（会话落盘，跨重启可续）。
- 空闲 **15 分钟**自动回收子进程，下次消息自动拉起并续上同一会话。
- 并发上限 4 个聊天进程（LRU 回收）。
- 模型默认 `cc-switch-zhipu-glm/glm-5.3-flash`，改 `~/.omp/profiles/wecombot/agent/config.yml` 里的 `model:` 行。

## ⚠️ 安全须知

当前 `OMP_ARGS=--approval-mode yolo`：**能在微信里 @到机器人的人，可以驱动它在你电脑上执行任意命令、改文件**（omp 自动批准一切工具调用）。

个人测试群无所谓；有外人就收紧（改 `.env` 后重启）：

```
OMP_ARGS=--profile wecombot --tools read,grep,glob,web_search --approval-mode write
```

## 长期运行稳定性（设计为可持续运行数周不用管）

| 机制 | 说明 |
|---|---|
| 崩溃自愈 | `bridge-forever.cmd` 守护循环：node 异常退出 5 秒后自动重启；端口已被占则不重复拉起 |
| 停止协议 | `bridge.stop` 标志文件：stop-bridge.bat / 面板"停止"写入 → node 退出 → 守护消费标志后一并退出（不会越杀越活） |
| 全局异常兜底 | `uncaughtException` / `unhandledRejection` 记日志不退出，单个回调出错不影响整体服务 |
| 日志轮转 | `logs/bridge.log` 超 5MB 自动转 `.old`（`--log-max` 可调）；inbox 超 20MB 转 `.old` |
| 会话自动清理 | 目录内最新文件超 `--session-max-days`（默认 14 天）未更新的 omp 会话自动删除（启动时 + 每 6 小时扫描） |
| WS 长连接 | 断线指数退避重连 `[2,5,10,30,60]s`；30s 心跳；订阅超时自动重连；被踢（disconnected_event）抑制重连防互踢 |
| omp 子进程 | 空闲 15 分钟回收；回合 10 分钟看门狗；子进程崩溃自动提示重发并下次拉起续会话 |
| fd 泄漏防护 | 所有同步文件句柄 try/finally 关闭；面板读取失败会在 404 响应和日志里给出错误码 |
| 开机自启 | 启动文件夹放 `wecom-bridge-autostart.vbs`（`shell:startup`），登录后自动拉起 |

## 已处理的协议坑（官方文档没写的）

| 坑 | 处理 |
|---|---|
| 心跳必须客户端主动发 | 每 30s 应用层 `{"cmd":"ping"}` + 协议层 ping，否则连接被回收 |
| 每机器人只允许 1 条长连接，新连接踢旧连接 | 收到 `disconnected_event` 后**抑制重连**（避免两实例互踢死循环）；Hermes gateway 同理 |
| 手机端把长文本拆成多条发送 | 0.6s 合并窗（硬上限 2s），纯附件再等 0.8s 等尾随文本 |
| 流式回复窗口实际约 6 分钟（不是文档的 10 分钟），连接 ping 不续期 | 回合看门狗 10 分钟；`finish` 遇 846608/846604/846609 → 降级 `aibot_send_msg` 主动推送 |
| final 帧 ack 超时 | 视为已送达不重发（重发会产生重复消息，对齐官方插件行为） |
| 消息重复推送 | 按 `msgid` 去重（FIFO 1000 条） |
| Node 内置 WebSocket(undici) 对 openws 握手失败 | 改用 `ws` 包（唯一依赖） |
| 回答生成中又来一条消息（omp 拒绝并发 prompt） | 桥内按聊天 FIFO 排队：忙时回执"已排队"（前 2 条），回合结束自动处理下一条；队列上限 10 条 |
| 想终止正在生成的回答 | 微信里发 `停止` / `stop` / `取消`（回答中才生效）→ 终止当前回合并清空排队；控制台"活跃聊天进程"卡片里也有**终止**按钮 |
| 假凭证/凭证错 | 服务端回 `853000 invalid bot_id or secret`，按退避 `[2,5,10,30,60]s` 重试 |

## 本地 HTTP API（127.0.0.1:8765）

```
GET  /health   → {ws_open, subscribed, suppressed, bot_id, echo, inbox, last_inbound}
POST /reply    → {"req_id","content","msgtype"?:"stream"|"markdown","stream_id"?,"finish"?}
                 req_id 必须透传 inbox.jsonl 里对应消息的 req_id
POST /send     → {"chat_id","chat_type":1|2,"content"} 主动推送
                 前提：用户先给机器人发过消息；群聊支持情况以服务端为准
```

## 常见问题

- **机器人没反应**：先 `/health` 看 `subscribed` 是否 true；再看 `data/inbox.jsonl` 有没有进消息；都没问题看 `logs/bridge.log` 里 `BRAIN` 行。
- **提示已在运行**：端口 8765 被占，`netstat -ano | findstr :8765` 找 pid，确认是桥就别再启；不是就 taskkill。
- **想切回 Hermes 的企微通道**：先 `stop-bridge.bat`，再重启 Hermes gateway（两边只能活一个，谁后连谁踢对方）。
- **改了 .env 不生效**：必须重启（stop + start）。
- **首次回复慢 5~10 秒**：该聊天的 omp 子进程冷启动，属正常；同聊天后续消息走热进程。

## 参考

- 企业微信智能机器人长连接：https://developer.work.weixin.qq.com/document/path/101463
- 接收消息（长连接）：https://developer.work.weixin.qq.com/document/62155
- omp RPC 协议 / SDK：omp 安装目录文档（`omp://rpc.md`、`omp://sdk.md`）
- Hermes Agent（工程参数参考）：`C:\Users\liujh-r\AppData\Local\hermes\hermes-agent\plugins\platforms\wecom\`
