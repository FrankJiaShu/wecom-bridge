#!/usr/bin/env node
// wecom-bridge.mjs — 企业微信智能机器人 长连接桥（Node >= 22 + ws 包，npm i ws）
// 弃用 Node 内置 WebSocket（undici）：对 openws 端点握手报 "non-101"，curl 同头重放却返回 101，
// 属 undici WS 实现兼容性问题；ws 包实测可用。
// 协议: https://developer.work.weixin.qq.com/document/path/101463
// 工程参数对齐 Hermes wecom adapter（plugins/platforms/wecom/adapter.py、streaming.py）实测值:
//   应用层 ping 30s、退避 [2,5,10,30,60]s、disconnected_event 抑制重连（防互踢）、
//   msgid 去重(1000)、长文本分片合并 0.6s/硬上限 2s、纯附件等尾随文本 0.8s、
//   final ack 超时视为已送达、流错误码 846608/846604/846609 降级 /send。
//
// 配置三层优先级：命令行参数 > .env 文件/进程环境变量 > 内置默认
// 命令行参数（node wecom-bridge.mjs --help 查看全部）:
//   --bot-id / --secret           必填（或用 .env；secret 走命令行会出现在进程列表，推荐 .env）
//   --ws-url --port --welcome --echo --brain --inbox --workspace --profile-dir
//   --log-max --session-max-days --omp-cmd --omp-args --omp-approval
// 环境变量（.env 文件或进程 env，.env 同目录优先级低于进程 env）:
//   WECOM_BOT_ID / WECOM_SECRET   必填；管理后台-智能机器人-API模式-长连接 页面获取
//   WECOM_WEBSOCKET_URL           默认 wss://openws.work.weixin.qq.com
//   BRIDGE_PORT                   本地 HTTP API 端口（仅监听 127.0.0.1），默认 8765
//   WECOM_WELCOME_TEXT            进入会话欢迎语，留空不回复
//   BRIDGE_ECHO                   =1 回声测试模式：收到的文本原样流式回回（首次联调）
//   INBOX_FILE                    收件箱 JSONL 路径，默认 ./data/inbox.jsonl
//   BRIDGE_LOG_MAX                日志轮转阈值字节，默认 5MB
//   SESSION_MAX_DAYS              会话闲置自动清理天数，默认 14
//   OMP_CMD / OMP_ARGS / OMP_APPROVAL_MODE / OMP_PROFILE_DIR
//
// HTTP API（127.0.0.1）:
//   GET  /health
//   POST /reply  {"req_id","content","msgtype"?:"stream"|"markdown","stream_id"?,"finish"?}
//                req_id 必须透传 inbox 记录里的 req_id；流式同一回合复用同一 stream_id
//   POST /send   {"chat_id","chat_type"?:1|2,"content"}  主动推送（需用户先给机器人发过消息；群聊支持存疑）

import fs from "node:fs";
import path from "node:path";
import http from "node:http";
import crypto from "node:crypto";
import os from "node:os";
import { parseArgs } from "node:util";
import WebSocket from "ws";
import { OmpBrain, safeName } from "./omp-brain.mjs";

// ---- .env 加载（进程 env 优先） ----
const envFile = path.join(import.meta.dirname, ".env");
if (fs.existsSync(envFile)) {
  for (const line of fs.readFileSync(envFile, "utf8").split(/\r?\n/)) {
    const m = line.match(/^\s*([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.*)\s*$/);
    if (m && process.env[m[1]] === undefined) {
      process.env[m[1]] = m[2].trim().replace(/^["']|["']$/g, "");
    }
  }
}

// ---- 配置：CLI 参数 > .env/环境变量 > 默认 ----
const CLI_OPTIONS = {
  "bot-id": { type: "string" }, "secret": { type: "string" }, "ws-url": { type: "string" },
  "port": { type: "string" }, "welcome": { type: "string" }, "echo": { type: "string" },
  "brain": { type: "string" }, "inbox": { type: "string" }, "workspace": { type: "string" },
  "profile-dir": { type: "string" }, "log-max": { type: "string" },
  "session-max-days": { type: "string" }, "omp-cmd": { type: "string" },
  "omp-args": { type: "string" }, "omp-approval": { type: "string" },
};
const arg = (v) => (v === undefined || v === null || String(v).trim() === "") ? undefined : String(v).trim();
const cfg = (cliVal, envVal, def = undefined) => arg(cliVal) ?? ((envVal && String(envVal).trim() !== "") ? String(envVal).trim() : def);
const argv = process.argv.includes("--help")
  ? {}
  : parseArgs({ options: CLI_OPTIONS, strict: false }).values;
if (process.argv.includes("--help")) {
  for (const [k, v] of Object.entries(CLI_OPTIONS)) console.log(`  --${k}`.padEnd(22) + `(.env: ${k.toUpperCase().replace(/-/g, "_")})`);
  process.exit(0);
}

const BOT_ID = cfg(argv["bot-id"], process.env.WECOM_BOT_ID, "");
const SECRET = cfg(argv["secret"], process.env.WECOM_SECRET || process.env.WECOM_BOT_SECRET, "");
const WS_URL = cfg(argv["ws-url"], process.env.WECOM_WEBSOCKET_URL, "wss://openws.work.weixin.qq.com");
const PORT = Number(cfg(argv["port"], process.env.BRIDGE_PORT, 8765));
const WELCOME = cfg(argv["welcome"], process.env.WECOM_WELCOME_TEXT, "");
const ECHO = cfg(argv["echo"], process.env.BRIDGE_ECHO, "0") === "1";
const BRAIN = cfg(argv["brain"], process.env.BRIDGE_BRAIN, "") === "omp"; // omp 大脑（启用时优先于回声）
const INBOX = path.resolve(cfg(argv["inbox"], process.env.INBOX_FILE, path.join(import.meta.dirname, "data", "inbox.jsonl")));
try { fs.mkdirSync(path.dirname(INBOX), { recursive: true }); } catch { /* ignore */ }
const PROFILE_DIR = path.resolve(cfg(argv["profile-dir"], process.env.OMP_PROFILE_DIR, path.join(os.homedir(), ".omp", "profiles", "wecombot", "agent")));
const STARTED_AT = Date.now();
const THINKING_LEVELS = ["auto", "off", "minimal", "low", "medium", "high", "xhigh", "max"];

// ---- Hermes 实测工程参数 ----
const CONNECT_TIMEOUT_MS = 20_000;   // 握手/订阅等待
const HEARTBEAT_MS = 30_000;         // 应用层 ping（官方文档未写但必需）
const RECONNECT_BACKOFF = [2_000, 5_000, 10_000, 30_000, 60_000];
const REPLY_ACK_TIMEOUT_MS = 15_000; // final ack 超时 → 视为已送达（对齐官方插件行为）
const DEDUP_MAX = 1000;
const STREAM_MAX_CHARS = 20_000;     // 服务端单帧 20480 字节上限，留余量
const TEXT_BATCH_MS = 600;           // 手机端长文本分片合并窗
const ATTACH_HOLD_MS = 800;          // 纯附件等待尾随文本窗
const BATCH_HARD_CAP_MS = 2000;      // 合并硬上限（分片场景 Hermes 用 2s）

// 流窗口已死的错误码（stream ~6 分钟即过期，连接 ping 不续期）
const STREAM_DEAD_ERRCODES = new Set([846608, 846604, 846609]);
const CALLBACK_CMDS = new Set(["aibot_msg_callback", "aibot_callback", "aibot_event_callback"]);

const uuid = () => crypto.randomUUID();
const DEVICE_ID = uuid().replace(/-/g, "");
const mask = (s) => (s ? `${s.slice(0, 4)}…${s.slice(-2)}` : "(unset)");

// ---- 日志：进程自持 fd，超限轮转 logs/bridge.log → logs/bridge.log.old（长稳运行防无限增长）----
const LOG_DIR = path.join(import.meta.dirname, "logs");
const BRIDGE_LOG = path.join(LOG_DIR, "bridge.log");
const LOG_ROTATE_BYTES = Number(cfg(argv["log-max"], process.env.BRIDGE_LOG_MAX, 5 * 1024 * 1024));
let logFd = null;
function openLog() {
  try { fs.mkdirSync(LOG_DIR, { recursive: true }); } catch { /* ignore */ }
  try { logFd = fs.openSync(BRIDGE_LOG, "a"); } catch { logFd = null; }
}
function rotateLog() {
  try { if (logFd !== null) fs.closeSync(logFd); } catch { /* ignore */ }
  logFd = null;
  try { fs.rmSync(BRIDGE_LOG + ".old", { force: true }); } catch { /* ignore */ }
  try { fs.renameSync(BRIDGE_LOG, BRIDGE_LOG + ".old"); } catch { /* 原文件可能不存在 */ }
  openLog();
}
openLog();
const fmtLog = (a) => a.map((v) => (typeof v === "string" ? v : JSON.stringify(v) ?? String(v))).join(" ");
const log = (...a) => {
  const line = `${new Date().toISOString()} ${fmtLog(a)}`;
  try {
    if (logFd !== null && fs.fstatSync(logFd).size > LOG_ROTATE_BYTES) rotateLog();
    if (logFd !== null) fs.writeSync(logFd, line + "\n");
  } catch { /* 日志失败不影响主流程 */ }
  console.log(line);
};

// ---- 全局异常兜底：任何回调抛错只记录，不杀进程（长稳关键）----
process.on("uncaughtException", (e) => log("UNCAUGHT", e?.stack || String(e)));
process.on("unhandledRejection", (e) => log("UNHANDLED_REJECTION", e?.stack || String(e)));

// ---- 连接状态 ----
let ws = null;
let subscribed = false;
let backoffIdx = 0;
let suppressed = false; // 被踢（disconnected_event）后抑制重连，避免与另一实例互踢
let reconnectTimer = null;
let heartbeatTimer = null;
let lastInbound = 0;
let connectAttempts = 0;
const pendingHandshake = { req_id: null, resolve: null };
const pendingReplies = new Map(); // req_id -> resolve(fn)（respond/send 的 ack 关联）
const dedup = new Map();          // msgid -> true，FIFO 有界
const batches = new Map();        // chatKey -> 待合并事件
const STOP_FLAG = path.join(import.meta.dirname, "bridge.stop");
const INBOX_ROTATE_BYTES = 20 * 1024 * 1024;
const SESSION_MAX_AGE_MS = Number(cfg(argv["session-max-days"], process.env.SESSION_MAX_DAYS, 14)) * 86_400_000;

/** inbox 追加；超 20MB 轮转到 inbox.jsonl.old（长稳防无限增长） */
function appendInbox(record) {
  try {
    if (fs.existsSync(INBOX) && fs.statSync(INBOX).size > INBOX_ROTATE_BYTES) {
      fs.rmSync(INBOX + ".old", { force: true });
      fs.renameSync(INBOX, INBOX + ".old");
      log("INBOX rotated (>20MB)");
    }
  } catch { /* 轮转失败则继续追加 */ }
  try {
    fs.appendFileSync(INBOX, JSON.stringify(record) + "\n");
  } catch (e) {
    log("INBOX write failed:", e.message);
  }
}

/** 会话清扫：目录内最新文件 mtime 超 SESSION_MAX_DAYS 的会话删除（omp 记忆清零） */
function sweepStaleSessions() {
  const root = path.join(import.meta.dirname, "workspace", "sessions");
  let dirs = [];
  try { dirs = fs.readdirSync(root, { withFileTypes: true }).filter((d) => d.isDirectory()); } catch { return; }
  const now = Date.now();
  for (const d of dirs) {
    const dir = path.join(root, d.name);
    let latest = 0;
    try {
      for (const f of fs.readdirSync(dir)) {
        const st = fs.statSync(path.join(dir, f));
        if (st.mtimeMs > latest) latest = st.mtimeMs;
      }
    } catch { continue; }
    if (latest && now - latest > SESSION_MAX_AGE_MS) {
      if (brain) for (const key of [...brain.chats.keys()]) if (safeName(key) === d.name) brain.killChatByKey(key);
      try {
        fs.rmSync(dir, { recursive: true, force: true });
        log("SESSIONS sweep removed", d.name, `idle=${Math.round((now - latest) / 86_400_000)}d`);
      } catch (e) {
        log("SESSIONS sweep failed", d.name, e.message);
      }
    }
  }
}

function sendJson(obj) {
  if (ws && ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify(obj));
    return true;
  }
  return false;
}

/** 发送请求帧并等待同 req_id 的 ack 帧 */
function request(cmd, body, { replyReqId, timeoutMs = REPLY_ACK_TIMEOUT_MS } = {}) {
  const req_id = replyReqId || `${cmd}-${uuid()}`;
  const p = new Promise((resolve) => {
    pendingReplies.set(req_id, resolve);
    setTimeout(() => {
      if (pendingReplies.has(req_id)) {
        pendingReplies.delete(req_id);
        resolve({ _timeout: true });
      }
    }, timeoutMs);
  });
  sendJson({ cmd, headers: { req_id }, body });
  return p;
}

// ---- 连接与订阅 ----
function connect() {
  if (suppressed) return;
  if (!BOT_ID || !SECRET) {
    log("CREDENTIALS missing: WECOM_BOT_ID / WECOM_SECRET 未配置，跳过连接。请在 .env 中填写后重启");
    return;
  }
  connectAttempts++;
  log(`CONNECT #${connectAttempts} ${WS_URL} device=${DEVICE_ID.slice(0, 8)}`);
  try {
    ws = new WebSocket(WS_URL);
  } catch (e) {
    scheduleReconnect(`constructor: ${e.message}`);
    return;
  }
  ws.on("open", () => {
    log("WS open, subscribing");
    const req_id = `subscribe-${uuid()}`;
    pendingHandshake.req_id = req_id;
    pendingHandshake.resolve = null;
    sendJson({
      cmd: "aibot_subscribe",
      headers: { req_id },
      body: { bot_id: BOT_ID, secret: SECRET, device_id: DEVICE_ID },
    });
    setTimeout(() => {
      if (pendingHandshake.req_id === req_id && !subscribed) {
        pendingHandshake.req_id = null;
        log("SUBSCRIBE timeout — reconnecting");
        try { ws?.close(); } catch { /* ignore */ }
      }
    }, CONNECT_TIMEOUT_MS);
  });
  ws.on("message", (data) => handleFrame(data.toString()));
  ws.on("error", (e) => log("WS error", e?.message || "(no detail)"));
  ws.on("close", (code) => {
    const wasSub = subscribed;
    subscribed = false;
    stopHeartbeat();
    if (suppressed) return; // 被踢后不再重连
    log(`WS close code=${code} wasSubscribed=${wasSub}`);
    scheduleReconnect("closed");
  });
}

function scheduleReconnect(reason) {
  const delay = RECONNECT_BACKOFF[Math.min(backoffIdx, RECONNECT_BACKOFF.length - 1)];
  backoffIdx++;
  log(`RECONNECT in ${delay}ms (reason=${reason}, attempt=${backoffIdx})`);
  clearTimeout(reconnectTimer);
  reconnectTimer = setTimeout(connect, delay);
}

function startHeartbeat() {
  stopHeartbeat();
  heartbeatTimer = setInterval(() => {
    if (ws && ws.readyState === WebSocket.OPEN) {
      ws.ping(); // 协议层 ping（对齐 Hermes aiohttp heartbeat）
      sendJson({ cmd: "ping", headers: { req_id: `ping-${uuid()}` }, body: {} }); // 应用层 ping
    }
  }, HEARTBEAT_MS);
}

function stopHeartbeat() {
  clearInterval(heartbeatTimer);
  heartbeatTimer = null;
}

// ---- 帧分发 ----
function handleFrame(raw) {
  let frame;
  try {
    frame = JSON.parse(raw);
  } catch {
    // 企微偶发 JSON 字符串内裸控制字符 → strict 宽松解析兜底
    try {
      frame = JSON.parse(raw.replace(/[\u0000-\u001f]+(?=(?:[^"]*"[^"]*")*[^"]*$)/g, " "));
    } catch {
      log("DROP unparseable frame len=", raw.length);
      return;
    }
  }
  if (typeof frame !== "object" || frame === null) return;
  const req_id = frame?.headers?.req_id || "";
  const cmd = String(frame.cmd || "");
  // 心跳 ack（无 cmd、req_id 以 ping- 开头）只证明链路存活，静默处理避免刷屏
  if (!cmd && typeof req_id === "string" && req_id.startsWith("ping-")) return;

  // 1) 订阅结果
  if (pendingHandshake.req_id && req_id === pendingHandshake.req_id) {
    pendingHandshake.req_id = null;
    const ok = frame.errcode === 0 || frame.errcode === undefined;
    if (ok) {
      subscribed = true;
      backoffIdx = 0;
      log("SUBSCRIBED ok — long connection ready");
      startHeartbeat();
    } else {
      log(`SUBSCRIBE failed errcode=${frame.errcode} errmsg=${frame.errmsg} — 检查凭证与后台「长连接」模式`);
      try { ws?.close(); } catch { /* ignore */ }
    }
    return;
  }
  // 2) respond/send 的 ack（回调类 cmd 除外）
  if (req_id && !CALLBACK_CMDS.has(cmd) && pendingReplies.has(req_id)) {
    const resolve = pendingReplies.get(req_id);
    pendingReplies.delete(req_id);
    resolve(frame);
    return;
  }
  // 3) 业务回调
  if (cmd === "aibot_msg_callback" || cmd === "aibot_callback") {
    onMessage(frame);
  } else if (cmd === "aibot_event_callback") {
    onEvent(frame);
  } else if (cmd !== "ping") {
    log("FRAME unrouted", JSON.stringify(frame).slice(0, 300));
  }
}

function dedupMsg(msgid) {
  if (!msgid) return false;
  if (dedup.has(msgid)) return true;
  dedup.set(msgid, true);
  while (dedup.size > DEDUP_MAX) dedup.delete(dedup.keys().next().value);
  return false;
}

// ---- 消息回调：去重 → @前缀剥离 → 分片合并 → inbox ----
function onMessage(frame) {
  const body = frame.body;
  if (typeof body !== "object" || body === null) return;
  const req_id = frame?.headers?.req_id || "";
  const msgid = String(body.msgid || req_id || uuid());
  if (dedupMsg(msgid)) {
    log("DROP duplicate msgid", msgid.slice(0, 12));
    return;
  }
  const userId = String(body?.from?.userid || "").trim();
  const chatType = String(body.chattype || "single").toLowerCase() === "group" ? "group" : "single";
  const chatId = String(body.chatid || userId).trim();
  if (!chatId) {
    log("DROP message without chat id");
    return;
  }
  let text = String(body?.text?.content || "").trim();
  if (chatType === "group" && text) text = text.replace(/^@\S+\s*/, ""); // "@Bot xxx" → "xxx"
  const media = [];
  for (const key of ["image", "file", "video", "voice"]) {
    const m = body?.[key];
    if (m && typeof m === "object" && m.url) media.push({ type: key, url: m.url, aeskey: m.aeskey || undefined });
  }
  enqueueBatch({ chatKey: chatId, chatId, chatType, userId, reqId: req_id, msgId: msgid, text, media });
}

/** 长文本分片合并：文本到达 600ms 后冲刷（再到达则顺延，硬上限 2s）；纯附件等 800ms 尾随文本 */
function enqueueBatch(evt) {
  const now = Date.now();
  let b = batches.get(evt.chatKey);
  if (!b) {
    b = { texts: [], media: [], reqId: evt.reqId, userId: evt.userId, chatId: evt.chatId, chatType: evt.chatType, firstAt: now, deadline: 0, timer: null };
    batches.set(evt.chatKey, b);
  }
  if (evt.text) b.texts.push(evt.text);
  if (evt.media.length) b.media.push(...evt.media);
  if (evt.reqId) b.reqId = evt.reqId;
  const delay = evt.text ? TEXT_BATCH_MS : ATTACH_HOLD_MS;
  const deadline = Math.min(now + delay, b.firstAt + BATCH_HARD_CAP_MS);
  clearTimeout(b.timer);
  b.timer = setTimeout(() => flushBatch(evt.chatKey), deadline - now);
}

function flushBatch(chatKey) {
  const b = batches.get(chatKey);
  if (!b) return;
  batches.delete(chatKey);
  const record = {
    ts: new Date().toISOString(),
    cmd: "aibot_msg_callback",
    req_id: b.reqId,
    chat_id: b.chatId,
    chat_type: b.chatType,
    user_id: b.userId,
    msg_id: undefined,
    text: b.texts.join(" ").trim(),
    media: b.media,
  };
  lastInbound = Date.now();
  appendInbox(record);
  log("MSG", JSON.stringify(record));
  if (brain && record.text) {
    brain.handle(record).catch((e) => log("BRAIN dispatch error:", e.message));
  } else if (ECHO && record.text) {
    const streamId = `echo-${uuid().slice(0, 8)}`;
    sendReply(record.req_id, { msgtype: "stream", content: `echo: ${record.text}`, stream_id: streamId, finish: true })
      .then((r) => log("ECHO reply", JSON.stringify(r)))
      .catch(() => {});
  }
}

// ---- 事件回调 ----
function onEvent(frame) {
  const body = frame.body;
  if (typeof body !== "object" || body === null) return;
  const eventType = String(body?.event?.eventtype || body?.event_type || "");
  const req_id = frame?.headers?.req_id || "";
  if (eventType === "disconnected_event") {
    // 被踢：另一条连接接管了本机器人。抑制重连，否则两条实例会互踢死循环。
    suppressed = true;
    subscribed = false;
    stopHeartbeat();
    log("WARN disconnected_event — 被新连接接管（另一实例在线？）。已抑制重连；人工排查后重启本进程");
    return;
  }
  if (eventType === "enter_chat" && WELCOME) {
    // 官方要求 5s 内回复欢迎语，立即发
    const ok = sendJson({
      cmd: "aibot_respond_welcome_msg",
      headers: { req_id },
      body: { msgtype: "text", text: { content: WELCOME } },
    });
    log("WELCOME sent=", ok);
  }
  const record = { ts: new Date().toISOString(), cmd: "aibot_event_callback", req_id, event: eventType, chat_id: body.chatid, user_id: body?.from?.userid };
  log("EVENT", JSON.stringify(record));
  appendInbox(record);
}

// ---- 回复与主动推送 ----
async function sendReply(reqId, { msgtype = "stream", content, stream_id, finish = true } = {}) {
  if (typeof content !== "string" || !content) return { ok: false, error: "content required" };
  if (!reqId) return { ok: false, error: "req_id required（透传 inbox 记录里的 req_id）" };
  if (!subscribed) {
    return { ok: false, error: suppressed ? "connection taken over（被其他实例踢掉）" : "not connected/subscribed" };
  }
  const wasSubscribed = subscribed;
  let body;
  if (msgtype === "stream") {
    let text = content;
    if (text.length > STREAM_MAX_CHARS) {
      text = text.slice(0, STREAM_MAX_CHARS);
      log(`WARN stream content truncated to ${STREAM_MAX_CHARS} chars`);
    }
    body = { msgtype: "stream", stream: { id: stream_id || `s-${uuid().slice(0, 12)}`, finish: !!finish, content: text } };
  } else {
    body = { msgtype, [msgtype]: { content } };
  }
  const ack = await request("aibot_respond_msg", body, { replyReqId: reqId });
  const errcode = ack?.errcode ?? ack?.body?.errcode;
  const errmsg = ack?.errmsg ?? ack?.body?.errmsg ?? "";
  if (ack?._timeout) {
    if (!wasSubscribed) return { ok: false, error: "ack timeout（未在订阅状态下收到确认）" };
    // 对齐官方插件：final 帧字节已发出、ack 迟到视为已送达，不重发（重发会产生重复消息）
    return { ok: true, note: "ack_timeout_assumed_delivered" };
  }
  if (errcode === 0 || errcode === undefined) return { ok: true, errcode: errcode ?? 0 };
  const dead = STREAM_DEAD_ERRCODES.has(errcode);
  return {
    ok: false,
    errcode,
    errmsg,
    hint: dead ? "流窗口已死（约 6 分钟）→ 用 /send 主动推送兜底，或让用户重新发消息" : undefined,
  };
}

async function sendPush({ chat_id, chat_type = 1, content } = {}) {
  if (!chat_id || typeof content !== "string") return { ok: false, error: "chat_id and content required" };
  if (!(ws && ws.readyState === WebSocket.OPEN)) return { ok: false, error: "not connected" };
  const ack = await request("aibot_send_msg", {
    chatid: chat_id,
    chat_type,
    msgtype: "markdown",
    markdown: { content },
  });
  const errcode = ack?.errcode ?? ack?.body?.errcode;
  if (ack?._timeout) return { ok: false, error: "ack timeout" };
  return errcode === 0 || errcode === undefined
    ? { ok: true }
    : { ok: false, errcode, errmsg: ack?.errmsg ?? ack?.body?.errmsg ?? "" };
}

// ---- omp 大脑（BRIDGE_BRAIN=omp 启用，优先级高于回声） ----
let brain = null;
sweepStaleSessions(); // brain 声明后首扫（函数内 if (brain) 判空，此处 brain 尚未赋值也安全）
setInterval(sweepStaleSessions, 6 * 3600_000);
if (BRAIN) {
  brain = new OmpBrain({
    workspace: cfg(argv["workspace"], undefined, path.join(import.meta.dirname, "workspace")),
    ompCmd: cfg(argv["omp-cmd"], process.env.OMP_CMD, "omp"),
    extraArgs: cfg(argv["omp-args"], process.env.OMP_ARGS, ""),
    approvalMode: cfg(argv["omp-approval"], process.env.OMP_APPROVAL_MODE, "yolo"),
    log,
    sendReply,
    sendPush,
  });
  log("BRAIN omp enabled (per-chat rpc child, --no-context-files)");
}

// ---- 控制面板辅助（读写 profile 配置 / 会话清单 / 收件箱尾部） ----
function readProfileConfig() {
  let text = "";
  try { text = fs.readFileSync(path.join(PROFILE_DIR, "config.yml"), "utf8"); } catch { /* 不存在则空 */ }
  const model = (text.match(/^model:\s*(.+)$/m) || [])[1]?.trim() || "";
  const thinking = (text.match(/^defaultThinkingLevel:\s*(.+)$/m) || [])[1]?.trim() || "auto";
  return { text, model, thinking };
}

function writeProfileConfig(patch) {
  let { text, model, thinking } = readProfileConfig();
  if (patch.model) {
    if (/^model:/m.test(text)) text = text.replace(/^model:.*$/m, `model: ${patch.model}`);
    else text = `model: ${patch.model}\n${text}`;
    model = patch.model;
  }
  if (patch.thinking) {
    if (/^defaultThinkingLevel:/m.test(text)) text = text.replace(/^defaultThinkingLevel:.*$/m, `defaultThinkingLevel: ${patch.thinking}`);
    else text += `\ndefaultThinkingLevel: ${patch.thinking}\n`;
    thinking = patch.thinking;
  }
  fs.mkdirSync(PROFILE_DIR, { recursive: true });
  fs.writeFileSync(path.join(PROFILE_DIR, "config.yml"), text);
  return { model, thinking };
}

function availableModels() {
  let text = "";
  try { text = fs.readFileSync(path.join(PROFILE_DIR, "models.yml"), "utf8"); } catch { return []; }
  const out = [];
  let provider = null;
  for (const line of text.split(/\r?\n/)) {
    const pm = line.match(/^  ([A-Za-z0-9_-]+):\s*$/);
    if (pm) { provider = pm[1]; continue; }
    const mm = line.match(/^\s*-\s*id:\s*(\S+)/);
    if (mm && provider) out.push(`${provider}/${mm[1]}`);
  }
  return out;
}

function listSessions() {
  const root = path.join(import.meta.dirname, "workspace", "sessions");
  let dirs = [];
  try {
    dirs = fs.readdirSync(root, { withFileTypes: true }).filter((d) => d.isDirectory());
  } catch { return []; }
  const busySet = new Set(brain ? [...brain.chats.keys()].map((k) => safeName(k)) : []);
  return dirs.map((d) => {
    const dir = path.join(root, d.name);
    let bytes = 0, preview = "";
    try {
      for (const f of fs.readdirSync(dir)) {
        bytes += fs.statSync(path.join(dir, f)).size;
        if (preview) continue;
        const fd = fs.openSync(path.join(dir, f), "r");
        try {
          const buf = Buffer.alloc(65536);
          const n = fs.readSync(fd, buf, 0, buf.length, 0);
          const m = buf.toString("utf8", 0, n).match(/"text":"((?:[^"\\]|\\.)*)"/);
          if (m) preview = m[1].replace(/\\n/g, " ").replace(/\\"/g, '"').slice(0, 80);
        } finally {
          fs.closeSync(fd);
        }
      }
    } catch { /* ignore */ }
    return {
      id: d.name,
      busy: busySet.has(d.name),
      bytes,
      mtime: fs.statSync(dir).mtime.getTime(),
      preview,
    };
  }).sort((a, b) => b.mtime - a.mtime);
}

function tailInbox(n = 100) {
  let raw = "";
  try {
    const st = fs.statSync(INBOX);
    const fd = fs.openSync(INBOX, "r");
    try {
      const start = Math.max(0, st.size - 256 * 1024);
      const buf = Buffer.alloc(st.size - start);
      fs.readSync(fd, buf, 0, buf.length, start);
      raw = buf.toString("utf8");
    } finally {
      fs.closeSync(fd);
    }
  } catch { return []; }
  return raw.split("\n").filter(Boolean).slice(-n).reverse()
    .map((l) => { try { return JSON.parse(l); } catch { return null; } })
    .filter(Boolean);
}

function readJsonBody(req, limit = 1_000_000) {
  return new Promise((resolve, reject) => {
    let raw = "";
    req.on("data", (c) => { raw += c; if (raw.length > limit) req.destroy(); });
    req.on("end", () => { try { resolve(JSON.parse(raw || "{}")); } catch { reject(new Error("bad json")); } });
    req.on("error", reject);
  });
}

// ---- 本地 HTTP API（仅 127.0.0.1） ----
const server = http.createServer((req, res) => {
  const json = (code, obj) => {
    res.writeHead(code, { "content-type": "application/json; charset=utf-8" });
    res.end(JSON.stringify(obj));
  };
  const route = (req.url || "/").split("?")[0];

  // ---- 控制面板页面 ----
  if (req.method === "GET" && (route === "/" || route === "/panel")) {
    try {
      const html = fs.readFileSync(path.join(import.meta.dirname, "panel.html"));
      res.writeHead(200, { "content-type": "text/html; charset=utf-8", "cache-control": "no-store" });
      res.end(html);
    } catch (e) {
      console.error(new Date().toISOString(), "PANEL read failed:", e && (e.code || ""), e && e.message);
      res.writeHead(404, { "content-type": "text/plain; charset=utf-8" });
      res.end("panel.html 缺失: " + (e && e.code ? e.code : "unknown"));
    }
    return;
  }
  if (req.method === "GET" && route === "/health") {
    return json(200, {
      ws_open: !!(ws && ws.readyState === WebSocket.OPEN),
      subscribed,
      suppressed,
      bot_id: mask(BOT_ID),
      ws_url: WS_URL,
      echo: ECHO,
      inbox: INBOX,
      last_inbound: lastInbound ? new Date(lastInbound).toISOString() : null,
    });
  }
  // ---- 控制面板 API ----
  if (req.method === "GET" && route === "/api/status") {
    const cfg = readProfileConfig();
    return json(200, {
      ws_open: !!(ws && ws.readyState === WebSocket.OPEN),
      subscribed,
      suppressed,
      brain: !!(BRAIN && brain),
      bot_id: mask(BOT_ID),
      model: cfg.model,
      thinking: cfg.thinking,
      models: availableModels(),
      thinking_levels: THINKING_LEVELS,
      started_at: new Date(STARTED_AT).toISOString(),
      uptime_ms: Date.now() - STARTED_AT,
      last_inbound: lastInbound ? new Date(lastInbound).toISOString() : null,
      chats: brain ? [...brain.chats.values()].map((c) => ({
        chat_id: c.key,
        pid: c.child.pid,
        busy: [...c.turns.values()].some((t) => !t.finished),
        last_used: new Date(c.lastUsed).toISOString(),
      })) : [],
      sessions: listSessions(),
    });
  }
  if (req.method === "GET" && route === "/api/messages") {
    return json(200, { messages: tailInbox(100) });
  }
  if (req.method === "POST" && route === "/api/model") {
    readJsonBody(req).then((body) => {
      const model = String(body.model || "").trim();
      if (!availableModels().includes(model)) return json(400, { ok: false, error: `未知模型: ${model || "(空)"}` });
      const cfg = writeProfileConfig({ model });
      const recycled = brain ? brain.recycleAll() : 0;
      log(`PANEL model → ${model} (recycled ${recycled} chat child)`);
      json(200, { ok: true, model: cfg.model, thinking: cfg.thinking, recycled });
    }).catch((e) => json(400, { ok: false, error: e.message }));
    return;
  }
  if (req.method === "POST" && route === "/api/thinking") {
    readJsonBody(req).then((body) => {
      const level = String(body.level || "").trim();
      if (!THINKING_LEVELS.includes(level)) return json(400, { ok: false, error: `未知思考强度: ${level || "(空)"}` });
      const cfg = writeProfileConfig({ thinking: level });
      const recycled = brain ? brain.recycleAll() : 0;
      log(`PANEL thinking → ${level} (recycled ${recycled} chat child)`);
      json(200, { ok: true, model: cfg.model, thinking: cfg.thinking, recycled });
    }).catch((e) => json(400, { ok: false, error: e.message }));
    return;
  }
  if (req.method === "POST" && route === "/api/apply") {
    const recycled = brain ? brain.recycleAll() : 0;
    log(`PANEL apply → recycled ${recycled} chat child`);
    return json(200, { ok: true, recycled });
  }
  if (req.method === "POST" && route === "/api/abort") {
    readJsonBody(req).then((body) => {
      const chatId = String(body.chat_id || "");
      const ok = brain ? brain.abortChatByKey(chatId) : false;
      log(`PANEL abort chat=${safeName(chatId)} ok=${ok}`);
      json(200, { ok });
    }).catch((e) => json(400, { ok: false, error: e.message }));
    return;
  }
  if (req.method === "POST" && route === "/api/sessions/delete") {
    readJsonBody(req).then((body) => {
      const ids = Array.isArray(body.ids) ? body.ids.filter((x) => /^[A-Za-z0-9_-]+$/.test(String(x))) : [];
      if (!ids.length) return json(400, { ok: false, error: "ids required" });
      const root = path.join(import.meta.dirname, "workspace", "sessions");
      const deleted = [], failed = [];
      for (const id of ids) {
        if (brain) for (const key of [...brain.chats.keys()]) if (safeName(key) === id) brain.killChatByKey(key);
        try {
          const dir = path.join(root, id);
          if (!fs.existsSync(dir)) {
            failed.push({ id, error: "session dir not found" });
            continue;
          }
          fs.rmSync(dir, { recursive: true, force: true });
          deleted.push(id);
        } catch (e) {
          failed.push({ id, error: e.message });
        }
      }
      log(`PANEL sessions delete: ${deleted.length} ok, ${failed.length} failed`);
      json(200, { ok: failed.length === 0, deleted, failed });
    }).catch((e) => json(400, { ok: false, error: e.message }));
    return;
  }
  if (req.method === "POST" && route === "/api/stop") {
    log("PANEL stop requested — shutting down");
    try { fs.writeFileSync(STOP_FLAG, new Date().toISOString()); } catch { /* ignore */ }
    json(200, { ok: true, note: "shutting down" });
    setTimeout(() => {
      brain?.dispose();
      try { ws?.close(); } catch { /* ignore */ }
      process.exit(0);
    }, 400);
    return;
  }
  if (req.method === "POST" && (route === "/reply" || route === "/send")) {
    let raw = "";
    req.on("data", (c) => {
      raw += c;
      if (raw.length > 1_000_000) req.destroy();
    });
    req.on("end", async () => {
      let payload;
      try {
        payload = JSON.parse(raw);
      } catch {
        return json(400, { ok: false, error: "bad json" });
      }
      try {
        const result = route === "/reply" ? await sendReply(payload.req_id, payload) : await sendPush(payload);
        log(route.toUpperCase(), JSON.stringify(result));
        json(result.ok ? 200 : 400, result);
      } catch (e) {
        json(500, { ok: false, error: e.message });
      }
    });
    return;
  }
  json(404, { ok: false, error: "unknown route" });
});

server.listen(PORT, "127.0.0.1", () => {
  log(`HTTP console listening on http://127.0.0.1:${PORT}  (面板 / ，API /health /api/*)`);
  log(`bot_id=${mask(BOT_ID)} secret=${mask(SECRET)} echo=${ECHO} inbox=${INBOX}`);
  connect();
});

// ---- 退出清理 ----
for (const sig of ["SIGINT", "SIGTERM"]) {
  process.on(sig, () => {
    log("shutting down…");
    suppressed = true;
    brain?.dispose();
    stopHeartbeat();
    clearTimeout(reconnectTimer);
    try { ws?.close(); } catch { /* ignore */ }
    server.close(() => process.exit(0));
    setTimeout(() => process.exit(0), 1500).unref();
  });
}
