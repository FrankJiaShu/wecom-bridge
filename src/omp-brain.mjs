// omp-brain.mjs — 把微信消息交给 omp 处理（普通 omp，无问数约束）
// 架构：每个聊天（chat_id）一个 `omp --mode rpc` 子进程 + 独立 --session-dir
//   - 独立会话目录 → 与用户平时 omp 会话完全隔离，追问保留跨消息记忆
//   - --no-context-files → 不注入用户全局 AGENTS.md（问数域守卫等约束不生效）
//   - open_session 语义：目录里有旧会话就续，没有就新开（等价 --session-dir + --continue）
//   - 流式：assistant text_delta → 节流推送 aibot_respond_msg stream（finish=false），完成时 finish=true
//   - 回收：空闲 15min 杀子进程（下次消息重新拉起并续上同一会话）；并发上限 LRU

import { spawn } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";

const CHAT_IDLE_MS = 15 * 60_000;      // 空闲聊天子进程回收
const TURN_TIMEOUT_MS = 10 * 60_000;   // 单回合看门狗
const SPAWN_TIMEOUT_MS = 30_000;       // 子进程启动等待
const STREAM_PUSH_MS = 1_200;          // 流式节流
const MAX_CHATS = 4;                   // 并发聊天子进程上限
const REAPER_MS = 60_000;
const CTX_TTL_MS = 10 * 60_000;        // 群名/姓名正缓存 TTL
const CTX_NEG_TTL_MS = 2 * 60_000;     // 查询失败负缓存 TTL
const CTX_BUDGET_MS = 6_000;           // 上下文解析总预算（超时先发 prompt，查询后台继续）
const CLI_TIMEOUT_MS = 12_000;         // wecom-cli 单次调用超时

const uuid = () => crypto.randomUUID();
export const safeName = (s) => {
  const ok = String(s || "").replace(/[^A-Za-z0-9_-]/g, "");
  return ok.length >= 6 ? ok : "c" + crypto.createHash("sha1").update(String(s || "")).digest("hex").slice(0, 12);
};

export class OmpBrain {
  /**
   * @param opts { workspace, ompCmd, extraArgs, approvalMode, log, sendReply(reqId,{content,stream_id,finish,msgtype}), sendPush({chat_id,chat_type,content}) }
   */
  constructor({ workspace, ompCmd = "omp", extraArgs = "", approvalMode = "yolo", log, sendReply, sendPush, maxChats = MAX_CHATS }) {
    this.workspace = workspace;
    this.ompCmd = ompCmd;
    this.extraArgs = String(extraArgs || "").trim().split(/\s+/).filter(Boolean);
    this.approvalMode = approvalMode;
    this.log = log;
    this.sendReply = sendReply;
    this.sendPush = sendPush;
    this.maxChats = maxChats;
    this.chats = new Map(); // chatKey -> chat state
    this.chatNames = new Map();        // chat_id -> {name, at}
    this.chatNameInflight = new Map(); // chat_id -> Promise
    this.userNames = new Map();        // user_id -> {name, at}
    this.userNameInflight = new Map();
    fs.mkdirSync(path.join(workspace, "sessions"), { recursive: true });
    this.reaper = setInterval(() => this.reap(), REAPER_MS);
  }

  /** 入口：一条合并后的微信消息 */
  async handle(record) {
    if (!record.text || !record.req_id) return;
    const key = record.chat_id || record.user_id;
    let chat = this.chats.get(key);
    if (!chat || chat.exited) {
      chat = this.spawnChat(key);
      this.chats.set(key, chat);
    }
    chat.lastUsed = Date.now();
    const text = record.text.trim();
    const busy = [...chat.turns.values()].some((t) => !t.finished);

    // 终止指令：忙时发"停止/stop/取消" → 终止当前回合并清空排队
    if (busy && /^(停止|终止|取消|stop|abort)$/i.test(text)) {
      this.abortChat(chat, record);
      return;
    }

    if (busy) {
      // 回合进行中：入队（omp 拒绝并发 prompt）；前 2 条回执确认，避免刷屏
      if (chat.queue.length >= 10) {
        await this.sendReply(record.req_id, {
          msgtype: "stream", content: "⚠️ 排队已满（10 条），本条被丢弃，请稍后再发。",
          stream_id: `q-${uuid().slice(0, 8)}`, finish: true,
        }).catch(() => {});
        return;
      }
      chat.queue.push(record);
      if (chat.queue.length <= 2) {
        await this.sendReply(record.req_id, {
          msgtype: "stream", content: "⏳ 上一条还在回答中，本条已排队，完成后自动处理。",
          stream_id: `q-${uuid().slice(0, 8)}`, finish: true,
        }).catch(() => {});
      }
      return;
    }
    try {
      await this.runTurn(chat, record);
    } catch (e) {
      this.log(`BRAIN turn error chat=${safeName(key)}: ${e.message}`);
      await this.sendReply(record.req_id, {
        msgtype: "stream", content: `⚠️ omp 处理失败：${e.message}`, stream_id: `err-${uuid().slice(0, 8)}`, finish: true,
      }).catch(() => {});
    }
    this.drainQueue(chat);
  }

  /** 回合结束后取下一条排队消息 */
  drainQueue(chat) {
    if (chat.exited || chat.turns.size > 0) return;
    const next = chat.queue.shift();
    if (!next) return;
    this.log(`BRAIN dequeue chat=${safeName(key(chat))} text=${(next.text || "").slice(0, 40)}`);
    this.runTurn(chat, next).catch((e) => this.log("BRAIN queued turn error:", e.message));
  }

  /** 微信终止指令：终止当前回合并清空排队 */
  abortChat(chat, record) {
    const turn = [...chat.turns.values()].find((t) => !t.finished);
    const dropped = chat.queue.length;
    chat.queue = [];
    if (turn) this.write(chat, { type: "abort" });
    this.sendReply(record.req_id, {
      msgtype: "stream",
      content: turn ? `✋ 已终止当前回答${dropped ? `，并清空 ${dropped} 条排队` : ""}。` : "当前没有正在进行的回答。",
      stream_id: `ab-${uuid().slice(0, 8)}`,
      finish: true,
    }).catch(() => {});
    this.log(`BRAIN abort chat=${safeName(key(chat))} by=${record.user_id || "?"}`);
  }

  spawnChat(key) {
    const sessionDir = path.join(this.workspace, "sessions", safeName(key));
    fs.mkdirSync(sessionDir, { recursive: true });
    const args = ["--mode", "rpc", "--no-ui", "--no-context-files", "--session-dir", sessionDir, ...this.extraArgs];
    const child = spawn(this.ompCmd, args, { cwd: this.workspace, stdio: ["pipe", "pipe", "pipe"] });
    const chat = {
      key, child, sessionDir, exited: false, exitCode: null, buf: "",
      ready: false, readyWaiters: [], pending: new Map(), turns: new Map(),
      queue: [], lastUsed: Date.now(), stderr: [],
    };
    child.stdout.on("data", (d) => this.onStdout(chat, d));
    child.stderr.on("data", (d) => {
      chat.stderr.push(String(d).trim());
      if (chat.stderr.length > 20) chat.stderr.shift();
    });
    child.on("exit", (code) => this.onChatExit(chat, code));
    this.log(`BRAIN spawn chat=${safeName(key)} pid=${child.pid} sessionDir=${sessionDir}`);
    return chat;
  }

  onChatExit(chat, code) {
    if (chat.exited) return;
    chat.exited = true;
    chat.exitCode = code;
    this.log(`BRAIN exit chat=${safeName(key(chat))} pid=${chat.child.pid} code=${code}`);
    for (const w of chat.readyWaiters.splice(0)) w(); // 唤醒等待者（runTurn 会因超时/检查退出而失败）
    for (const rec of chat.queue.splice(0)) {
      this.sendReply(rec.req_id, {
        msgtype: "stream", content: "⚠️ omp 进程退出，本条排队消息丢失，请重发。",
        stream_id: `q-${uuid().slice(0, 8)}`, finish: true,
      }).catch(() => {});
    }
    for (const turn of chat.turns.values()) {
      turn.finished = true;
      clearTimeout(turn.watchdog);
      clearTimeout(turn.pushTimer);
      if (chat.suppressExitMsg) continue;
      this.sendReply(turn.reqId, {
        msgtype: "stream", content: "⚠️ omp 进程退出，本次回答中断。请重发消息（会自动拉起新进程并续上本会话）。",
        stream_id: turn.streamId, finish: true,
      }).catch(() => {});
    }
    chat.turns.clear();
    chat.pending.clear();
    setTimeout(() => { if (this.chats.get(key(chat)) === chat && chat.exited) this.chats.delete(key(chat)); }, 1000);
  }

  onStdout(chat, data) {
    chat.buf += data.toString();
    let idx;
    while ((idx = chat.buf.indexOf("\n")) >= 0) {
      const line = chat.buf.slice(0, idx).trim();
      chat.buf = chat.buf.slice(idx + 1);
      if (!line) continue;
      let frame;
      try { frame = JSON.parse(line); } catch { continue; }
      try { this.onFrame(chat, frame); } catch (e) { this.log("BRAIN frame error:", e.message); }
    }
  }

  write(chat, obj) {
    if (chat.exited || chat.child.stdin.destroyed) return false;
    try {
      chat.child.stdin.write(JSON.stringify(obj) + "\n");
      return true;
    } catch { return false; }
  }

  command(chat, type, extra = {}, timeoutMs = 30_000) {
    const id = `c-${uuid()}`;
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        if (chat.pending.has(id)) { chat.pending.delete(id); reject(new Error(`${type} timeout`)); }
      }, timeoutMs);
      chat.pending.set(id, {
        resolve: (v) => { clearTimeout(timer); resolve(v); },
        reject: (e) => { clearTimeout(timer); reject(e); },
      });
      this.write(chat, { id, type, ...extra });
    });
  }

  onFrame(chat, frame) {
    if (frame.type === "response" && frame.id && chat.pending.has(frame.id)) {
      const p = chat.pending.get(frame.id);
      chat.pending.delete(frame.id);
      if (frame.success === false) p.reject(new Error(frame.error || "rpc error"));
      else p.resolve(frame.data ?? {});
      return;
    }
    if (frame.type === "ready") {
      chat.ready = true;
      // 缩小事件面：只要 assistant 文本增量（失败不影响功能，仅事件偏多）
      this.command(chat, "set_event_filter", { events: ["message_update"], messageUpdates: "delta" }, 10_000).catch(() => {});
      for (const w of chat.readyWaiters.splice(0)) w();
      return;
    }
    if (frame.type === "prompt_result") {
      const turn = chat.turns.get(frame.id);
      if (turn) this.finishTurn(chat, turn, frame).catch((e) => this.log("BRAIN finalize error:", e.message));
      return;
    }
    if (frame.type === "message_update") {
      const ev = frame.assistantMessageEvent || {};
      if (ev.type === "text_delta" && typeof ev.delta === "string" && ev.delta) {
        // 增量归属插入顺序最早的未完成 turn（即当前活跃回合）
        for (const turn of chat.turns.values()) {
          if (turn.finished) continue;
          turn.started = true;
          turn.text += ev.delta;
          this.schedulePush(chat, turn);
          break;
        }
      }
      return;
    }
    // agent_start / agent_end / subagent_* 等事件：忽略
  }

  // ---- 会话上下文解析：群名（sessions list）+ 发送者姓名（通讯录搜索）----
  /** wecom-cli 调用（.cmd 需走 shell）；输出截取首个 JSON 解析，失败返回 null */
  runCli(cliArgs, timeoutMs = CLI_TIMEOUT_MS) {
    const q = (a) => (/[\s"^&|<>()%!=;]/.test(a) ? `"${String(a).replace(/"/g, "")}"` : String(a));
    const line = ["wecom-cli", ...cliArgs].map(q).join(" ");
    return new Promise((resolve) => {
      const child = spawn(line, { shell: true, windowsHide: true, stdio: ["ignore", "pipe", "pipe"] });
      let out = "", done = false;
      const finish = (v) => { if (!done) { done = true; clearTimeout(timer); resolve(v); } };
      const timer = setTimeout(() => { try { child.kill(); } catch { /* ignore */ } finish(null); }, timeoutMs);
      child.stdout.on("data", (d) => (out += d));
      child.on("error", () => finish(null));
      child.on("exit", () => {
        const s = out.indexOf("{");
        if (s < 0) return finish(null);
        try { finish(JSON.parse(out.slice(s))); } catch { finish(null); }
      });
    });
  }

  /** chat_id → 群/会话名（wecom-cli message aibot sessions list，带 TTL 缓存与在途去重） */
  chatNameOf(chatId) {
    const hit = this.chatNames.get(chatId);
    if (hit && Date.now() - hit.at < (hit.name ? CTX_TTL_MS : CTX_NEG_TTL_MS)) return Promise.resolve(hit.name);
    const inflight = this.chatNameInflight.get(chatId);
    if (inflight) return inflight;
    const p = this.runCli(["message", "aibot", "sessions", "list"]).then((j) => {
      const s = Array.isArray(j?.sessions) ? j.sessions.find((x) => x?.chat_id === chatId) : null;
      const name = typeof s?.chat_name === "string" ? s.chat_name.trim() : "";
      this.chatNames.set(chatId, { name, at: Date.now() });
      this.chatNameInflight.delete(chatId);
      return name;
    }).catch(() => {
      this.chatNames.set(chatId, { name: "", at: Date.now() });
      this.chatNameInflight.delete(chatId);
      return "";
    });
    this.chatNameInflight.set(chatId, p);
    return p;
  }

  /** 在 JSON 树里找 userid 匹配项的姓名（兼容 userid/UserId/user_id 与 name/Name/display_name） */
  findUserName(node, userId) {
    if (!node || typeof node !== "object") return null;
    if (Array.isArray(node)) {
      for (const x of node) { const r = this.findUserName(x, userId); if (r !== null) return r; }
      return null;
    }
    const uid = node.userid ?? node.UserId ?? node.user_id;
    if (typeof uid === "string" && uid === userId) {
      const n = node.name ?? node.Name ?? node.display_name;
      return typeof n === "string" ? n.trim() : "";
    }
    for (const v of Object.values(node)) {
      const r = this.findUserName(v, userId);
      if (r !== null) return r;
    }
    return null;
  }

  /** user_id → 姓名（wecom-cli contact users search；未授权通讯录时返回 ""） */
  userNameOf(userId) {
    if (!/^[A-Za-z0-9_-]{3,64}$/.test(userId)) return Promise.resolve("");
    const hit = this.userNames.get(userId);
    if (hit && Date.now() - hit.at < (hit.name ? CTX_TTL_MS : CTX_NEG_TTL_MS)) return Promise.resolve(hit.name);
    const inflight = this.userNameInflight.get(userId);
    if (inflight) return inflight;
    const p = this.runCli(["contact", "users", "search", "--keywords", userId]).then((j) => {
      const name = j ? (this.findUserName(j, userId) ?? "") : "";
      this.userNames.set(userId, { name, at: Date.now() });
      this.userNameInflight.delete(userId);
      return name;
    }).catch(() => {
      this.userNames.set(userId, { name: "", at: Date.now() });
      this.userNameInflight.delete(userId);
      return "";
    });
    this.userNameInflight.set(userId, p);
    return p;
  }

  /**
   * 解析一条消息的会话上下文。总预算 CTX_BUDGET_MS：超时先发 prompt（查询留在后台继续并写缓存）。
   * 返回 {ctx, groupName, userName}，ctx 为注入文本（无有效信息时为空串）。
   */
  async resolveCtx(record) {
    const budget = new Promise((r) => setTimeout(() => r(null), CTX_BUDGET_MS));
    const [gName, uName] = await Promise.all([
      Promise.race([this.chatNameOf(record.chat_id).catch(() => ""), budget]).then((v) => v ?? ""),
      Promise.race([this.userNameOf(record.user_id).catch(() => ""), budget]).then((v) => v ?? ""),
    ]);
    const isGroup = record.chat_type === "group";
    const lines = [
      "[会话上下文（由 wecom-bridge 自动注入，非用户输入）]",
      `chat_type: ${isGroup ? "group" : "single"}`,
      `chat_id: ${record.chat_id || record.user_id || ""}`,
    ];
    if (isGroup) lines.push(`群名: ${gName || "（未设置群名或查询不可用）"}`);
    if (uName) lines.push(`发送者: ${uName}（userid=${record.user_id}）`);
    else lines.push(`发送者 userid: ${record.user_id}（姓名未授权查询或未命中）`);
    const ctx = lines.join("\n");
    this.log(`CTX chat=${safeName(record.chat_id || record.user_id)} group=${isGroup ? gName || "-" : "(single)"} sender=${uName || record.user_id}`);
    return { ctx, groupName: gName, userName: uName };
  }

  async runTurn(chat, record) {
    if (!chat.ready) {
      const deadline = Date.now() + SPAWN_TIMEOUT_MS;
      while (!chat.ready && !chat.exited && Date.now() < deadline) {
        await new Promise((res) => chat.readyWaiters.push(res));
      }
      if (!chat.ready) throw new Error(chat.exited ? `omp 启动失败 (code=${chat.exitCode}) ${(chat.stderr.at(-1) || "").slice(0, 120)}` : "omp 启动超时");
    }
    const id = `p-${uuid()}`;
    const turn = {
      id, reqId: record.req_id, chatId: record.chat_id, chatType: record.chat_type,
      text: "", streamId: `s-${uuid().slice(0, 12)}`,
      seeded: false, started: false, finished: false, lastPush: 0, pushTimer: null,
      watchdog: null,
    };
    turn.watchdog = setTimeout(() => this.timeoutTurn(chat, turn), TURN_TIMEOUT_MS);
    chat.turns.set(id, turn); // 先注册：上下文解析期间再来消息会正常走排队
    const { ctx } = await this.resolveCtx(record);
    const prompt = ctx ? `${ctx}\n———— 以下为用户消息 ————\n${record.text}` : record.text;
    this.write(chat, { id, type: "prompt", message: prompt });
    this.log(`BRAIN prompt chat=${safeName(key(chat))} id=${id.slice(0, 8)} text=${record.text.slice(0, 50)}`);
  }

  schedulePush(chat, turn) {
    if (turn.finished) return;
    if (!turn.seeded) {
      turn.seeded = true;
      this.pushTurn(chat, turn, false);
      return;
    }
    if (turn.pushTimer) return;
    const wait = Math.max(0, STREAM_PUSH_MS - (Date.now() - turn.lastPush));
    turn.pushTimer = setTimeout(() => {
      turn.pushTimer = null;
      if (!turn.finished) this.pushTurn(chat, turn, false);
    }, wait);
  }

  pushTurn(chat, turn, finish) {
    turn.lastPush = Date.now();
    const content = turn.text || (finish ? "（本轮没有文本输出）" : "…");
    this.sendReply(turn.reqId, { msgtype: "stream", content, stream_id: turn.streamId, finish })
      .then((r) => {
        if (finish && r && r.ok === false && r.hint) {
          // 流窗已死（约 6 分钟）→ 尝试主动推送兜底（群聊可能不支持，会记入日志）
          this.log(`BRAIN stream dead (chat=${safeName(key(chat))}) → fallback send`);
          this.sendPush({ chat_id: turn.chatId, chat_type: turn.chatType === "group" ? 2 : 1, content }).catch(() => {});
        }
      })
      .catch(() => {});
  }

  async finishTurn(chat, turn, frame) {
    turn.finished = true;
    clearTimeout(turn.watchdog);
    clearTimeout(turn.pushTimer);
    chat.turns.delete(turn.id);
    const status = frame.status;
    if (status === "error") {
      const msg = `⚠️ omp 回合出错：${frame.error?.message || "unknown"}${frame.error?.code ? ` (${frame.error.code})` : ""}`;
      this.log(`BRAIN turn error ${turn.id.slice(0, 8)}: ${msg}`);
      await this.sendReply(turn.reqId, { msgtype: "stream", content: msg, stream_id: turn.streamId, finish: true }).catch(() => {});
      return;
    }
    if (!turn.text.trim()) {
      try {
        const d = await this.command(chat, "get_last_assistant_text", {}, 15_000);
        if (d && typeof d.text === "string" && d.text.trim()) turn.text = d.text;
      } catch { /* 忽略，走空输出提示 */ }
    }
    this.pushTurn(chat, turn, true);
    this.log(`BRAIN done chat=${safeName(key(chat))} ${turn.id.slice(0, 8)} status=${status} len=${turn.text.length}`);
    this.drainQueue(chat);
  }

  timeoutTurn(chat, turn) {
    if (turn.finished) return;
    this.log(`BRAIN turn timeout ${turn.id.slice(0, 8)} → abort`);
    this.write(chat, { type: "abort" });
    turn.finished = true;
    clearTimeout(turn.pushTimer);
    chat.turns.delete(turn.id);
    this.sendReply(turn.reqId, {
      msgtype: "stream", content: (turn.text || "") + "\n\n（超过 10 分钟未完成，已中止）",
      stream_id: turn.streamId, finish: true,
    }).catch(() => {});
    this.drainQueue(chat);
  }

  /** 按 key 杀掉指定聊天的子进程（删除会话前调用） */
  killChatByKey(key) {
    const chat = this.chats.get(key);
    if (!chat) return false;
    chat.suppressExitMsg = true;
    try { chat.child.kill(); } catch { /* ignore */ }
    this.chats.delete(key);
    return true;
  }

  /** 面板终止：按 chat_id 终止当前回合并清空排队 */
  abortChatByKey(key) {
    const chat = this.chats.get(key);
    if (!chat) return false;
    let n = 0;
    for (const t of chat.turns.values()) {
      if (!t.finished) { this.write(chat, { type: "abort" }); n++; }
    }
    chat.queue = [];
    return n > 0;
  }

  /** 全部回收（配置热更后调用；正在跑的回合静默终止，下条消息用新配置拉起） */
  recycleAll() {
    let n = 0;
    for (const chat of this.chats.values()) {
      chat.suppressExitMsg = true;
      try { chat.child.kill(); } catch { /* ignore */ }
      n++;
    }
    this.chats.clear();
    return n;
  }

  reap() {
    const now = Date.now();
    const idle = [];
    for (const [key, chat] of this.chats) {
      if (chat.exited) { this.chats.delete(key); continue; } // 已退出的聊天状态及时清理
      const busy = [...chat.turns.values()].some((t) => !t.finished);
      if (!busy && now - chat.lastUsed > CHAT_IDLE_MS) idle.push([key, chat, now - chat.lastUsed]);
    }
    idle.sort((a, b) => b[2] - a[2]);
    // 超上限先杀最久空闲；空闲到期的一律杀
    let force = Math.max(0, this.chats.size - this.maxChats);
    for (const [key, chat, idleMs] of idle) {
      if (idleMs > CHAT_IDLE_MS || force > 0) {
        force--;
        this.log(`BRAIN reap chat=${safeName(key)} idle=${Math.round(idleMs / 1000)}s`);
        try { chat.child.kill(); } catch { /* ignore */ }
        this.chats.delete(key);
      }
    }
  }

  dispose() {
    clearInterval(this.reaper);
    for (const chat of this.chats.values()) {
      try { chat.child.kill(); } catch { /* ignore */ }
    }
    this.chats.clear();
  }
}

function key(chat) { return chat.key; }
