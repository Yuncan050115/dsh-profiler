/* ============================================================================
 * dsh-profiler — DeepSeek Harness 运行时剖析器
 * ----------------------------------------------------------------------------
 * 对标 Minecraft 服务端的 Spark（剖析）+ CoreProtect（审计与回档）。
 *
 * v2.0 能力：
 *   1) 采集：session/event 中 tool/call → tool/result 的耗时（环形缓冲 + 时间序列）
 *   2) 归因：进程启动 → 插件挂载耗时、RSS/Heap、会话 token 分布
 *   3) 回档：写文件类工具调用前自动快照原文，支持按时间点一键恢复（append-only 历史库）
 *   4) 诊断：阈值规则 findings（启动慢、内存高、跨会话记忆缺口…）
 *   5) 暴露：/profiler 命令、profiler_report 工具、/dsh-profiler 看板（SVG 图表）与 JSON
 *
 * 设计约束：宿主半侧只依赖 Node 内置模块，不 import @deepseek-ai/*。
 * ========================================================================== */

import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'
import crypto from 'node:crypto'
import { fileURLToPath } from 'node:url'

const PACKAGE_ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..')
const DSH_HOME = process.env.DSH_HOME || path.join(os.homedir(), '.dsh')
const NS = 'dsh-profiler'
const ROUTE_BASE = '/dsh-profiler'

const DEFAULTS = {
  enabled: true,
  sampleLimit: 800,
  slowToolMs: 3000,
  sessionScanTtlMs: 60000,
  http: true,
  rewind: true,
  rewindMaxEntries: 2000,
  rewindMaxFileKb: 512
}

/** 2026-10-23 在 Windows 实机测得（DeepSeek Harness 0.2.1-rc.2，6 进程）。 */
const BASELINE = { coldStartMs: 11738, warmStartMs: 4391, rssMb: 2021, processes: 6 }

const PLUGIN_VERSION = (() => {
  try {
    return String(JSON.parse(fs.readFileSync(path.join(PACKAGE_ROOT, 'package.json'), 'utf8')).version || '0.0.0')
  } catch { return '0.0.0' }
})()

const name = NS
const inject = ['tools', 'commands']

function normalizeConfig(input) {
  const out = { ...DEFAULTS }
  const src = input && typeof input === 'object' ? input : {}
  if (typeof src.enabled === 'boolean') out.enabled = src.enabled
  if (typeof src.http === 'boolean') out.http = src.http
  if (typeof src.rewind === 'boolean') out.rewind = src.rewind
  for (const key of ['sampleLimit', 'slowToolMs', 'sessionScanTtlMs', 'rewindMaxEntries', 'rewindMaxFileKb']) {
    const n = Number(src[key])
    if (Number.isFinite(n) && n > 0) out[key] = Math.min(600000, Math.round(n))
  }
  return out
}

const mb = (bytes) => Math.round((Number(bytes) || 0) / 1048576)

function percentile(sorted, p) {
  if (!sorted.length) return 0
  const idx = Math.min(sorted.length - 1, Math.ceil((p / 100) * sorted.length) - 1)
  return Math.round(sorted[Math.max(0, idx)])
}

/* -------------------------------------------------------------------------- */
/* 采集器（含时间序列）                                                          */
/* -------------------------------------------------------------------------- */

function createCollector(config) {
  const pending = new Map()
  const samples = []
  let total = 0
  let failed = 0

  function push(sample) {
    total += 1
    if (!sample.ok) failed += 1
    samples.push(sample)
    if (samples.length > config.sampleLimit) samples.splice(0, samples.length - config.sampleLimit)
  }

  function readCall(event) {
    const data = event && event.data ? event.data : {}
    const message = data.message && typeof data.message === 'object' ? data.message : undefined
    let callId = data.callId || data.id
    if (!callId && message) callId = message.toolCallId || message.id || message.source?.callId
    let toolName = data.name || data.toolName
    if (!toolName && message) {
      toolName = message.name
      if (!toolName && Array.isArray(message.content)) {
        const block = message.content.find((b) => b && b.type === 'tool-call')
        if (block) toolName = block.name
      }
      if (!toolName && message.source) toolName = message.source.toolName
    }
    return { callId: callId ? String(callId) : '', toolName: toolName ? String(toolName) : 'unknown' }
  }

  return {
    /** 返回 {callId, toolName} 供 rewind 在 call 阶段复用。 */
    readCall,
    observe(session, event) {
      if (!event || typeof event.type !== 'string') return
      if (event.type === 'tool/call') {
        const { callId, toolName } = readCall(event)
        if (!callId) return
        pending.set(callId, { toolName, at: Number(event.time) || Date.now(), sessionId: session?.id })
        return
      }
      if (event.type === 'tool/result') {
        const { callId } = readCall(event)
        const started = callId ? pending.get(callId) : undefined
        if (!started) return
        pending.delete(callId)
        const endedAt = Number(event.time) || Date.now()
        const ms = Math.max(0, endedAt - started.at)
        const isError = Boolean(event.data?.message?.isError)
        push({ tool: started.toolName, ms, ok: !isError, sessionId: started.sessionId, at: endedAt })
      }
    },
    dispose() { pending.clear() },
    snapshot() {
      const groups = new Map()
      for (const s of samples) {
        const g = groups.get(s.tool) || { tool: s.tool, count: 0, totalMs: 0, maxMs: 0, failed: 0 }
        g.count += 1
        g.totalMs += s.ms
        g.maxMs = Math.max(g.maxMs, s.ms)
        if (!s.ok) g.failed += 1
        groups.set(s.tool, g)
      }
      const perTool = [...groups.values()]
        .map((g) => ({ tool: g.tool, count: g.count, avgMs: Math.round(g.totalMs / g.count), maxMs: g.maxMs, failed: g.failed, totalMs: g.totalMs }))
        .sort((a, b) => b.totalMs - a.totalMs)
      const durations = samples.map((s) => s.ms).sort((a, b) => a - b)
      return {
        sampled: samples.length,
        total,
        failed,
        successRate: total ? Math.round(((total - failed) / total) * 1000) / 10 : 100,
        avgMs: durations.length ? Math.round(durations.reduce((a, b) => a + b, 0) / durations.length) : 0,
        p50Ms: percentile(durations, 50),
        p95Ms: percentile(durations, 95),
        p99Ms: percentile(durations, 99),
        maxMs: durations.length ? durations[durations.length - 1] : 0,
        perTool,
        slowest: [...samples].sort((a, b) => b.ms - a.ms).slice(0, 5),
        series: samples.slice(-120).map((s) => ({ t: s.at, ms: s.ms, tool: s.tool, ok: s.ok }))
      }
    }
  }
}

/* -------------------------------------------------------------------------- */
/* CoreProtect 式回档：写前快照 + append-only 历史库 + 一键恢复                   */
/* -------------------------------------------------------------------------- */

const WRITE_TOOL_RE = /write|edit|patch|create|replace|delete|rename|move|append|insert|str_replace|save/i
const PATH_KEYS = ['path', 'file', 'filePath', 'file_path', 'absolute_path', 'target_file', 'targetFile', 'filename', 'target']

function extractFilePath(event) {
  const data = event && event.data ? event.data : {}
  const message = data.message && typeof data.message === 'object' ? data.message : {}
  const candidates = [data.args, data.arguments, data.input, message.args, message.arguments, message.input]
  const block = Array.isArray(message.content) ? message.content.find((b) => b && b.type === 'tool-call') : null
  if (block) candidates.push(block.args, block.input, block.arguments)
  for (const src of candidates) {
    let obj = src
    if (typeof obj === 'string') { try { obj = JSON.parse(obj) } catch { obj = null } }
    if (!obj || typeof obj !== 'object') continue
    for (const key of PATH_KEYS) {
      const v = obj[key]
      if (typeof v === 'string' && v.length > 1 && /[:\\/.]/.test(v)) return v
    }
  }
  return ''
}

function createRewind(config) {
  const dir = path.join(DSH_HOME, 'dsh-profiler', 'rewind')
  const blobsDir = path.join(dir, 'blobs')
  const logFile = path.join(dir, 'history.jsonl')
  const staged = new Map() // callId -> 待确认的写前快照
  let seq = 0
  let bytes = 0

  try { fs.mkdirSync(blobsDir, { recursive: true }) } catch { /* noop */ }
  try { bytes = fs.existsSync(logFile) ? fs.statSync(logFile).size : 0 } catch { bytes = 0 }

  function storeBlob(content) {
    const hash = crypto.createHash('sha1').update(content).digest('hex')
    const file = path.join(blobsDir, hash)
    try {
      if (!fs.existsSync(file)) fs.writeFileSync(file, content)
      return { hash, ok: true }
    } catch { return { hash, ok: false } }
  }

  function append(entry) {
    try {
      const line = JSON.stringify(entry) + '\n'
      fs.appendFileSync(logFile, line, 'utf8')
      bytes += Buffer.byteLength(line)
      seq += 1
      return true
    } catch { return false }
  }

  /** tool/call 阶段：写入发生前抓原文。 */
  function stage(callId, toolName, event, sessionId) {
    if (!config.rewind || !callId) return
    if (!WRITE_TOOL_RE.test(toolName)) return
    const file = extractFilePath(event)
    if (!file) return
    let prev = null
    let prevExists = false
    let truncated = false
    try {
      const stat = fs.statSync(file)
      if (stat.isFile()) {
        prevExists = true
        if (stat.size <= config.rewindMaxFileKb * 1024) {
          prev = fs.readFileSync(file)
        } else truncated = true
      }
    } catch { prevExists = false }
    staged.set(callId, { toolName, file, prev, prevExists, truncated, sessionId, at: Number(event?.time) || Date.now() })
  }

  /** tool/result 阶段：确认成功后落盘历史。 */
  function commit(callId, ok) {
    const s = staged.get(callId)
    if (!s) return
    staged.delete(callId)
    if (!ok || s.truncated) return
    let hash = ''
    if (s.prevExists && s.prev) {
      const stored = storeBlob(s.prev)
      if (!stored.ok) return
      hash = stored.hash
    }
    append({
      id: `${s.at}-${seq}`,
      ts: s.at,
      sessionId: s.sessionId || '',
      tool: s.toolName,
      file: s.file,
      prevExists: s.prevExists,
      hash,
      sizeBytes: s.prev ? s.prev.length : 0
    })
    trim()
  }

  function trim() {
    try {
      const lines = fs.readFileSync(logFile, 'utf8').split('\n').filter(Boolean)
      if (lines.length <= config.rewindMaxEntries) return
      fs.writeFileSync(logFile, lines.slice(-config.rewindMaxEntries).join('\n') + '\n', 'utf8')
    } catch { /* noop */ }
  }

  function history(limit = 50) {
    try {
      const lines = fs.readFileSync(logFile, 'utf8').split('\n').filter(Boolean)
      return lines.slice(-Math.min(500, limit)).map((l) => { try { return JSON.parse(l) } catch { return null } }).filter(Boolean).reverse()
    } catch { return [] }
  }

  /** 回档：把 id 对应的写前快照写回文件。回档前对当前内容再存一档（可反向）。 */
  function restore(id) {
    const entry = history(500).find((e) => e.id === id)
    if (!entry) return { ok: false, error: 'entry not found' }
    const file = entry.file
    // 安全边界：只允许回档历史库中出现过的路径
    if (!history(500).some((e) => e.file === file)) return { ok: false, error: 'path not in history' }
    // 当前内容存档，使回档本身可逆
    try {
      const stat = fs.statSync(file)
      if (stat.isFile() && stat.size <= config.rewindMaxFileKb * 1024) {
        const cur = fs.readFileSync(file)
        const stored = storeBlob(cur)
        if (stored.ok) {
          append({ id: `${Date.now()}-${seq}`, ts: Date.now(), sessionId: entry.sessionId, tool: 'rewind/restore-point', file, prevExists: true, hash: stored.hash, sizeBytes: cur.length })
        }
      }
    } catch { /* 当前文件可能不存在 */ }
    try {
      if (!entry.prevExists) {
        fs.unlinkSync(file) // 当时是新文件，回档 = 删除
        return { ok: true, file, action: 'deleted (file did not exist at snapshot time)' }
      }
      const blob = path.join(blobsDir, entry.hash)
      const content = fs.readFileSync(blob)
      fs.mkdirSync(path.dirname(file), { recursive: true })
      fs.writeFileSync(file, content)
      return { ok: true, file, action: `restored ${content.length} bytes`, hash: entry.hash }
    } catch (err) {
      return { ok: false, error: String(err && err.message || err) }
    }
  }

  return {
    stage,
    commit,
    history,
    restore,
    summary() {
      const h = history(15)
      return { count: seq + h.length, logBytes: bytes, recent: h }
    }
  }
}

/* -------------------------------------------------------------------------- */
/* 会话 / 插件清单扫描                                                          */
/* -------------------------------------------------------------------------- */

function createScanners(config) {
  let sessionCache = { at: 0, data: null }

  function scanSessions() {
    const now = Date.now()
    if (sessionCache.data && now - sessionCache.at < config.sessionScanTtlMs) return sessionCache.data
    const dir = path.join(DSH_HOME, 'storages', 'session_projcache', 'sessions')
    const tokenKeys = new Map()
    const sessions = []
    let bytes = 0
    let files = []
    try { files = fs.readdirSync(dir).filter((f) => f.endsWith('.json')) } catch { files = [] }
    for (const file of files) {
      const full = path.join(dir, file)
      try {
        const raw = fs.readFileSync(full, 'utf8')
        bytes += Buffer.byteLength(raw)
        const doc = JSON.parse(raw)
        const rows = doc?.record?.rows || {}
        const totals = rows.tokenUsage?.val?.totals || {}
        let sessionTokens = 0
        for (const [key, value] of Object.entries(totals)) {
          if (typeof value !== 'number') continue
          sessionTokens += value
          tokenKeys.set(key, (tokenKeys.get(key) || 0) + value)
        }
        sessions.push({
          id: String(doc?.record?.identity?.id || file.replace(/\.json$/, '')),
          title: String(rows.title?.val || rows.titleInput?.val?.first?.text || '').slice(0, 80),
          createdAt: Number(doc?.record?.identity?.createdAt) || 0,
          tokens: sessionTokens,
          fileBytes: Buffer.byteLength(raw)
        })
      } catch { /* 单个损坏文件不影响整体 */ }
    }
    sessions.sort((a, b) => b.tokens - a.tokens)
    const read = tokenKeys.get('cacheReadTokens') || 0
    const uncached = tokenKeys.get('uncachedInputTokens') || 0
    const data = {
      count: sessions.length,
      storeBytes: bytes,
      tokens: Object.fromEntries([...tokenKeys.entries()].sort((a, b) => b[1] - a[1])),
      cacheHitRate: read + uncached > 0 ? Math.round((read / (read + uncached)) * 1000) / 10 : 0,
      top: sessions.slice(0, 5),
      largest: sessions.slice().sort((a, b) => b.fileBytes - a.fileBytes).slice(0, 3)
    }
    sessionCache = { at: now, data }
    return data
  }

  function scanPlugins() {
    const root = path.join(DSH_HOME, 'profiles')
    const profiles = []
    let dirs = []
    try { dirs = fs.readdirSync(root) } catch { dirs = [] }
    for (const profile of dirs) {
      const manifest = path.join(root, profile, 'package.json')
      let pkg
      try { pkg = JSON.parse(fs.readFileSync(manifest, 'utf8')) } catch { continue }
      const bundles = Array.isArray(pkg?.dsh?.profile?.bundles) ? pkg.dsh.profile.bundles : []
      const deps = pkg?.dependencies || {}
      const installed = bundles.map((bundle) => {
        const dirName = path.join(root, profile, 'node_modules', bundle)
        let version = deps[bundle] || ''
        let description = ''
        try {
          const sub = JSON.parse(fs.readFileSync(path.join(dirName, 'package.json'), 'utf8'))
          version = sub.version || version
          description = String(sub.description || '').slice(0, 120)
        } catch { /* 未安装或官方包内联 */ }
        return { bundle, version, installed: fs.existsSync(path.join(dirName, 'package.json')), description }
      })
      profiles.push({ profile, bundles: installed, count: installed.length })
    }
    const all = profiles.flatMap((p) => p.bundles.map((b) => b.bundle))
    return {
      profiles,
      totalBundles: all.length,
      thirdParty: all.filter((n) => !String(n).startsWith('@deepseek-ai/')).length,
      hasMemoryPlugin: all.some((n) => /memory|recall|mnemonic|longterm/i.test(String(n)))
    }
  }

  return { scanSessions, scanPlugins }
}

/* -------------------------------------------------------------------------- */
/* 诊断                                                                        */
/* -------------------------------------------------------------------------- */

function buildFindings(report, config) {
  const findings = []
  const { process: proc, tools, sessions, plugins, boot, rewind } = report
  if (boot.appliedAfterMs > 6000) {
    findings.push({ level: 'warn', code: 'boot/slow-apply', message: `插件树挂载耗时 ${boot.appliedAfterMs} ms，已超过 6s；对照实测冷启动 ${BASELINE.coldStartMs} ms，建议裁剪重型 bundle 或延迟加载。` })
  }
  if (proc.rssMb > 1500) {
    findings.push({ level: 'warn', code: 'process/high-memory', message: `主进程 RSS ${proc.rssMb} MB（实测基线 ${BASELINE.rssMb} MB），建议检查桌宠/壁纸类插件与图片缓存。` })
  }
  if (tools.sampled === 0) {
    findings.push({ level: 'info', code: 'tools/no-sample', message: '本次会话尚未采样到工具调用；产生一次工具调用后再运行 /profiler tools。' })
  } else if (tools.p95Ms > config.slowToolMs) {
    findings.push({ level: 'warn', code: 'tools/slow-p95', message: `工具调用 P95 为 ${tools.p95Ms} ms，超过阈值 ${config.slowToolMs} ms；最慢的是 ${tools.perTool[0]?.tool || 'unknown'}。` })
  }
  if (tools.successRate < 95) {
    findings.push({ level: 'warn', code: 'tools/failures', message: `工具失败率 ${Math.round((100 - tools.successRate) * 10) / 10}%（${tools.failed}/${tools.total}），建议查看重复失败的工具。` })
  }
  const totalTokens = Object.values(sessions.tokens).reduce((a, b) => a + b, 0)
  if (sessions.count >= 20 && !plugins.hasMemoryPlugin) {
    findings.push({ level: 'warn', code: 'memory/cross-session-gap', message: `本机已有 ${sessions.count} 个会话、累计 ${totalTokens} tokens，但未安装任何 memory/recall 插件：会话之间上下文完全隔离，跨对话记忆缺失。` })
  }
  if (sessions.cacheHitRate < 50 && totalTokens > 0) {
    findings.push({ level: 'info', code: 'session/low-cache-hit', message: `会话缓存命中率仅 ${sessions.cacheHitRate}%（cacheRead / 输入），长上下文重复计费风险较高。` })
  }
  if (sessions.top[0] && sessions.top[0].tokens > 120000) {
    findings.push({ level: 'info', code: 'session/large-context', message: `最大会话消耗 ${sessions.top[0].tokens} tokens（${sessions.top[0].title || '未命名'}），接近上下文压力区，可考虑 /compact。` })
  }
  if (rewind && rewind.recent.length === 0) {
    findings.push({ level: 'info', code: 'rewind/empty', message: '回档历史为空：发生一次写文件类工具调用后，这里会出现可回滚的时间点。' })
  }
  if (plugins.thirdParty === 0) {
    findings.push({ level: 'info', code: 'plugins/no-third-party', message: '当前 profile 未检测到第三方插件。' })
  }
  if (!findings.length) findings.push({ level: 'ok', code: 'all/good', message: '未发现异常：启动、内存、工具延迟与会话分布均在阈值内。' })
  return findings
}

function summarize(report) {
  const t = report.tools
  const s = report.sessions
  const lines = [
    `dsh-profiler v${PLUGIN_VERSION} · ${new Date(report.generatedAt).toLocaleString()}`,
    `进程  uptime ${report.process.uptimeSec}s · RSS ${report.process.rssMb} MB · Heap ${report.process.heapUsedMb}/${report.process.heapTotalMb} MB`,
    `启动  进程→插件挂载 ${report.boot.appliedAfterMs} ms（实测冷启动 ${BASELINE.coldStartMs} ms / 热启动 ${BASELINE.warmStartMs} ms）`,
    `工具  采样 ${t.sampled} 次 · avg ${t.avgMs} ms · p95 ${t.p95Ms} ms · 成功率 ${t.successRate}%`,
    `会话  ${s.count} 个 · 累计 ${Object.values(s.tokens).reduce((a, b) => a + b, 0)} tokens · 缓存命中 ${s.cacheHitRate}% · 存储 ${Math.round(s.storeBytes / 1024)} KB`,
    `插件  ${report.plugins.totalBundles} 个 bundle（第三方 ${report.plugins.thirdParty}）· 跨会话记忆插件 ${report.plugins.hasMemoryPlugin ? '已安装' : '缺失'}`,
    `回档  ${report.rewind.recent.length} 个最近变更点（/profiler rewind 查看，/profiler restore <id> 恢复）`,
    '',
    '诊断：',
    ...report.findings.map((f) => `  [${f.level}] ${f.message}`)
  ]
  return lines.join('\n')
}

/* -------------------------------------------------------------------------- */
/* 看板 HTML（纯手写 SVG 图表，零依赖）                                          */
/* -------------------------------------------------------------------------- */

const DASHBOARD = `<!doctype html><html lang="zh-CN"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>dsh-profiler · Runtime Profiler</title>
<style>
:root{--bg:#08090c;--panel:#101319;--line:#1e2530;--fg:#e8edf3;--dim:#7d8a99;--accent:#ff7500;--ok:#3ecf8e;--warn:#ffb020;--bad:#ff5d5d;--cyan:#22d3ee;--violet:#a78bfa}
*{box-sizing:border-box}
body{margin:0;background:var(--bg);color:var(--fg);font:14px/1.55 "Segoe UI",system-ui,sans-serif;
background-image:radial-gradient(ellipse 80% 50% at 50% -20%,rgba(255,117,0,.08),transparent),linear-gradient(rgba(30,37,48,.35) 1px,transparent 1px),linear-gradient(90deg,rgba(30,37,48,.35) 1px,transparent 1px);background-size:100% 100%,40px 40px,40px 40px}
header{padding:22px 32px;border-bottom:1px solid var(--line);display:flex;align-items:baseline;gap:16px;backdrop-filter:blur(6px);position:sticky;top:0;background:rgba(8,9,12,.8);z-index:9}
h1{font-size:18px;margin:0;letter-spacing:2px;font-weight:800}h1 b{color:var(--accent);text-shadow:0 0 18px rgba(255,117,0,.5)}
.sub{color:var(--dim);font-size:12px}
.live{margin-left:auto;font-size:11px;color:var(--ok);letter-spacing:1px}
.live::before{content:"●";margin-right:6px;animation:pulse 1.6s infinite}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.25}}
main{padding:24px 32px;display:grid;gap:18px;grid-template-columns:repeat(auto-fit,minmax(270px,1fr));max-width:1560px;margin:0 auto}
.card{background:linear-gradient(180deg,rgba(255,255,255,.025),transparent),var(--panel);border:1px solid var(--line);border-radius:14px;padding:18px 20px;animation:rise .5s ease both}
.card:nth-child(2){animation-delay:.05s}.card:nth-child(3){animation-delay:.1s}.card:nth-child(4){animation-delay:.15s}
@keyframes rise{from{opacity:0;transform:translateY(10px)}to{opacity:1;transform:none}}
.card h2{margin:0 0 12px;font-size:11px;color:var(--dim);font-weight:700;letter-spacing:2px;text-transform:uppercase}
.big{font-size:30px;font-weight:800;color:var(--accent);font-variant-numeric:tabular-nums;letter-spacing:-.5px}
.kv{display:flex;justify-content:space-between;padding:5px 0;border-bottom:1px dashed var(--line);font-size:13px}
.kv:last-child{border:0}.kv span:last-child{color:var(--dim);font-variant-numeric:tabular-nums}
.wide{grid-column:1/-1}
ul.findings{list-style:none;margin:0;padding:0}
ul.findings li{padding:9px 12px;border-left:3px solid var(--ok);background:#0d1117;margin-bottom:6px;border-radius:0 8px 8px 0;font-size:13px}
li.warn{border-color:var(--warn)}li.bad{border-color:var(--bad)}li.info{border-color:#4a9eff}
table{width:100%;border-collapse:collapse;font-size:13px}th,td{text-align:left;padding:7px 9px;border-bottom:1px solid var(--line)}
th{color:var(--dim);font-weight:600;font-size:11px;letter-spacing:1px}
td{font-variant-numeric:tabular-nums}
.bar{height:26px;border-radius:6px;position:relative;margin:5px 0;min-width:2px;transition:filter .15s}
.bar:hover{filter:brightness(1.35)}
.bar span{position:absolute;right:8px;top:3px;font-size:11px;color:rgba(0,0,0,.75);font-weight:700}
.barlbl{display:flex;justify-content:space-between;font-size:12px;color:var(--dim)}
.rw{display:flex;align-items:center;gap:10px;padding:8px 10px;border:1px solid var(--line);border-radius:8px;margin-bottom:6px;font-size:12px}
.rw .f{flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;color:var(--cyan)}
.rw .t{color:var(--dim)}
button.rb{background:rgba(255,117,0,.12);color:var(--accent);border:1px solid rgba(255,117,0,.45);border-radius:6px;padding:4px 12px;font-size:12px;cursor:pointer;transition:all .15s}
button.rb:hover{background:var(--accent);color:#000;box-shadow:0 0 14px rgba(255,117,0,.5)}
button.rb:disabled{opacity:.4;cursor:default}
#toast{position:fixed;bottom:24px;left:50%;transform:translateX(-50%);background:var(--panel);border:1px solid var(--accent);border-radius:10px;padding:10px 18px;font-size:13px;opacity:0;transition:opacity .25s;pointer-events:none;z-index:99;box-shadow:0 8px 30px rgba(0,0,0,.5)}
.legend{display:flex;gap:16px;flex-wrap:wrap;font-size:12px;color:var(--dim);margin-top:8px}
.legend i{display:inline-block;width:10px;height:10px;border-radius:3px;margin-right:5px;vertical-align:-1px}
svg text{font-family:inherit}
</style></head><body>
<header><h1>DSH <b>PROFILER</b></h1><span class="sub" id="meta">loading…</span><span class="live" id="live">LIVE</span></header>
<main id="root"></main>
<div id="toast"></div>
<script>
const f=(n)=>new Intl.NumberFormat('en-US').format(n);
const esc=(x)=>String(x).replace(/[&<>"]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]));
const COLORS=['#ff7500','#22d3ee','#a78bfa','#3ecf8e','#ffb020','#ff5d5d','#4a9eff','#f472b6'];
function toast(msg,ok){const t=document.getElementById('toast');t.textContent=msg;t.style.borderColor=ok?'var(--ok)':'var(--bad)';t.style.opacity=1;setTimeout(()=>t.style.opacity=0,2600)}

/* 时间序列折线图（含 P50/P95 参考线） */
function seriesChart(series){
  if(!series||!series.length)return '<div class="sub">等待工具调用样本…</div>';
  const W=1100,H=180,P=28;
  const ms=series.map(s=>s.ms),max=Math.max(10,...ms);
  const t0=series[0].t,t1=series[series.length-1].t||t0+1;
  const X=(t)=>P+((t-t0)/Math.max(1,t1-t0))*(W-P-8);
  const Y=(v)=>H-10-(v/max)*(H-44);
  const pts=series.map(s=>X(s.t).toFixed(1)+','+Y(s.ms).toFixed(1)).join(' ');
  const sorted=[...ms].sort((a,b)=>a-b);
  const q=(p)=>sorted[Math.min(sorted.length-1,Math.ceil(p/100*sorted.length)-1)];
  const p50=q(50),p95=q(95);
  const dots=series.filter(s=>s.ms>=p95).map(s=>'<circle cx="'+X(s.t)+'" cy="'+Y(s.ms)+'" r="3.5" fill="var(--bad)"><title>'+esc(s.tool)+' '+s.ms+'ms</title></circle>').join('');
  return '<svg viewBox="0 0 '+W+' '+H+'" width="100%" preserveAspectRatio="none" style="display:block">'+
    '<defs><linearGradient id="ag" x1="0" y1="0" x2="0" y2="1"><stop offset="0" stop-color="#ff7500" stop-opacity=".35"/><stop offset="1" stop-color="#ff7500" stop-opacity="0"/></linearGradient></defs>'+
    [0.25,0.5,0.75,1].map(g=>'<line x1="'+P+'" y1="'+Y(max*g)+'" x2="'+(W-8)+'" y2="'+Y(max*g)+'" stroke="#1e2530" stroke-width="1"/><text x="4" y="'+(Y(max*g)+4)+'" fill="#54606e" font-size="9">'+Math.round(max*g)+'</text>').join('')+
    '<polygon points="'+P+','+(H-10)+' '+pts+' '+(W-8)+','+(H-10)+'" fill="url(#ag)"/>'+
    '<polyline points="'+pts+'" fill="none" stroke="#ff7500" stroke-width="2" stroke-linejoin="round"/>'+
    '<line x1="'+P+'" y1="'+Y(p95)+'" x2="'+(W-8)+'" y2="'+Y(p95)+'" stroke="var(--warn)" stroke-dasharray="5 4" stroke-width="1"/><text x="'+(W-70)+'" y="'+(Y(p95)-4)+'" fill="var(--warn)" font-size="10">P95 '+p95+'ms</text>'+
    '<line x1="'+P+'" y1="'+Y(p50)+'" x2="'+(W-8)+'" y2="'+Y(p50)+'" stroke="#3ecf8e" stroke-dasharray="2 4" stroke-width="1"/><text x="'+(W-70)+'" y="'+(Y(p50)+12)+'" fill="#3ecf8e" font-size="10">P50 '+p50+'ms</text>'+
    dots+'</svg>';
}

/* 火焰条：按工具总耗时占比 */
function flame(perTool){
  if(!perTool||!perTool.length)return '<div class="sub">暂无样本</div>';
  const max=Math.max(...perTool.map(t=>t.totalMs),1);
  return perTool.slice(0,10).map((t,i)=>{
    const w=Math.max(2,t.totalMs/max*100);
    const inner=t.count>1?Math.max(4,t.avgMs/t.maxMs*100):100;
    return '<div class="barlbl"><span>'+esc(t.tool)+(t.failed?' <b style="color:var(--bad)">✕'+t.failed+'</b>':'')+'</span><span>'+t.count+' 次 · avg '+t.avgMs+'ms · max '+t.maxMs+'ms</span></div>'+
      '<div class="bar" style="width:'+w.toFixed(1)+'%;background:linear-gradient(90deg,'+COLORS[i%COLORS.length]+'33,'+COLORS[i%COLORS.length]+');"><div style="width:'+inner.toFixed(0)+'%;height:100%;background:rgba(255,255,255,.18);border-radius:6px"></div><span>'+f(t.totalMs)+'ms</span></div>';
  }).join('');
}

/* Token 环形图 */
function donut(tokens){
  const entries=Object.entries(tokens||{}).filter(([,v])=>v>0);
  const total=entries.reduce((a,[,v])=>a+v,0);
  if(!total)return '<div class="sub">无 token 数据</div>';
  let acc=0;const R=52,C=2*Math.PI*R;
  const segs=entries.map(([k,v],i)=>{
    const frac=v/total;
    const s='<circle cx="70" cy="70" r="'+R+'" fill="none" stroke="'+COLORS[i%COLORS.length]+'" stroke-width="20" stroke-dasharray="'+(frac*C).toFixed(1)+' '+C.toFixed(1)+'" stroke-dashoffset="'+(-acc*C).toFixed(1)+'" transform="rotate(-90 70 70)"><title>'+esc(k)+' '+f(v)+'</title></circle>';
    acc+=frac;return s;
  }).join('');
  return '<div style="display:flex;gap:24px;align-items:center;flex-wrap:wrap"><svg width="140" height="140" viewBox="0 0 140 140">'+segs+
    '<text x="70" y="66" text-anchor="middle" fill="var(--fg)" font-size="15" font-weight="800">'+(total/1e6).toFixed(1)+'M</text><text x="70" y="84" text-anchor="middle" fill="var(--dim)" font-size="10">tokens</text></svg>'+
    '<div>'+entries.map(([k,v],i)=>'<div class="kv" style="min-width:250px"><span><i style="display:inline-block;width:9px;height:9px;border-radius:2px;background:'+COLORS[i%COLORS.length]+';margin-right:7px"></i>'+esc(k)+'</span><span>'+f(v)+' · '+(v/total*100).toFixed(1)+'%</span></div>').join('')+'</div></div>';
}

/* 启动瀑布 */
function bootfall(boot,baseline,up){
  const stages=[['进程启动',Math.max(0,boot.appliedAfterMs-boot.selfApplyMs),'#22d3ee'],['插件挂载',boot.selfApplyMs,'#ff7500'],['运行至今',Math.max(1,up*1000-boot.appliedAfterMs),'#3ecf8e']];
  const sum=stages.reduce((a,s)=>a+s[1],0);
  return stages.map(([n,v,c])=>'<div class="barlbl"><span>'+n+'</span><span>'+(v>=1000?(v/1000).toFixed(1)+'s':v+'ms')+' · '+(v/sum*100).toFixed(1)+'%</span></div><div class="bar" style="width:'+Math.max(2,v/sum*100).toFixed(1)+'%;background:'+c+'"></div>').join('')+
    '<div class="sub" style="margin-top:6px">实测基线：冷启动 '+baseline.coldStartMs+'ms / 热启动 '+baseline.warmStartMs+'ms</div>';
}

async function restore(id){
  try{
    const r=await fetch('/dsh-profiler/rewind/restore',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({id})});
    const d=await r.json();
    toast(d.ok?('✓ 已回档：'+d.file+'（'+d.action+'）'):('✕ 回档失败：'+d.error),d.ok);
  }catch(e){toast('✕ 请求失败：'+e,false)}
}

async function boot(){
  const r=await fetch('/dsh-profiler/report.json',{cache:'no-store'});const d=await r.json();
  document.getElementById('meta').textContent='v'+d.version+' · '+new Date(d.generatedAt).toLocaleString();
  const t=d.tools,s=d.sessions;
  document.getElementById('root').innerHTML=[
    card('BOOT','<div class="big">'+d.boot.appliedAfterMs+' ms</div><div class="sub">进程启动 → 插件挂载</div>',[['冷启动实测',d.baseline.coldStartMs+' ms'],['热启动实测',d.baseline.warmStartMs+' ms'],['进程数',d.process.processes]]),
    card('PROCESS','<div class="big">'+d.process.rssMb+' MB</div><div class="sub">RSS · heap '+d.process.heapUsedMb+' MB</div>',[['Uptime',d.process.uptimeSec+' s'],['Node',d.process.node],['平台',d.process.platform]]),
    card('TOOLS','<div class="big">'+t.p95Ms+' ms</div><div class="sub">P95 · 采样 '+t.sampled+' 次</div>',[['P50',t.p50Ms+' ms'],['P99',t.p99Ms+' ms'],['成功率',t.successRate+' %']]),
    card('SESSIONS','<div class="big">'+s.count+'</div><div class="sub">累计 '+f(Object.values(s.tokens).reduce((a,b)=>a+b,0))+' tokens</div>',[['缓存命中',s.cacheHitRate+' %'],['存储',Math.round(s.storeBytes/1024)+' KB'],['记忆插件',d.plugins.hasMemoryPlugin?'已安装':'缺失']]),
    '<div class="card wide"><h2>TOOL LATENCY TIMELINE</h2>'+seriesChart(t.series)+'<div class="legend"><span><i style="background:#ff7500"></i>延迟</span><span><i style="background:var(--warn)"></i>P95</span><span><i style="background:#3ecf8e"></i>P50</span><span><i style="background:var(--bad)"></i>慢调用点</span></div></div>',
    '<div class="card wide"><h2>FLAME · 工具总耗时占比</h2>'+flame(t.perTool)+'</div>',
    '<div class="card"><h2>TOKEN BREAKDOWN</h2>'+donut(s.tokens)+'</div>',
    '<div class="card"><h2>BOOT WATERFALL</h2>'+bootfall(d.boot,d.baseline,d.process.uptimeSec)+'</div>',
    '<div class="card wide"><h2>REWIND · 变更回档（CoreProtect 模式）</h2>'+
      (d.rewind.recent.length?d.rewind.recent.map(e=>'<div class="rw"><span class="t">'+new Date(e.ts).toLocaleTimeString()+'</span><span class="f" title="'+esc(e.file)+'">'+esc(e.file)+'</span><span class="t">'+esc(e.tool)+' · '+f(e.sizeBytes)+'B'+(e.prevExists?'':' · 新文件')+'</span><button class="rb" onclick="restore(\\''+e.id+'\\')">回档</button></div>').join(''):'<div class="sub">暂无变更记录：发生一次写文件类工具调用后，这里会出现可回滚的时间点。</div>')+'</div>',
    '<div class="card wide"><h2>DIAGNOSIS</h2><ul class="findings">'+d.findings.map(x=>'<li class="'+x.level+'">'+esc(x.message)+'</li>').join('')+'</ul></div>',
    '<div class="card wide"><h2>TOP SESSIONS</h2><table><tr><th>Title</th><th>Tokens</th><th>Size</th></tr>'+
      s.top.map(x=>'<tr><td>'+esc(x.title||'(untitled)')+'</td><td>'+f(x.tokens)+'</td><td>'+Math.round(x.fileBytes/1024)+' KB</td></tr>').join('')+'</table></div>'
  ].join('');
}
function card(title,body,kv){
  return '<div class="card"><h2>'+title+'</h2>'+body+kv.map(([k,v])=>'<div class="kv"><span>'+k+'</span><span>'+v+'</span></div>').join('')+'</div>';
}
boot().catch(e=>{document.getElementById('root').innerHTML='<div class="card wide">failed: '+e+'</div>'});
setInterval(()=>{boot().catch(()=>{})},5000);
</script></body></html>`

/* -------------------------------------------------------------------------- */
/* 插件入口                                                                    */
/* -------------------------------------------------------------------------- */

function apply(ctx, rawConfig) {
  const config = normalizeConfig(rawConfig)
  if (!config.enabled) return
  const appliedAt = Date.now()
  const processStart = appliedAt - Math.round(process.uptime() * 1000)
  const collector = createCollector(config)
  const scanners = createScanners(config)
  const rewind = createRewind(config)
  let applyMs = 0
  const t0 = performance.now()

  async function report() {
    const proc = process.memoryUsage()
    const sessions = scanners.scanSessions()
    const plugins = scanners.scanPlugins()
    const base = {
      version: PLUGIN_VERSION,
      generatedAt: Date.now(),
      baseline: BASELINE,
      boot: {
        processStartedAt: processStart,
        pluginAppliedAt: appliedAt,
        appliedAfterMs: Math.max(0, Math.round(appliedAt - processStart)),
        selfApplyMs: Math.round(applyMs)
      },
      process: {
        uptimeSec: Math.round(process.uptime()),
        rssMb: mb(proc.rss),
        heapUsedMb: mb(proc.heapUsed),
        heapTotalMb: mb(proc.heapTotal),
        processes: BASELINE.processes,
        node: process.version,
        platform: process.platform + '-' + process.arch,
        cpu: os.cpus().length
      },
      tools: collector.snapshot(),
      sessions,
      plugins,
      rewind: rewind.summary()
    }
    base.findings = buildFindings(base, config)
    return base
  }

  function slice(data, scope, limit) {
    if (scope === 'tools') return { version: data.version, generatedAt: data.generatedAt, tools: data.tools, findings: data.findings.filter((x) => x.code.startsWith('tools/')) }
    if (scope === 'sessions') return { version: data.version, generatedAt: data.generatedAt, sessions: data.sessions, findings: data.findings.filter((x) => x.code.startsWith('memory/') || x.code.startsWith('session/')) }
    if (scope === 'plugins') return { version: data.version, generatedAt: data.generatedAt, plugins: data.plugins, findings: data.findings.filter((x) => x.code.startsWith('plugins/')) }
    if (scope === 'rewind') return { version: data.version, generatedAt: data.generatedAt, rewind: data.rewind, findings: data.findings.filter((x) => x.code.startsWith('rewind/')) }
    return { version: data.version, generatedAt: data.generatedAt, baseline: data.baseline, boot: data.boot, process: data.process, tools: data.tools, sessions: data.sessions, plugins: data.plugins, rewind: data.rewind, findings: data.findings, topN: limit }
  }

  /* 1) 工具：让 Agent 自己也能查运行时状况 */
  try {
    ctx.tools.register({
      name: 'profiler_report',
      description: '读取 DeepSeek Harness 运行时剖析数据：启动耗时、RSS/堆、工具调用延迟（p50/p95/p99）、会话 token 归因、文件变更回档历史与诊断建议。',
      parameters: {
        type: 'object',
        additionalProperties: false,
        properties: {
          scope: { type: 'string', enum: ['summary', 'tools', 'sessions', 'plugins', 'rewind'], description: '要返回的切片，默认 summary。' },
          limit: { type: 'integer', minimum: 1, maximum: 20, description: 'top 列表长度，默认 5。' }
        }
      },
      output: {
        schema: {
          type: 'object',
          additionalProperties: false,
          properties: {
            summary: { type: 'string', required: true },
            data: { type: 'object', additionalProperties: true, required: true }
          }
        },
        render: (_args, value) => [{ type: 'text', text: value.summary }]
      },
      async execute(args) {
        const scope = ['summary', 'tools', 'sessions', 'plugins', 'rewind'].includes(args?.scope) ? args.scope : 'summary'
        const limit = Number.isFinite(args?.limit) ? Math.min(20, Math.max(1, args.limit)) : 5
        const full = await report()
        const data = slice(full, scope, limit)
        return { summary: summarize(full), data }
      },
      presentCall: () => ({ card: 'generic', title: 'Reading runtime profile', kind: 'other' })
    })
  } catch (err) {
    try { ctx.logger?.warn?.(`[${NS}] tool registration failed: ${err && err.message}`) } catch { /* noop */ }
  }

  /* 2) 会话事件：延迟采样 + 写前快照 */
  try {
    ctx.on('session/event', (session, event) => {
      try {
        collector.observe(session, event)
        if (!event || typeof event.type !== 'string') return
        if (event.type === 'tool/call') {
          const { callId, toolName } = collector.readCall(event)
          rewind.stage(callId, toolName, event, session?.id)
        } else if (event.type === 'tool/result') {
          const { callId } = collector.readCall(event)
          const ok = !event.data?.message?.isError
          rewind.commit(callId, ok)
        }
      } catch { /* 采样失败绝不影响主流程 */ }
    }, { global: true })
  } catch { /* 老版本没有 session/event 时静默降级 */ }

  /* 3) /profiler 命令 */
  try {
    ctx.commands.register({
      definitionId: 'dsh-profiler',
      name: 'profiler',
      description: '运行时剖析与回档（/profiler [tools|sessions|plugins|rewind|restore <id>|json|export]）',
      handler: async (invocation) => {
        const raw = String(invocation?.rawInput || '').trim()
        const arg = raw.toLowerCase()
        const data = await report()
        if (arg === 'json') return { kind: 'success', text: '```json\n' + JSON.stringify(data, null, 2) + '\n```' }
        if (arg === 'tools') {
          const rows = data.tools.perTool.map((t) => `${t.tool}\t${t.count} 次\tavg ${t.avgMs}ms\tmax ${t.maxMs}ms`)
          return { kind: 'success', text: rows.length ? rows.join('\n') : '暂无工具调用样本。' }
        }
        if (arg === 'sessions') {
          return { kind: 'success', text: data.sessions.top.map((s) => `${s.tokens} tokens\t${s.title || '(untitled)'}`).join('\n') || '暂无会话。' }
        }
        if (arg === 'plugins') {
          return { kind: 'success', text: data.plugins.profiles.map((p) => `[${p.profile}] ${p.count} bundles\n  ` + p.bundles.map((b) => `${b.bundle}@${b.version || '?'}`).join('\n  ')).join('\n') || '未发现 profile。' }
        }
        if (arg === 'rewind') {
          const h = rewind.history(15)
          if (!h.length) return { kind: 'success', text: '回档历史为空。发生一次写文件类工具调用后会出现可回滚的时间点。' }
          return { kind: 'success', text: h.map((e) => `${e.id}\t${new Date(e.ts).toLocaleTimeString()}\t${e.tool}\t${e.file}${e.prevExists ? '' : '（新文件）'}`).join('\n') + '\n\n用 /profiler restore <id> 回滚到该时间点之前的状态。' }
        }
        if (arg.startsWith('restore ')) {
          const id = raw.slice(8).trim()
          const r = rewind.restore(id)
          return r.ok ? { kind: 'success', text: `✓ 已回档：${r.file}\n${r.action}` } : { kind: 'error', text: `回档失败：${r.error}` }
        }
        if (arg === 'export') {
          const dir = path.join(DSH_HOME, 'dsh-profiler')
          fs.mkdirSync(dir, { recursive: true })
          const file = path.join(dir, `report-${new Date().toISOString().replace(/[:.]/g, '-')}.json`)
          fs.writeFileSync(file, JSON.stringify(data, null, 2), 'utf8')
          return { kind: 'success', text: `已导出剖析报告：${file}` }
        }
        return { kind: 'success', text: summarize(data) }
      }
    })
  } catch (err) {
    try { ctx.logger?.warn?.(`[${NS}] command registration failed: ${err && err.message}`) } catch { /* noop */ }
  }

  /* 4) HTTP：看板 + JSON + 回档 */
  let server = null
  try { server = (typeof ctx.get === 'function' ? ctx.get('webServer') : undefined) || ctx.webServer } catch { server = null }
  const disposers = []
  if (config.http && server && typeof server.register === 'function') {
    const connection = (() => { try { return typeof ctx.get === 'function' ? ctx.get('connection') : ctx.connection } catch { return null } })()
    const guard = (req, res) => {
      try {
        if (!connection || typeof connection.requestRejection !== 'function') return false
        const code = connection.requestRejection(req)
        if (code === undefined || code === null || code === false) return false
        res.statusCode = typeof code === 'number' ? code : 403
        res.end()
        return true
      } catch { return false }
    }
    const send = (res, status, body, type) => {
      const buf = Buffer.from(body, 'utf8')
      res.writeHead(status, { 'Content-Type': type, 'Cache-Control': 'no-store', 'Content-Length': String(buf.length) })
      res.end(buf)
    }
    const readBody = (req) => new Promise((resolve) => {
      let raw = ''
      req.on('data', (c) => { raw += c; if (raw.length > 65536) req.destroy() })
      req.on('end', () => { try { resolve(JSON.parse(raw || '{}')) } catch { resolve({}) } })
      req.on('error', () => resolve({}))
    })
    const json = async (req, res) => {
      if (guard(req, res)) return
      send(res, 200, JSON.stringify(await report()), 'application/json; charset=utf-8')
    }
    const html = (req, res) => {
      if (guard(req, res)) return
      send(res, 200, DASHBOARD, 'text/html; charset=utf-8')
    }
    const rewindJson = (req, res) => {
      if (guard(req, res)) return
      send(res, 200, JSON.stringify({ version: PLUGIN_VERSION, entries: rewind.history(100) }), 'application/json; charset=utf-8')
    }
    const rewindRestore = async (req, res) => {
      if (guard(req, res)) return
      const body = await readBody(req)
      const r = rewind.restore(String(body.id || ''))
      send(res, r.ok ? 200 : 400, JSON.stringify(r), 'application/json; charset=utf-8')
    }
    disposers.push(server.register({ kind: 'exact', path: ROUTE_BASE + '/report.json', handler: json }))
    disposers.push(server.register({ kind: 'exact', path: ROUTE_BASE + '/rewind.json', handler: rewindJson }))
    disposers.push(server.register({ kind: 'exact', path: ROUTE_BASE + '/rewind/restore', handler: rewindRestore }))
    disposers.push(server.register({ kind: 'exact', path: ROUTE_BASE, handler: html }))
    disposers.push(server.register({ kind: 'exact', path: ROUTE_BASE + '/', handler: html }))
  }

  applyMs = performance.now() - t0

  ctx.effect(() => () => {
    collector.dispose()
    for (const dispose of disposers) { try { dispose() } catch { /* noop */ } }
  }, 'dsh-profiler lifecycle')

  try { ctx.logger?.info?.(`[${NS}] profiler online v${PLUGIN_VERSION} (apply ${Math.round(applyMs)}ms, rewind=${config.rewind})`) } catch { /* noop */ }
}

export { apply, inject, name }
