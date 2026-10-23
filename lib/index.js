/* ============================================================================
 * dsh-profiler — DeepSeek Harness 运行时剖析器
 * ----------------------------------------------------------------------------
 * 对标 Minecraft 服务端的 Spark / CoreProtect：dsh 官方有 compaction 与
 * session projection，却没有任何「运行时剖析 / 启动归因 / 工具耗时」插件。
 *
 * 本插件做四件事：
 *   1) 采集：session/event 中 tool/call → tool/result 的耗时（环形缓冲）
 *   2) 归因：进程启动 → 插件挂载耗时、RSS/Heap、会话 token 分布
 *   3) 诊断：根据阈值给出 findings（启动慢、内存高、跨会话记忆缺口…）
 *   4) 暴露：/profiler 命令、profiler_report 工具、/dsh-profiler/report.json 与看板
 *
 * 设计约束：宿主半侧只依赖 Node 内置模块，不 import @deepseek-ai/*，
 * 这样在 profile 目录（无官方包软链）里也能直接加载。
 * ========================================================================== */

import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'
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
  http: true
}

/** 2026-10-23 在 Windows 实机测得（DeepSeek Harness 0.2.1-rc.2，6 进程）。 */
const BASELINE = { coldStartMs: 11738, warmStartMs: 4391, rssMb: 2021, processes: 6 }

const PLUGIN_VERSION = (() => {
  try {
    return String(JSON.parse(fs.readFileSync(path.join(PACKAGE_ROOT, 'package.json'), 'utf8')).version || '0.0.0')
  } catch { return '0.0.0' }
})()

const name = NS
/** 只声明真正必需的服务；webServer 在桌面端可能缺席，运行时探测。 */
const inject = ['tools', 'commands']

function normalizeConfig(input) {
  const out = { ...DEFAULTS }
  const src = input && typeof input === 'object' ? input : {}
  if (typeof src.enabled === 'boolean') out.enabled = src.enabled
  if (typeof src.http === 'boolean') out.http = src.http
  for (const key of ['sampleLimit', 'slowToolMs', 'sessionScanTtlMs']) {
    const n = Number(src[key])
    if (Number.isFinite(n) && n > 0) out[key] = Math.min(600000, Math.round(n))
  }
  return out
}

const mb = (bytes) => Math.round((Number(bytes) || 0) / 1048576)
const pct = (part, total) => (total > 0 ? Math.round((part / total) * 1000) / 10 : 0)

function percentile(sorted, p) {
  if (!sorted.length) return 0
  const idx = Math.min(sorted.length - 1, Math.ceil((p / 100) * sorted.length) - 1)
  return Math.round(sorted[Math.max(0, idx)])
}

/* -------------------------------------------------------------------------- */
/* 采集器                                                                      */
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

  /** 从事件里尽力提取 callId / 工具名，兼容不同版本的 data 形状。 */
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
        push({ tool: started.toolName, ms, ok: !isError, sessionId: started.sessionId })
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
        .map((g) => ({ tool: g.tool, count: g.count, avgMs: Math.round(g.totalMs / g.count), maxMs: g.maxMs, failed: g.failed }))
        .sort((a, b) => b.avgMs - a.avgMs)
      const durations = samples.map((s) => s.ms).sort((a, b) => a - b)
      return {
        sampled: samples.length,
        total,
        failed,
        successRate: total ? Math.round(((total - failed) / total) * 1000) / 10 : 100,
        avgMs: durations.length ? Math.round(durations.reduce((a, b) => a + b, 0) / durations.length) : 0,
        p95Ms: percentile(durations, 95),
        maxMs: durations.length ? durations[durations.length - 1] : 0,
        perTool,
        slowest: [...samples].sort((a, b) => b.ms - a.ms).slice(0, 5)
      }
    }
  }
}

/* -------------------------------------------------------------------------- */
/* 会话 / 插件清单扫描（带 TTL 缓存，避免每次请求都读盘）                        */
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
/* 报告构建                                                                    */
/* -------------------------------------------------------------------------- */

function buildFindings(report, config) {
  const findings = []
  const { process: proc, tools, sessions, plugins, boot } = report
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
    '',
    '诊断：',
    ...report.findings.map((f) => `  [${f.level}] ${f.message}`)
  ]
  return lines.join('\n')
}

/* -------------------------------------------------------------------------- */
/* 看板 HTML                                                                   */
/* -------------------------------------------------------------------------- */

const DASHBOARD = `<!doctype html><html lang="zh-CN"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>dsh-profiler · Runtime Profiler</title>
<style>
:root{--bg:#0b0d10;--panel:#14181d;--line:#242b33;--fg:#e8edf3;--dim:#8b97a5;--accent:#ff7500;--ok:#3ecf8e;--warn:#ffb020;--bad:#ff5d5d}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--fg);font:14px/1.55 "Segoe UI",system-ui,sans-serif}
header{padding:22px 28px;border-bottom:1px solid var(--line);display:flex;align-items:baseline;gap:14px}
h1{font-size:18px;margin:0;letter-spacing:.5px}h1 b{color:var(--accent)}
.sub{color:var(--dim);font-size:12px}
main{padding:22px 28px;display:grid;gap:16px;grid-template-columns:repeat(auto-fit,minmax(260px,1fr))}
.card{background:var(--panel);border:1px solid var(--line);border-radius:10px;padding:16px 18px}
.card h2{margin:0 0 10px;font-size:12px;color:var(--dim);font-weight:600;letter-spacing:1px;text-transform:uppercase}
.big{font-size:26px;font-weight:700;color:var(--accent)}
.kv{display:flex;justify-content:space-between;padding:4px 0;border-bottom:1px dashed var(--line);font-size:13px}
.kv:last-child{border:0}.kv span:last-child{color:var(--dim)}
.wide{grid-column:1/-1}
ul.findings{list-style:none;margin:0;padding:0}
ul.findings li{padding:8px 10px;border-left:3px solid var(--ok);background:#111519;margin-bottom:6px;border-radius:0 6px 6px 0;font-size:13px}
li.warn{border-color:var(--warn)}li.bad{border-color:var(--bad)}li.info{border-color:#4a9eff}
table{width:100%;border-collapse:collapse;font-size:13px}th,td{text-align:left;padding:6px 8px;border-bottom:1px solid var(--line)}
th{color:var(--dim);font-weight:500}
</style></head><body>
<header><h1>DSH <b>PROFILER</b></h1><span class="sub" id="meta">loading…</span></header>
<main id="root"></main>
<script>
const f=(n)=>new Intl.NumberFormat('en-US').format(n);
async function boot(){
  const r=await fetch('/dsh-profiler/report.json',{cache:'no-store'});const d=await r.json();
  document.getElementById('meta').textContent='v'+d.version+' · '+new Date(d.generatedAt).toLocaleString();
  const t=d.tools,s=d.sessions;
  const esc=(x)=>String(x).replace(/[&<>]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;'}[c]));
  document.getElementById('root').innerHTML=[
    card('BOOT', '<div class="big">'+d.boot.appliedAfterMs+' ms</div><div class="sub">进程启动 → 插件挂载</div>',
      [['冷启动实测',d.baseline.coldStartMs+' ms'],['热启动实测',d.baseline.warmStartMs+' ms'],['进程数',d.process.processes]]),
    card('PROCESS','<div class="big">'+d.process.rssMb+' MB</div><div class="sub">RSS · heap '+d.process.heapUsedMb+' MB</div>',
      [['Uptime',d.process.uptimeSec+' s'],['Node',d.process.node],['平台',d.process.platform]]),
    card('TOOLS','<div class="big">'+t.p95Ms+' ms</div><div class="sub">P95 · 采样 '+t.sampled+' 次</div>',
      [['平均',t.avgMs+' ms'],['最大',t.maxMs+' ms'],['成功率',t.successRate+' %']]),
    card('SESSIONS','<div class="big">'+s.count+'</div><div class="sub">累计 '+f(Object.values(s.tokens).reduce((a,b)=>a+b,0))+' tokens</div>',
      [['缓存命中',s.cacheHitRate+' %'],['存储',Math.round(s.storeBytes/1024)+' KB'],['最大会话',f(s.top[0]?s.top[0].tokens:0)+' tokens'],['记忆插件',d.plugins.hasMemoryPlugin?'已安装':'缺失']]),
    '<div class="card wide"><h2>TOKEN BREAKDOWN</h2><table><tr><th>Key</th><th>Tokens</th><th>Share</th></tr>'+
      Object.entries(s.tokens).map(([k,v])=>'<tr><td>'+esc(k)+'</td><td>'+f(v)+'</td><td>'+Math.round(v/Math.max(1,Object.values(s.tokens).reduce((a,b)=>a+b,0))*1000)/10+' %</td></tr>').join('')+
      '</table></div>',
    '<div class="card wide"><h2>DIAGNOSIS</h2><ul class="findings">'+d.findings.map(x=>'<li class="'+x.level+'">'+esc(x.message)+'</li>').join('')+'</ul></div>',
    '<div class="card wide"><h2>TOOL LATENCY</h2><table><tr><th>Tool</th><th>Count</th><th>Avg</th><th>Max</th><th>Failed</th></tr>'+
      (t.perTool.length?t.perTool.map(x=>'<tr><td>'+esc(x.tool)+'</td><td>'+x.count+'</td><td>'+x.avgMs+' ms</td><td>'+x.maxMs+' ms</td><td>'+x.failed+'</td></tr>').join(''):'<tr><td colspan="5">no samples</td></tr>')+
      '</table></div>',
    '<div class="card wide"><h2>TOP SESSIONS</h2><table><tr><th>Title</th><th>Tokens</th><th>Size</th></tr>'+
      s.top.map(x=>'<tr><td>'+esc(x.title||'(untitled)')+'</td><td>'+f(x.tokens)+'</td><td>'+Math.round(x.fileBytes/1024)+' KB</td></tr>').join('')+
      '</table></div>'
  ].join('');
}
function card(title,body,kv){
  return '<div class="card"><h2>'+title+'</h2>'+body+kv.map(([k,v])=>'<div class="kv"><span>'+k+'</span><span>'+v+'</span></div>').join('')+'</div>';
}
boot().catch(e=>{document.getElementById('root').innerHTML='<div class="card wide">failed: '+e+'</div>'});
setInterval(()=>{boot().catch(()=>{})},30000);
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
      plugins
    }
    base.findings = buildFindings(base, config)
    return base
  }

  function slice(data, scope, limit) {
    if (scope === 'tools') return { version: data.version, generatedAt: data.generatedAt, tools: data.tools, findings: data.findings.filter((f) => f.code.startsWith('tools/')) }
    if (scope === 'sessions') return { version: data.version, generatedAt: data.generatedAt, sessions: data.sessions, findings: data.findings.filter((f) => f.code.startsWith('memory/') || f.code.startsWith('session/')) }
    if (scope === 'plugins') return { version: data.version, generatedAt: data.generatedAt, plugins: data.plugins, findings: data.findings.filter((f) => f.code.startsWith('plugins/')) }
    return { version: data.version, generatedAt: data.generatedAt, baseline: data.baseline, boot: data.boot, process: data.process, tools: data.tools, sessions: data.sessions, plugins: data.plugins, findings: data.findings, topN: limit }
  }

  /* 1) 工具：让 Agent 自己也能查运行时状况 */
  try {
    ctx.tools.register({
      name: 'profiler_report',
      description: '读取 DeepSeek Harness 运行时剖析数据：启动耗时、RSS/堆、工具调用延迟（avg/p95）、会话 token 归因、插件清单与诊断建议。用于回答“为什么启动慢/哪个工具慢/一共花了多少 token”。',
      parameters: {
        type: 'object',
        additionalProperties: false,
        properties: {
          scope: { type: 'string', enum: ['summary', 'tools', 'sessions', 'plugins'], description: '要返回的切片，默认 summary。' },
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
        const scope = ['summary', 'tools', 'sessions', 'plugins'].includes(args?.scope) ? args.scope : 'summary'
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

  /* 2) 会话事件采样：tool/call → tool/result */
  try {
    ctx.on('session/event', (session, event) => {
      try { collector.observe(session, event) } catch { /* 采样失败绝不影响主流程 */ }
    }, { global: true })
  } catch { /* 老版本没有 session/event 时静默降级 */ }

  /* 3) /profiler 命令 */
  try {
    ctx.commands.register({
      definitionId: 'dsh-profiler',
      name: 'profiler',
      description: '打印运行时剖析摘要（/profiler [tools|sessions|plugins|json|export]）',
      handler: async (invocation) => {
        const arg = String(invocation?.rawInput || '').trim().toLowerCase()
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

  /* 4) HTTP：看板 + JSON。桌面端可能没有 webServer，静默跳过。 */
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
    const json = async (req, res) => {
      if (guard(req, res)) return
      const body = Buffer.from(JSON.stringify(await report()), 'utf8')
      res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8', 'Cache-Control': 'no-store', 'Content-Length': String(body.length) })
      res.end(body)
    }
    const html = (req, res) => {
      if (guard(req, res)) return
      const body = Buffer.from(DASHBOARD, 'utf8')
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-store', 'Content-Length': String(body.length) })
      res.end(body)
    }
    disposers.push(server.register({ kind: 'exact', path: ROUTE_BASE + '/report.json', handler: json }))
    disposers.push(server.register({ kind: 'exact', path: ROUTE_BASE, handler: html }))
    disposers.push(server.register({ kind: 'exact', path: ROUTE_BASE + '/', handler: html }))
  }

  applyMs = performance.now() - t0

  ctx.effect(() => () => {
    collector.dispose()
    for (const dispose of disposers) { try { dispose() } catch { /* noop */ } }
  }, 'dsh-profiler lifecycle')

  try { ctx.logger?.info?.(`[${NS}] profiler online (apply ${Math.round(applyMs)}ms)`) } catch { /* noop */ }
}

export { apply, inject, name }
