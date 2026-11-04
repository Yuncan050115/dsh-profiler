/**
 * dsh-profiler smoke test — 不启动 DSH，用假 ctx 验证插件契约：
 *   register(tool) / register(command) / session/event 采样 / HTTP 路由 / 报告字段。
 * 运行：node test/smoke.mjs
 *
 * 报告里有两项是真的去读 ~/.dsh 的（会话存储、profile 插件清单）。CI 上没有本机
 * 安装，所以先造一个临时 DSH_HOME 当夹具，再动态 import 插件 —— lib/index.js 在
 * 模块顶层就把 DSH_HOME 固化了，静态 import 会先于夹具生效。这样断言测的是
 * 「扫描逻辑对真实文件结构的解析」，而不是我这台机器的偶然状态。
 */
import assert from 'node:assert/strict'
import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'

const FIXTURE_HOME = path.join(os.tmpdir(), `dsh-profiler-fixture-${process.pid}`)
const SESSION_DIR = path.join(FIXTURE_HOME, 'storages', 'session_projcache', 'sessions')
const PROFILE_DIR = path.join(FIXTURE_HOME, 'profiles', 'acp')
fs.mkdirSync(SESSION_DIR, { recursive: true })
fs.mkdirSync(path.join(PROFILE_DIR, 'node_modules', '@deepseek-ai', 'dsh-core'), { recursive: true })

// 会话文档：形状对齐 host 的 record/rows 存储，token 统计走 tokenUsage.val.totals
fs.writeFileSync(path.join(SESSION_DIR, 'sess-fixture.json'), JSON.stringify({
  record: {
    identity: { id: 'sess-fixture', createdAt: 1730000000000 },
    rows: {
      title: { val: 'smoke fixture session' },
      tokenUsage: { val: { totals: { uncachedInputTokens: 4000, cacheReadTokens: 6000, outputTokens: 1200 } } },
    },
  },
}))
// 故意再放一个损坏文件：扫描器要跳过它，而不是让整份报告崩掉
fs.writeFileSync(path.join(SESSION_DIR, 'broken.json'), '{"record":{"rows"')
fs.writeFileSync(path.join(PROFILE_DIR, 'package.json'), JSON.stringify({
  name: 'dsh-profile-acp',
  dsh: { profile: { bundles: ['@deepseek-ai/dsh-core', 'some-third-party-bundle'] } },
  dependencies: { '@deepseek-ai/dsh-core': '0.2.1', 'some-third-party-bundle': '^1.0.0' },
}))
fs.writeFileSync(path.join(PROFILE_DIR, 'node_modules', '@deepseek-ai', 'dsh-core', 'package.json'),
  JSON.stringify({ name: '@deepseek-ai/dsh-core', version: '0.2.1-rc.5', description: 'core' }))

process.env.DSH_HOME = FIXTURE_HOME
const plugin = await import('../lib/index.js')

const tools = new Map()
const commands = new Map()
const routes = new Map()
const disposers = []

const fakeCtx = {
  logger: { info() {}, warn() {}, error() {} },
  tools: {
    register(def) {
      assert.equal(typeof def.name, 'string')
      assert.equal(typeof def.execute, 'function')
      assert.equal(typeof def.output?.render, 'function')
      assert.equal(typeof def.output?.schema, 'object')
      tools.set(def.name, def)
      return () => tools.delete(def.name)
    }
  },
  commands: {
    register(def) {
      assert.equal(typeof def.name, 'string')
      assert.equal(typeof def.handler, 'function')
      commands.set(def.name, def)
      return () => commands.delete(def.name)
    }
  },
  on(event, handler) { globalThis.__handlers = globalThis.__handlers || {}; (globalThis.__handlers[event] ||= []).push(handler); return () => {} },
  effect(fn) { const dispose = fn(); if (typeof dispose === 'function') disposers.push(dispose); return () => {} },
  get(service) {
    if (service === 'webServer') {
      return { register(route) { routes.set(route.path, route); return () => routes.delete(route.path) } }
    }
    if (service === 'connection') return { requestRejection() { return false } }
    return undefined
  }
}

plugin.apply(fakeCtx, { enabled: true, sampleLimit: 10, slowToolMs: 100, sessionScanTtlMs: 60000, http: true })

assert.equal(plugin.name, 'dsh-profiler')
assert.ok(Array.isArray(plugin.inject) && plugin.inject.includes('tools'))
assert.ok(tools.has('profiler_report'), 'profiler_report tool must be registered')
assert.ok(commands.has('profiler'), '/profiler command must be registered')
assert.ok(routes.has('/dsh-profiler/report.json'), 'report route must be registered')
assert.ok(routes.has('/dsh-profiler'), 'dashboard route must be registered')

// 模拟一次工具调用：120ms，超阈值
const emit = (session, event) => (globalThis.__handlers['session/event'] || []).forEach((h) => h(session, event))
const session = { id: 'session-test' }
emit(session, { type: 'tool/call', time: 1000, data: { callId: 'c1', name: 'read_file' } })
emit(session, { type: 'tool/result', time: 1120, data: { message: { source: { callId: 'c1' }, isError: false } } })

const toolResult = await tools.get('profiler_report').execute({ scope: 'summary', limit: 3 }, {})
assert.ok(toolResult.summary.includes('dsh-profiler'))
assert.ok(toolResult.summary.includes('工具'))
assert.equal(toolResult.data.tools.sampled, 1)
assert.equal(toolResult.data.tools.perTool[0].tool, 'read_file')
assert.equal(toolResult.data.tools.perTool[0].avgMs, 120)
assert.ok(toolResult.data.findings.some((f) => f.code === 'tools/slow-p95'), 'slow p95 finding expected')

// 命令
const cmd = await commands.get('profiler').handler({ rawInput: 'tools' })
assert.equal(cmd.kind, 'success')
assert.ok(cmd.text.includes('read_file'))

// HTTP 路由
const res = {
  statusCode: 0, headers: {}, body: '',
  writeHead(code, headers) { this.statusCode = code; this.headers = headers },
  end(chunk) { this.body = chunk ? String(chunk) : '' }
}
await routes.get('/dsh-profiler/report.json').handler({ method: 'GET', url: '/dsh-profiler/report.json' }, res)
assert.equal(res.statusCode, 200)
const report = JSON.parse(res.body)
assert.equal(report.version, '2.0.0')
assert.ok(report.process.rssMb > 0)
assert.ok(report.sessions.count >= 1, 'sessions should be scanned from the fixture DSH_HOME')
// 夹具里放了一个正常文档 + 一个损坏文档：count 只算解析成功的，损坏的那个必须被跳过
assert.equal(report.sessions.count, 1, 'only the well-formed session should be counted')
assert.equal(report.sessions.top[0].id, 'sess-fixture')
// 6000 cacheRead / (6000 + 4000) uncachedInput = 60%
assert.equal(report.sessions.cacheHitRate, 60, 'cache hit rate must come from the fixture totals')
assert.equal(typeof report.sessions.cacheHitRate, 'number')
assert.ok(Array.isArray(report.plugins.profiles) && report.plugins.profiles.length >= 1)
assert.equal(report.plugins.profiles[0].profile, 'acp')
assert.equal(report.plugins.thirdParty, 1, 'the fixture declares exactly one non-@deepseek-ai bundle')
assert.ok(Array.isArray(report.findings) && report.findings.length >= 1)
assert.ok(report.rewind && Array.isArray(report.rewind.recent), 'rewind summary must exist')

// 回档：写前快照 → 修改 → restore 恢复
const tmpFile = path.join(os.tmpdir(), `dsh-profiler-rewind-${Date.now()}.txt`)
fs.writeFileSync(tmpFile, 'VERSION-OLD', 'utf8')
emit(session, { type: 'tool/call', time: 2000, data: { callId: 'w1', name: 'write_file', args: { path: tmpFile, content: 'VERSION-NEW' } } })
emit(session, { type: 'tool/result', time: 2050, data: { message: { source: { callId: 'w1' }, isError: false } } })
fs.writeFileSync(tmpFile, 'VERSION-NEW', 'utf8') // 工具实际写入
const histRes = { statusCode: 0, headers: {}, body: '', writeHead(c, h) { this.statusCode = c; this.headers = h }, end(c) { this.body = String(c || '') } }
routes.get('/dsh-profiler/rewind.json').handler({ method: 'GET', url: '/dsh-profiler/rewind.json' }, histRes)
const hist = JSON.parse(histRes.body).entries
assert.ok(hist.length >= 1 && hist[0].file === tmpFile, 'rewind history must contain the staged write')
assert.equal(hist[0].prevExists, true)
// 通过命令执行回档
const restoreCmd = await commands.get('profiler').handler({ rawInput: `restore ${hist[0].id}` })
assert.equal(restoreCmd.kind, 'success', 'restore must succeed: ' + restoreCmd.text)
assert.equal(fs.readFileSync(tmpFile, 'utf8'), 'VERSION-OLD', 'file content must be rolled back')
// 新文件场景：prevExists=false → 回档 = 删除
const newFile = path.join(os.tmpdir(), `dsh-profiler-rewind-new-${Date.now()}.txt`)
emit(session, { type: 'tool/call', time: 3000, data: { callId: 'w2', name: 'write_file', args: { path: newFile, content: 'x' } } })
emit(session, { type: 'tool/result', time: 3010, data: { message: { source: { callId: 'w2' }, isError: false } } })
fs.writeFileSync(newFile, 'x', 'utf8')
const hist2 = JSON.parse((() => { const r2 = { statusCode: 0, body: '', writeHead() {}, end(c) { this.body = String(c || '') } }; routes.get('/dsh-profiler/rewind.json').handler({}, r2); return r2.body })()).entries
const newEntry = hist2.find((e) => e.file === newFile)
assert.ok(newEntry && newEntry.prevExists === false, 'new file entry must have prevExists=false')
const r2 = await commands.get('profiler').handler({ rawInput: `restore ${newEntry.id}` })
assert.equal(r2.kind, 'success')
assert.ok(!fs.existsSync(newFile), 'new file must be deleted on restore')
fs.unlinkSync(tmpFile)
// rewind 列表命令
const rewindCmd = await commands.get('profiler').handler({ rawInput: 'rewind' })
assert.equal(rewindCmd.kind, 'success')
assert.ok(rewindCmd.text.includes('restore'))

// 导出命令
const exported = await commands.get('profiler').handler({ rawInput: 'export' })
assert.equal(exported.kind, 'success')
const exportedPath = exported.text.replace('已导出剖析报告：', '')
assert.ok(fs.existsSync(exportedPath), 'export file must exist')
const exportedDoc = JSON.parse(fs.readFileSync(exportedPath, 'utf8'))
assert.equal(exportedDoc.version, '2.0.0')
fs.unlinkSync(exportedPath)

// 看板
const htmlRes = { statusCode: 0, headers: {}, body: '', writeHead(c, h) { this.statusCode = c; this.headers = h }, end(c) { this.body = String(c || '') } }
routes.get('/dsh-profiler').handler({ method: 'GET', url: '/dsh-profiler' }, htmlRes)
assert.equal(htmlRes.statusCode, 200)
assert.ok(htmlRes.body.includes('DSH <b>PROFILER</b>'))
assert.ok(htmlRes.body.includes('seriesChart') && htmlRes.body.includes('flame') && htmlRes.body.includes('donut'), 'dashboard must include SVG charts')

disposers.forEach((d) => d())

// 清理夹具与测试产生的回档条目，避免污染真实历史库
try {
  const logFile = path.join(FIXTURE_HOME, 'dsh-profiler', 'rewind', 'history.jsonl')
  if (fs.existsSync(logFile)) {
    const keep = fs.readFileSync(logFile, 'utf8').split('\n').filter((l) => l && !l.includes('dsh-profiler-rewind-'))
    fs.writeFileSync(logFile, keep.length ? keep.join('\n') + '\n' : '', 'utf8')
  }
} catch { /* noop */ }
try { fs.rmSync(FIXTURE_HOME, { recursive: true, force: true }) } catch { /* noop */ }

console.log('✓ dsh-profiler smoke test passed')
console.log('  tool   : profiler_report')
console.log('  command: /profiler (+rewind/restore)')
console.log('  routes : /dsh-profiler, report.json, rewind.json, rewind/restore')
console.log('  sample : read_file 120ms, p95', toolResult.data.tools.p95Ms + 'ms')
console.log('  rewind : snapshot→modify→restore verified (old file + new file)')
console.log('  scans  : sessions=' + report.sessions.count + ', bundles=' + report.plugins.totalBundles)
