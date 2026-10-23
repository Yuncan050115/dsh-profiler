/**
 * dsh-spark smoke test — 不启动 DSH，用假 ctx 验证插件契约：
 *   register(tool) / register(command) / session/event 采样 / HTTP 路由 / 报告字段。
 * 运行：node test/smoke.mjs
 */
import assert from 'node:assert/strict'
import fs from 'node:fs'
import * as plugin from '../lib/index.js'

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

assert.equal(plugin.name, 'dsh-spark')
assert.ok(Array.isArray(plugin.inject) && plugin.inject.includes('tools'))
assert.ok(tools.has('spark_report'), 'spark_report tool must be registered')
assert.ok(commands.has('spark'), '/spark command must be registered')
assert.ok(routes.has('/dsh-spark/report.json'), 'report route must be registered')
assert.ok(routes.has('/dsh-spark'), 'dashboard route must be registered')

// 模拟一次工具调用：120ms，超阈值
const emit = (session, event) => (globalThis.__handlers['session/event'] || []).forEach((h) => h(session, event))
const session = { id: 'session-test' }
emit(session, { type: 'tool/call', time: 1000, data: { callId: 'c1', name: 'read_file' } })
emit(session, { type: 'tool/result', time: 1120, data: { message: { source: { callId: 'c1' }, isError: false } } })

const toolResult = await tools.get('spark_report').execute({ scope: 'summary', limit: 3 }, {})
assert.ok(toolResult.summary.includes('dsh-spark'))
assert.ok(toolResult.summary.includes('工具'))
assert.equal(toolResult.data.tools.sampled, 1)
assert.equal(toolResult.data.tools.perTool[0].tool, 'read_file')
assert.equal(toolResult.data.tools.perTool[0].avgMs, 120)
assert.ok(toolResult.data.findings.some((f) => f.code === 'tools/slow-p95'), 'slow p95 finding expected')

// 命令
const cmd = await commands.get('spark').handler({ rawInput: 'tools' })
assert.equal(cmd.kind, 'success')
assert.ok(cmd.text.includes('read_file'))

// HTTP 路由
const res = {
  statusCode: 0, headers: {}, body: '',
  writeHead(code, headers) { this.statusCode = code; this.headers = headers },
  end(chunk) { this.body = chunk ? String(chunk) : '' }
}
await routes.get('/dsh-spark/report.json').handler({ method: 'GET', url: '/dsh-spark/report.json' }, res)
assert.equal(res.statusCode, 200)
const report = JSON.parse(res.body)
assert.equal(report.version, '1.1.0')
assert.ok(report.process.rssMb > 0)
assert.ok(report.sessions.count >= 1, 'sessions should be scanned from ~/.dsh')
assert.equal(typeof report.sessions.cacheHitRate, 'number')
assert.ok(Array.isArray(report.plugins.profiles) && report.plugins.profiles.length >= 1)
assert.ok(Array.isArray(report.findings) && report.findings.length >= 1)

// 导出命令
const exported = await commands.get('spark').handler({ rawInput: 'export' })
assert.equal(exported.kind, 'success')
const exportedPath = exported.text.replace('已导出剖析报告：', '')
assert.ok(fs.existsSync(exportedPath), 'export file must exist')
const exportedDoc = JSON.parse(fs.readFileSync(exportedPath, 'utf8'))
assert.equal(exportedDoc.version, '1.1.0')
fs.unlinkSync(exportedPath)

// 看板
const htmlRes = { statusCode: 0, headers: {}, body: '', writeHead(c, h) { this.statusCode = c; this.headers = h }, end(c) { this.body = String(c || '') } }
routes.get('/dsh-spark').handler({ method: 'GET', url: '/dsh-spark' }, htmlRes)
assert.equal(htmlRes.statusCode, 200)
assert.ok(htmlRes.body.includes('DSH <b>SPARK</b>'))

disposers.forEach((d) => d())

console.log('✓ dsh-spark smoke test passed')
console.log('  tool   : spark_report')
console.log('  command: /spark')
console.log('  routes : /dsh-spark, /dsh-spark/report.json')
console.log('  sample : read_file 120ms, p95', toolResult.data.tools.p95Ms + 'ms')
console.log('  scans  : sessions=' + report.sessions.count + ', bundles=' + report.plugins.totalBundles)
