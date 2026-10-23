/**
 * dsh-spark edge test — 降级与脏数据场景（第 2 轮健壮性优化）。
 * 运行：node test/edge.mjs
 */
import assert from 'node:assert/strict'
import * as plugin from '../lib/index.js'

function makeCtx({ webServer = null } = {}) {
  const tools = new Map()
  const commands = new Map()
  const handlers = []
  return {
    tools, commands, handlers,
    ctx: {
      logger: { info() {}, warn() {}, error() {} },
      tools: { register(d) { tools.set(d.name, d); return () => {} } },
      commands: { register(d) { commands.set(d.name, d); return () => {} } },
      on(_e, h) { handlers.push(h); return () => {} },
      effect(fn) { const d = fn(); if (typeof d === 'function') d(); return () => {} },
      get(s) { return s === 'webServer' ? webServer : undefined }
    }
  }
}

// 1) 无 webServer：插件仍应加载，不抛错
{
  const { ctx, tools, commands } = makeCtx()
  assert.doesNotThrow(() => plugin.apply(ctx, {}))
  assert.ok(tools.has('spark_report'))
  assert.ok(commands.has('spark'))
}

// 2) enabled=false：完全不注册
{
  const { ctx, tools, commands } = makeCtx()
  plugin.apply(ctx, { enabled: false })
  assert.equal(tools.size, 0)
  assert.equal(commands.size, 0)
}

// 3) 脏事件：缺 callId、无前置 call 的 result、未知类型都不应抛错
{
  const { ctx, handlers, tools } = makeCtx()
  plugin.apply(ctx, {})
  const emit = (event) => handlers.forEach((h) => h({ id: 's' }, event))
  assert.doesNotThrow(() => {
    emit({ type: 'tool/result', data: { message: { source: { callId: 'ghost' } } } })
    emit({ type: 'tool/call', data: {} })
    emit({ type: 'unknown/event', data: null })
    emit(null)
  })
  const out = await tools.get('spark_report').execute({}, {})
  assert.equal(out.data.tools.sampled, 0)
}

// 4) 配置夹取：非法数值回落到默认值，不产生 NaN
{
  const { ctx, tools } = makeCtx()
  plugin.apply(ctx, { sampleLimit: -5, slowToolMs: 'abc', sessionScanTtlMs: 1e12 })
  const out = await tools.get('spark_report').execute({ scope: 'tools' }, {})
  assert.ok(Number.isFinite(out.data.tools.p95Ms))
  assert.ok(Number.isFinite(out.data.tools.avgMs))
}

console.log('✓ dsh-spark edge test passed (4 scenarios)')
