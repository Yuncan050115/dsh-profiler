# dsh-spark

DeepSeek Harness 的运行时剖析器。对标 Minecraft 服务端的 **Spark**（性能剖析）与 **CoreProtect**（行为审计）——dsh 官方已有 compaction 与 session projection，但缺少「启动归因 / 工具耗时 / token 归因」这一层运维视角。

## 能力

| 能力 | 数据来源 | 输出 |
| --- | --- | --- |
| 启动归因 | `process.uptime()` + 插件挂载时刻 | 进程启动 → 插件挂载毫秒数 |
| 内存画像 | `process.memoryUsage()` | RSS / Heap 已用 / 总量 |
| 工具耗时 | `session/event` 的 `tool/call` → `tool/result` 配对 | 次数、avg、P95、max、失败数、最慢 5 条 |
| 会话归因 | `$DSH_HOME/storages/session_projcache` | 会话数、token 分布、缓存命中率、Top 会话 |
| 插件清单 | `$DSH_HOME/profiles/*/package.json` | bundle 数、第三方数、是否含 memory 插件 |
| 诊断 | 阈值规则 | `findings[]`：启动慢、内存高、工具慢、跨会话记忆缺口… |

## 安装

```bash
dsh plugin --profile desktop add link:<本目录>
```

## 使用

- 命令：`/spark`、`/spark tools`、`/spark sessions`、`/spark plugins`、`/spark json`、`/spark export`
- 工具：`spark_report`（Agent 可自行调用，支持 `scope` 切片）
- HTTP：`/dsh-spark`（看板）、`/dsh-spark/report.json`（原始数据）
- 导出：`/spark export` 写入 `$DSH_HOME/dsh-spark/report-*.json`

## 实测基线（2026-10-23，Windows / 0.2.1-rc.2）

- 冷启动到主窗口：**11,738 ms**；热启动：**4,391 ms**；第三次：**7,390 ms**
- 插件树挂载：**1,474–1,658 ms**
- 6 个进程，RSS 合计约 **2,021 MB**
- 37 个会话，累计 **33.0M tokens**，缓存命中 **97.5%**
- 最大单会话 **26.9M tokens**，无任何跨会话记忆插件

## 测试

```bash
node test/smoke.mjs   # 契约：tool / command / 路由 / 采样 / 导出
node test/edge.mjs    # 降级：无 webServer、禁用、脏事件、非法配置
```

## 设计约束

宿主半侧只使用 Node 内置模块，不 import `@deepseek-ai/*`。profile 的 `node_modules` 里没有官方包软链，第三方插件若直接 import 官方包会在加载期失败。
