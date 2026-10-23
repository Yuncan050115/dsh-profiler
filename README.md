# dsh-profiler

DeepSeek Harness 的运行时剖析器 + 变更回档。对标 Minecraft 服务端的 **Spark**（性能剖析）与 **CoreProtect**（审计回档）——dsh 官方已有 compaction 与 session projection，但缺少「启动归因 / 工具耗时 / token 归因 / 变更回滚」这一层运维视角。

## 能力

| 能力 | 数据来源 | 输出 |
|---|---|---|
| 启动归因 | 进程 uptime 反推 + apply 计时 | 冷/热启动对照、插件挂载耗时、启动瀑布图 |
| 工具延迟 | session/event 的 tool/call ↔ tool/result 配对 | count / avg / p50 / p95 / p99 / 成功率 / 时间序列折线 / 火焰条 |
| token 归因 | 本地会话持久化（projcache） | 分类汇总、缓存命中率、Top 会话、环形图 |
| 变更回档 | 写文件类工具调用前的自动快照（append-only 历史库 + blob 去重） | 时间点列表、一键回档（可逆）、新文件回档即删除 |
| 生态扫描 | profiles 清单 | 第三方插件统计、跨会话记忆插件检测 |
| 诊断 | 阈值规则 | findings（启动慢 / P95 高 / 记忆缺口 / 缓存低…） |

## 安装

```bash
dsh plugin --profile desktop add npm:dsh-profiler
```

或手动：profile 的 `package.json` dependencies 加 `"dsh-profiler": "^2"`，bundles 数组加 `"dsh-profiler"`，`npm install` 后重启。

## 使用

- 命令：`/profiler`、`/profiler tools|sessions|plugins|json|export`
- 回档：`/profiler rewind` 列出变更点，`/profiler restore <id>` 恢复（回档前会自动存 restore-point，可反向）
- 工具：`profiler_report`（Agent 可自行调用，scope: summary/tools/sessions/plugins/rewind）
- HTTP：`/dsh-profiler`（看板：延迟时间序列 / 火焰条 / token 环形图 / 启动瀑布 / 回档时间线，5s 自刷新）、`/dsh-profiler/report.json`、`/dsh-profiler/rewind.json`、`POST /dsh-profiler/rewind/restore`

## 设计约束

profile 的 node_modules 下没有 `@deepseek-ai/*` 软链，因此本插件零外部依赖（仅 node:fs/os/path/crypto/url），工具/命令定义按官方源码契约手工构造。

## 测试

```bash
node test/smoke.mjs   # 契约 + 采样 + 回档全链路（快照→修改→恢复→新文件删除）
node test/edge.mjs    # 降级与脏数据场景
```

## 实测基线（2026-10-23，DSH 0.2.1-rc.2，Windows）

冷启动 11,738 ms / 热启动 4,391 ms / 插件挂载约 1.5 s / 6 进程 RSS 约 2 GB / 37 会话累计 3300 万 tokens / 缓存命中 97.5%。
