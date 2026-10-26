# fork 改动清单（fork-patches）

本仓库是 shy3130/tick-stock-panel 的 fork，同时持续拉取上游更新。本文登记**所有 fork 与上游的差异**，
用于：

1. 每次 `git fetch upstream` 后逐条核对，确认合并没有丢语义（历史上出现过合并把重试逻辑丢掉的事故）；
2. 新同学/新 AI 会话快速理解「哪些是我们自己加的」；
3. 判断哪些改动应该升级为正式扩展点（见 `docs/secondary-development.md` 5.2）。

> 维护方式：`git diff --name-status upstream/main main` 应与本清单一一对应；
> 改动增删时同步更新本文件，并在「同步核对动作」列写明验证方法。

## 一、核心补丁（会与上游同区域改动冲突 —— 重点核对）

| 文件 | 改动要点 | 为什么（fork 需求） | 冲突风险 | 对应测试 / 核对动作 |
| --- | --- | --- | --- | --- |
| `backend/app/api/pipeline.py` | 任务执行改用上游 `_submit_job(make_run)` 的闭包 `run()`；刷新放在 `quote_service.paused()` 上下文内，并走 `refresh_repo_cache_safely`（异常不改变 job 终态） | 上游原实现 `repo.refresh_cache()` 在 paused 之外且无异常保护，实时行情写入会 bump generation 使刷新半途放弃（表现为"同步完必须重启才生效"） | **高**：上游重构过该函数（曾因混用签名/函数体导致 `capset` NameError） | `tests/test_sync_job_refresh_cache.py`；核对 `run()` 与 `ctx = qs.paused() if qs else nullcontext()` 仍在 |
| `backend/app/api/kline.py` | `/sync`、`/sync_batch` 透传 `zero_row_out` 返回 `zero_row_symbols`（fail-loud），并在有写入时 `invalidate_storage_cache()`；导入 `refresh_repo_cache_safely` | 裸符号/上游空数据不再"显示成功但 0 行"；同步后前端列表立即失效刷新 | **高**（改 import 块 + 端点体） | `tests/test_sync_job_refresh_cache.py::test_sync_symbol_invalidates_storage_cache`；核对两处 `zero_row_out=zero` 与 `refresh_repo_cache_safely` 导入 |
| `backend/app/jobs/daily_pipeline.py` | 新增 `run_symbols_sync()`（按股票同步：只拉指定标的近期日K/除权并局部重算 enriched）+ `_refresh_single_view` 等 | 收盘后只刷新少数关注标的，避免全市场管道 | **高**：上游在同一位置新增 `run_adj_factor_sync()`（两者必须共存） | `tests/test_symbol_sync.py`；核对 `def run_symbols_sync` 与 `def run_adj_factor_sync` 同时存在 |
| `backend/app/services/kline_sync.py` | 除权因子用 `_merge_factor_window()`：受影响标的在 `[start,end]` 窗口内**先删后插**（替代纯 upsert） | 2026-09 中天科技 0813 事故：伪造的"每日累计因子"行永久残留，污染 `cum_prod` 把历史前复权价压成 ~0 | **高**：上游用 `_merge_adj_factor_store`（不删窗口旧行） | `tests/test_adj_factor_merge_and_defense.py`；核对 TickFlow 分支调用 `_merge_factor_window` |
| `backend/app/indicators/pipeline.py` | 非稀疏防御：按"相邻因子行间隔 ≤3 天"识别伪造密集序列，跳过该 symbol 复权（保 raw 原价） | 同上 0813 事故的读侧兜底 | 中（上游也在改 enriched 计算） | `tests/test_adj_factor_merge_and_defense.py`；核对 `dense` / `_gap` 逻辑仍在 |
| `backend/app/enriched_generation.py` | `_replace()` 内用 `_replace_with_retry()` 替换 `os.replace`（Windows 目标被读者占用时重试） | Windows 上实时行情/前端读 parquet 时 rename 偶发 `PermissionError`，直接失败会让整轮发布中断 | **高**：上游把该段重构为 `_locked_claim(_replace)`，已合并为「上游结构 + fork 重试」 | `tests/test_enriched_generation_replace_retry.py`；核对 `_replace()` 内是 `_replace_with_retry` 而非裸 `os.replace` |
| `backend/app/api/data.py` | 新增 `refresh_repo_cache_safely(repo)`：`repo.refresh_cache()` 包 try/except，失败只记日志 | job 终态不能被"刷新失败"带崩；被 pipeline/kline 端点复用 | 低 | 与 pipeline/kline 测试联动 |
| `backend/app/data_providers/base.py` | 新增 `UpstreamThrottledError` | provider 连续多批空返回时主动中止本轮，区别于"无新增" | 低 | `tests/test_stocksdk_provider.py` 等；核对异常类仍被 provider 抛出 |
| `frontend/src/lib/api.ts` | `pipelineJob()` 轮询 404 时合成 `failed` 终态（`任务不存在(后端可能已重启…)`） | running 任务只存内存，服务重启后轮询 404 会让各轮询方每秒刷 toast；合成终态后自然停止 | **高**：`api.ts` 是上游最活跃文件之一 | 前端 tsc + 手动验证"重启后端后轮询不再刷屏"；核对 `pipelineJob` 内 `/job not found/` 分支 |
| `frontend/src/components/stock-analysis/AnalysisKChart.tsx` | 关键价位图增强（默认档位含 `ma` 等） | 个股分析(Beta) 的价位展示 | 中 | tsc；`frontend/src/custom/stock-analysis/extension.tsx` 的 `defaultLevelTypes` |
| `frontend/src/pages/Dashboard.tsx` | 数据同步卡片显示真实 `fetchError` 文案（不再统一"同步失败,请重试"） | 失败原因可见 | 中 | tsc + 页面手测 |
| `frontend/src/pages/settings/DataSources.tsx` | 切换 financial provider 后 `invalidateQueries(FINANCIAL_QK.status)` | 否则财务分析页保持"能力不可用"直到刷新过期 | 低 | tsc + 切换数据源手测 |
| `frontend/index.html` | 首屏前应用 `tf-page-zoom`（localStorage），避免加载后缩放跳变 | 界面缩放设置体验 | 低 | 手测 |
| `frontend/src/custom/stock-analysis/extension.tsx` | `defaultLevelTypes` 增加 `'ma'`（该文件由上游把旧页 `pages/StockAnalysis.tsx` 重命名而来） | 保留 fork 的默认价位档 | 中 | tsc；注意该文件是"上游文件 + 1 行本地改动" |

## 二、扩展 / 插件（隔离区 —— 上游不会改这些路径，几乎零冲突）

| 路径 | 内容 | 说明 |
| --- | --- | --- |
| `backend/app/plugins/amber/` | Amber 数据源插件（client/provider/plugin.yaml，约 640 行） | fork 新增数据源 |
| `backend/app/plugins/stocksdk/` | TickFlow 官方 SDK 桥（`bridge.mjs` + `bridge.py` + provider，约 870 行） | 在"上游简版 provider"上重套管并行桥/限流熔断/脏值守卫 |
| `backend/app/plugins/zzshare/` | zzshare 免费财务数据插件（provider + requirements） | 财务指标/三大报表 |
| `frontend/src/components/data/SyncSymbolsPanel.tsx` | 「按股票同步」面板（调 `/api/kline/sync`） | 配合 `run_symbols_sync` |
| `frontend/src/custom/assistant/ui/AiConfigEntry.tsx` | AI 助手配置入口 | fork 的助手 UI 扩展 |
| `backend/scripts/rebuild_adj_factor.py` | 除权因子重建工具（窗口化重算） | 事故后修复工具 |
| `scripts/sync-upstream.ps1` | 上游同步脚本（重写检测 + 同步点记录） | 见 `docs/fork-workflow.md` |
| `start.bat` | Windows 启动脚本 | fork 打包/本地运行 |

## 三、fork 独有测试（与上面的补丁一一对应，不要删）

| 测试文件 | 覆盖 |
| --- | --- |
| `backend/tests/test_symbol_sync.py` | `run_symbols_sync` + 代码规范化 |
| `backend/tests/test_sync_job_refresh_cache.py` | 任务终态刷新（extend/repair/rebuild/sync_symbol 四类端点） |
| `backend/tests/test_enriched_generation_replace_retry.py` | 目标被占用时 rename 重试 |
| `backend/tests/test_adj_factor_merge_and_defense.py` | 除权窗口先删后插 + 非稀疏防御 |
| `backend/tests/test_stocksdk_provider.py` | stocksdk 插件桥接与实时规范化 |
| `backend/tests/test_amber_provider.py` | amber 插件 |
| `backend/tests/test_zzshare_financial_plugin.py` | zzshare 插件 |
| `backend/tests/test_packaging_spec.py` | 打包 spec 双声明契约（fork 侧只做了行尾空白清理） |

## 四、同步后的核对口令

```powershell
# 1) 差异集必须与本清单一致（新增/消失都要解释）
git diff --name-status upstream/main main

# 2) fork 关键锚点仍在（一次全查）
git grep -c "refresh_repo_cache_safely" -- backend/app/api/pipeline.py backend/app/api/kline.py
git grep -c "_merge_factor_window" -- backend/app/services/kline_sync.py
git grep -c "_replace_with_retry" -- backend/app/enriched_generation.py
git grep -c "def run_symbols_sync" -- backend/app/jobs/daily_pipeline.py
git grep -c "zero_row_symbols" -- backend/app/api/kline.py
git grep -c "任务不存在" -- frontend/src/lib/api.ts

# 3) 未定义名静态检查（专抓合并混用签名/函数体这类事故）
cd backend; .\.venv\Scripts\ruff.exe check --select F821 --no-cache app tests

# 4) 验证矩阵
cd backend; .\.venv\Scripts\python.exe -m pytest tests -q
cd frontend; pnpm exec tsc --noEmit
```
