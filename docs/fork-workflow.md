# fork 开发与上游同步工作流

本仓库 = 上游（`upstream` → shy3130/tick-stock-panel）+ 自己的功能开发（`origin` → 你的 fork）。
上游**多次强推重写历史**，且双方都会改同一批热点文件，因此需要一套固定流程。

配套文档：

- `docs/secondary-development.md` —— 二次开发分级（L1/L2/L3）、扩展插槽契约
- `docs/fork-patches.md` —— 本 fork 与上游的全部差异清单（每次同步后逐条核对）

## 1. 总原则：新功能优先做成扩展，而不是改核心源码

| 需求 | 落点 | 同步冲突 |
| --- | --- | --- |
| 新页面/新面板/局部 UI 增强 | `frontend/src/custom/<feature>/extension.tsx`（从 `custom/_template` 复制）+ 路由/导航注册 | 无（上游不碰 `custom/*`） |
| 新数据源 / 新能力 | `backend/app/plugins/<name>/`（`plugin.yaml` + provider，参考 `zzshare`/`tdxquant`） | 无 |
| 可替换业务规则 | 小粒度继承 / 策略接口（`docs/secondary-development.md` 4.1/4.2） | 低 |
| 核心流程本身必须变 | 直接改源码（L3），改动最小 + 补回归测试 + 登记到 `docs/fork-patches.md` | 高，需重点核对 |

判断标准：如果一个需求能用扩展点完成，就不要复制整页/整服务 —— 复制会把一次显式冲突变成长期的隐性分叉。

## 2. 分支模型：`main` 只做集成线

```
main            只放「上游同步 + 合并提交」，永远可运行、可推送
├─ feat/<功能>   新功能分支，从最新 main 切出，开发完整后合回 main
└─ backup/*      同步前的兜底快照（重要同步前手动建）
```

固定动作：

```powershell
# 开始新功能（feat/example 已作为空示例分支切好）
git switch main; git pull --ff-only origin main
git switch -c feat/my-feature

# 开发完成 → 合回 main（用 --no-ff 保留功能边界）
git switch main
git merge --no-ff feat/my-feature

# 推送
git push origin main
```

规则：

- **不在 main 上直接开发**。main 保持干净，同步上游时才能用「工作区必须清洁」的脚本守卫。
- 功能分支落后时：未推送的用 `git rebase main`，已推送/多人协作的用 `git merge main`。
- 上游同步后先合 main，再让功能分支 rebase/merge main，避免功能分支长期落后。

## 3. 同步上游：小步高频

```powershell
# 推荐：每周一次，或上游发版 tag 后立即同步
pwsh scripts\sync-upstream.ps1            # fetch + 增量 merge（冲突时停下等你解决）
pwsh scripts\sync-upstream.ps1 -Push      # 合并成功后顺便推 origin
```

脚本做了四件事：

1. 工作区不干净 / 不在 main 上 → 拒绝执行；
2. `fetch --prune`，schannel TLS 失败自动降级 openssl（仓库已配置 `http.sslBackend=openssl`）；
3. **历史重写检测**：上次同步点记在 `refs/upstream-sync/last`，若它已不是 `upstream/main`
   的祖先，说明上游 rebase/强推了 → 立即中止并打印恢复步骤（见第 4 节）；
4. 合并成功后更新同步点。

已启用 `rerere`（`git config rerere.enabled true`）：上游反复改写历史时，同样的冲突会重复出现，
rerere 会自动复用上次的解决结果，冲突只需真正决策一次。

## 4. 冲突处理 playbook

### 4.1 常规冲突（有共同祖先）

按文件看 `<<<<<<< HEAD` / `>>>>>>> upstream/main`，逐块决策：

- 上游重构 + 本地语义补丁 → **保上游结构，补回本地语义**（如 `_locked_claim(_replace)` 里保留
  `_replace_with_retry`）；
- 两边各加一个独立函数/端点 → **两个都留**（如 `run_symbols_sync` + `run_adj_factor_sync`）；
- 上游改进是本地需求的超集 → 取上游；
- 本地是上游缺陷的修复 → 保留本地（如除权窗口先删后插）。

### 4.2 上游又重写历史了（冲突爆炸）

症状：`git fetch` 输出 `+ xxx...yyy main -> upstream/main (forced update)`，脚本报"上游重写了历史"。

恢复步骤（已在两次实践中验证）：

```powershell
# 记下上次同步点（脚本提示或 refs/upstream-sync/last）
$base = git rev-parse refs/upstream-sync/last

git merge --abort        # 丢弃这批假冲突
git merge --no-commit upstream/main   # 重新进入冲突（此时冲突是"假"的）

# 对每个未合并文件，用 $base 做三方合并（字节级，不动编码/换行）
foreach ($f in git diff --name-only --diff-filter=U) {
    git show "HEAD:$f"          > $env:TEMP\o
    git show "${base}:$f"       > $env:TEMP\b    # 若 base 无此文件 → 两边新增
    git show "upstream/main:$f" > $env:TEMP\t
    git merge-file $env:TEMP\o $env:TEMP\b $env:TEMP\t
    if ($LASTEXITCODE -eq 0) { Copy-Item $env:TEMP\o $f -Force; git add $f }  # 自动解决
    else { "$f 需要人工" }
}
```

剩下的人工项只有三类：两边真实冲突（逐个决策）、上游删除且本地未改（接受删除）、
二进制双方都改（取上游）。经验值：160 个假冲突 → 只有 4 个需要人工。

### 4.3 合并后必须做的核对

```powershell
git diff --cached --check                    # 冲突标记/空白
# 未定义名（专抓"上游签名 + 本地函数体"混用，曾导致 capset NameError）
cd backend; .\.venv\Scripts\ruff.exe check --select F821 --no-cache app tests
# 与 fork-patches.md 逐条对照
git diff --name-status upstream/main main
# 验证矩阵
cd backend; .\.venv\Scripts\python.exe -m pytest tests -q
cd frontend; pnpm exec tsc --noEmit
```

## 5. 环境事实（避免重复踩坑）

- 后端：`uv sync --extra dev`（与上游 CI 一致；`--extra backtest` 会装 vectorbt，其顶层 `tests/`
  包会遮蔽 `backend/tests`，导致 `from tests.signal_seeds import ...` 报 ModuleNotFoundError）。
- 前端：`frontend/` 下 `pnpm install` 后 `pnpm exec tsc --noEmit`。
- Git：`core.autocrlf=true`（工作区 CRLF、仓库 LF）。批量改文件时若用 PowerShell 文本管道，
  中文可能被按 ANSI 解码破坏 —— 优先用 `[System.IO.File]::WriteAllBytes/ReadAllBytes` 或 Python 读写。
- `.ps1` 脚本必须带 UTF-8 BOM，否则 Windows PowerShell 按 GBK 解析中文注释会报语法错。
- `git ls-tree`/`git diff --name-only` 默认把非 ASCII 路径转义成八进制，脚本里用它判断文件是否存在
  会误判 —— 用 `git cat-file -e -- "<真实路径>"` 或 `-c core.quotepath=false`。

## 6. 提交规范

- 每个提交只做一类事；核心补丁 + 对应测试放在同一提交。
- 提交信息写清「为什么」，同步相关的提交标注上游版本（如 `至 463729ea`）。
- 合并提交保留双亲结构（不要 squash 上游历史）。
