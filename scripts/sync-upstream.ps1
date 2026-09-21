<#
.SYNOPSIS
    把上游 shy3130（upstream remote）的 main 增量同步合并到本地 main。

.DESCRIPTION
    fork 常规同步 = fetch upstream + merge upstream/main。有了共同祖先之后
    （本仓库 ac75623 已将上游 49a2bed 挂入历史）这只是增量合并，几秒完成。

    脚本自带防护：
    - 工作区有未提交改动时拒绝执行（避免把本地工作卷进合并）；
    - fetch 遇到 schannel TLS 凭据错误时自动降级 openssl 后端重试；
    - 检测不到共同祖先时（上游又整体重写历史）立即报警并中止，
      不产生上百个 add/add 冲突；
    - upstream/main 已包含在本地时直接跳过（幂等）。

.PARAMETER Push
    合并成功后把 main 推送到 origin（你的 fork 远程）。

.EXAMPLE
    pwsh scripts/sync-upstream.ps1            # 只同步合并到本地
    pwsh scripts/sync-upstream.ps1 -Push      # 合并后推送 origin/main
#>
param(
    [switch]$Push
)

$ErrorActionPreference = 'Stop'

# --- 0. 前置检查 ---
git diff --quiet
$cleanWork = $LASTEXITCODE -eq 0
git diff --cached --quiet
$cleanIndex = $LASTEXITCODE -eq 0
if (-not ($cleanWork -and $cleanIndex)) {
    Write-Host "[sync] 工作区有未提交改动, 请先提交或 stash 后再同步" -ForegroundColor Yellow
    exit 1
}
$branch = git rev-parse --abbrev-ref HEAD
if ($branch -ne 'main') {
    Write-Host "[sync] 当前分支是 $branch, 请在 main 上执行" -ForegroundColor Yellow
    exit 1
}

# --- 1. fetch upstream (schannel 失败时自动降级 openssl) ---
git fetch upstream --prune
if (-not $?) {
    Write-Host "[sync] 常规 fetch 失败, 尝试 openssl TLS 后端..." -ForegroundColor Yellow
    git -c http.sslBackend=openssl fetch upstream --prune
    if (-not $?) {
        Write-Host "[sync] fetch 失败, 请检查网络 / 代理 (127.0.0.1:7897)" -ForegroundColor Red
        exit 1
    }
}

# --- 2. 快路径: upstream/main 已包含在本地历史 ---
git merge-base --is-ancestor upstream/main HEAD
if ($LASTEXITCODE -eq 0) {
    Write-Host "[sync] upstream/main 已包含在本地历史中, 无需合并" -ForegroundColor Green
    exit 0
}

# --- 3. 无共同祖先防护: 上游重写历史, 不能用常规 merge ---
git merge-base HEAD upstream/main | Out-Null
if ($LASTEXITCODE -ne 0) {
    Write-Host "[sync] 警告: 本地 main 与 upstream/main 无共同祖先 —— 上游重写了历史!" -ForegroundColor Red
    Write-Host "[sync]   直接 merge 会产生上百个 add/add 冲突, 请参考历史经验:" -ForegroundColor Yellow
    Write-Host "[sync]   1) git log --oneline --all | 找到上次合并过的上游 tip 作为三方基线" -ForegroundColor Yellow
    Write-Host "[sync]   2) git merge --allow-unrelated-histories upstream/main 后用该基线逐文件 merge-file" -ForegroundColor Yellow
    exit 1
}

# --- 4. 常规增量合并 ---
git merge upstream/main --no-edit
if (-not $?) {
    Write-Host "[sync] 存在冲突, 请手工解决后 git commit 完成合并" -ForegroundColor Yellow
    exit 1
}
Write-Host "[sync] 合并完成: $(git log --oneline -1)" -ForegroundColor Green

# --- 5. 可选推送 ---
if ($Push) {
    git push origin main
    if ($LASTEXITCODE -ne 0) {
        Write-Host "[sync] 推送失败" -ForegroundColor Red
        exit 1
    }
    Write-Host "[sync] 已推送到 origin/main" -ForegroundColor Green
}