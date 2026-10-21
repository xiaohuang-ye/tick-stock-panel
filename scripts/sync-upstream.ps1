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

# --- 3. 历史重写检测: 上次同步的上游 tip 是否仍是 upstream/main 的祖先 ---
# 上游一旦 rebase/强推, 旧 tip 会从新历史中消失, 此时用 git 默认祖先(很旧)合并
# 会产生大量假冲突(曾出现 160 个冲突里只有 4 个是真的)。以记录的上次同步点
# (refs/upstream-sync/last)为三方基线才是正确恢复方式。
$lastRef = 'refs/upstream-sync/last'
git rev-parse --verify --quiet $lastRef | Out-Null
if ($LASTEXITCODE -eq 0) {
    git merge-base --is-ancestor $lastRef upstream/main
    if ($LASTEXITCODE -ne 0) {
        $last = (git rev-parse --short $lastRef)
        Write-Host "[sync] 警告: 上游重写了历史! 上次同步点 $last 已不在 upstream/main 历史中." -ForegroundColor Red
        Write-Host "[sync]   直接 merge 会产生大量假冲突。已验证的恢复步骤:" -ForegroundColor Yellow
        Write-Host "[sync]   1) git merge --no-commit upstream/main        # 或用 --abort 后重来" -ForegroundColor Yellow
        Write-Host "[sync]   2) 对每个未合并文件, 以 $last 为基线重做三方合并:" -ForegroundColor Yellow
        Write-Host "[sync]      git show HEAD:<f> > o; git show ${last}:<f> > b; git show upstream/main:<f> > t" -ForegroundColor Yellow
        Write-Host "[sync]      git merge-file o b t   # 0=自动解决, 1=真冲突需人工" -ForegroundColor Yellow
        Write-Host "[sync]   3) 仅上游新增(取 theirs)、上游删除且本地未改(接受删除)、二进制(取上游)再人工判断" -ForegroundColor Yellow
        exit 1
    }
} else {
    Write-Host "[sync] 首次运行: 未记录同步点, 本次成功后自动记录到 $lastRef" -ForegroundColor DarkGray
}

# --- 3b. 无共同祖先兜底(上游重建仓库) ---
git merge-base HEAD upstream/main | Out-Null
if ($LASTEXITCODE -ne 0) {
    Write-Host "[sync] 警告: 本地 main 与 upstream/main 无共同祖先 —— 上游重建了仓库!" -ForegroundColor Red
    Write-Host "[sync]   请按 README/历史经验用 --allow-unrelated-histories 加旧上游 tip 做三方合并。" -ForegroundColor Yellow
    exit 1
}

# --- 4. 常规增量合并 ---
git merge upstream/main --no-edit
if (-not $?) {
    Write-Host "[sync] 存在冲突, 请手工解决后 git commit 完成合并" -ForegroundColor Yellow
    exit 1
}
Write-Host "[sync] 合并完成: $(git log --oneline -1)" -ForegroundColor Green
# 记录本次同步点, 供下次检测上游是否又重写历史
git update-ref $lastRef upstream/main
Write-Host "[sync] 已记录同步点: $((git rev-parse --short upstream/main))" -ForegroundColor DarkGray

# --- 5. 可选推送 ---
if ($Push) {
    git push origin main
    if ($LASTEXITCODE -ne 0) {
        Write-Host "[sync] 推送失败" -ForegroundColor Red
        exit 1
    }
    Write-Host "[sync] 已推送到 origin/main" -ForegroundColor Green
}