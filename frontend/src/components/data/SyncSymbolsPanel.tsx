import { useMemo, useState } from 'react'
import { useMutation, useQueryClient } from '@tanstack/react-query'
import { Loader2 } from 'lucide-react'
import { api } from '@/lib/api'
import { QK } from '@/lib/queryKeys'
import { MissingCapChip } from '@/lib/capability-labels'

/** 把用户输入拆成候选代码: 逗号/空格/分号/换行分隔 */
function parseInput(text: string): string[] {
  return text
    .split(/[,，;；\s]+/)
    .map(s => s.trim())
    .filter(Boolean)
}

export function SyncSymbolsPanel({ caps, isRunning, onStart }: {
  caps: { label: string; capabilities: Record<string, { rpm: number | null; batch: number | null; subscribe: number | null }> } | undefined
  isRunning: boolean
  onStart: () => void
}) {
  const qc = useQueryClient()
  const hasBatchCap = !!caps?.capabilities?.['kline.daily.batch']

  const [text, setText] = useState('')
  const codes = useMemo(() => parseInput(text), [text])

  const sync = useMutation({
    mutationFn: () => api.syncSymbols(codes),
    onSuccess: () => {
      onStart()
      qc.invalidateQueries({ queryKey: QK.pipelineJobs })
    },
  })

  const canSubmit = hasBatchCap && !isRunning && !sync.isPending && codes.length > 0 && codes.length <= 100

  return (
    <div className="space-y-3">
      <div className="rounded-card bg-accent/8 border border-accent/20 p-3 space-y-1.5">
        <div className="text-xs text-foreground">
          只同步指定的几只股票,几十秒即完成(全市场管道约需几十分钟)。
        </div>
        <div className="text-[10px] text-muted leading-relaxed">
          拉取这些个股近 30 天日K · 除权因子并重算其指标。适合收盘后快速刷新关注标的;输入 6 位代码即可,自动补交易所后缀。
        </div>
      </div>

      <textarea
        value={text}
        onChange={e => setText(e.target.value)}
        placeholder="例如: 600519 000001 300750 (逗号或空格分隔,最多 100 只)"
        rows={3}
        className="w-full rounded-btn border border-border bg-base px-3 py-2 font-mono text-xs text-foreground placeholder:text-muted focus:border-accent/60 focus:outline-none resize-none"
      />

      <div className="flex items-center justify-between text-[10px] text-muted -mt-1">
        <span>
          已识别 <span className="font-mono text-secondary">{codes.length}</span> 个代码
        </span>
        {codes.length > 100 && <span className="text-warning/90">超过 100 只,请减少或使用「立即同步」</span>}
      </div>

      <button
        onClick={() => sync.mutate()}
        disabled={!canSubmit}
        className="w-full inline-flex items-center justify-center gap-1.5 px-3 py-1.5 rounded-btn bg-accent/90 text-base text-xs font-medium hover:bg-accent disabled:opacity-40 disabled:pointer-events-none transition-colors duration-150"
      >
        {sync.isPending ? (
          <>
            <Loader2 className="h-3 w-3 animate-spin" />
            请求中…
          </>
        ) : (
          <>开始同步</>
        )}
      </button>

      {sync.isError && (
        <div className="rounded-btn border border-danger/40 bg-danger/5 px-3 py-2 text-xs text-danger">
          {String((sync.error as any).message)}
        </div>
      )}

      {!hasBatchCap && (
        <span className="block text-center">
          <MissingCapChip capKey="kline.daily.batch" />
        </span>
      )}
    </div>
  )
}
