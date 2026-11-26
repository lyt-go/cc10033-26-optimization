<template>
  <article
    class="receipt-card"
    :class="{ selected, highlight, confirmed: r.status_code === 'confirmed', conflict: r.status_code === 'conflict' }"
    :id="`receipt-card-${rowId}-${rid}`"
    :data-testid="`receipt-${rowId}-${rid}`"
    :data-receipt-id="rid"
    :data-kind="kindCode"
    @click="$emit('select', rid)"
  >
    <div class="receipt-top">
      <strong :data-testid="`receipt-no-${rowId}-${rid}`">{{ r.receipt_no || `#${rid}` }}</strong>
      <span class="kind-tag" :data-testid="`receipt-kind-${rowId}-${rid}`">{{ r.prediction_label || kindText }}</span>
    </div>
    <p class="receipt-line">
      <span>状态：</span>
      <em :data-testid="`receipt-status-${rowId}-${rid}`">{{ r.status }}（{{ r.status_code }}）</em>
      <span class="ver">v{{ r.version }}</span>
    </p>
    <p class="receipt-line"><span>容量动作：</span>{{ r.action }}（{{ r.from_instances }}→{{ r.target_instances }}，上下限 {{ r.min_instances }}~{{ r.max_instances }}）</p>
    <p class="receipt-line" :data-testid="`receipt-basis-${rowId}-${rid}`">
      <span>采用依据：</span><code>{{ r.basis_code || r.basis }}</code>
      <template v-if="r.predicted_load != null">；预测负载 {{ r.predicted_load }}</template>
    </p>
    <p class="receipt-line reason" :data-testid="`receipt-reason-${rowId}-${rid}`"><span>理由：</span>{{ r.reason }}</p>
    <p class="receipt-line scope" :data-testid="`receipt-scope-${rowId}-${rid}`"><span>适用范围：</span>{{ r.scope_text || r.scope }}</p>
    <p v-if="r.conflict_reason" class="receipt-conflict">{{ r.conflict_reason }}</p>

    <div v-if="r.status_code !== 'confirmed'" class="confirm-row" @click.stop>
      <input
        :value="draft"
        type="number"
        :min="r.min_instances"
        :max="r.max_instances"
        :data-testid="`actual-${rowId}-${rid}`"
        :aria-label="`实际实例数-${rid}`"
        placeholder="实际实例数"
        @input="$emit('draft', { rid, value: ($event.target as HTMLInputElement).value })"
      />
      <button
        type="button"
        class="btn primary"
        :data-testid="`confirm-receipt-${rowId}-${rid}`"
        @click="$emit('confirm', { rid, actual: draft })"
      >确认实际容量变更</button>
    </div>
    <p v-else class="confirmed-line">
      已确认实际容量 <strong>{{ r.actual_instances }}</strong> 实例（{{ r.confirmed_at }}）
    </p>
  </article>
</template>

<script setup lang="ts">
import { computed } from 'vue'

const props = defineProps<{
  r: Record<string, any>
  rowId: number
  draft: string
  selected: boolean
  highlight: boolean
}>()
defineEmits<{
  (e: 'select', rid: number): void
  (e: 'draft', payload: { rid: number; value: string }): void
  (e: 'confirm', payload: { rid: number; actual: string }): void
}>()

const rid = computed(() => Number(props.r.receipt_id))
const kindCode = computed(() => String(props.r.kind || props.r.prediction_kind || 'missing'))
const kindText = computed(() => ({
  missing: '没有预测', zero: '预测为零', failed: '预测失败', ok: '预测有效',
}[kindCode.value] || kindCode.value))
</script>

<style scoped>
.receipt-card { border: 1px solid var(--border); border-radius: 8px; padding: 8px 10px; margin: 8px 0; background: #fcfdff; cursor: pointer; transition: box-shadow .15s; }
.receipt-card.selected { border-color: var(--brand); box-shadow: 0 0 0 2px rgb(31 111 235 / 15%); }
.receipt-card.highlight { border-color: #f79009; box-shadow: 0 0 0 2px rgb(247 144 9 / 25%); }
.receipt-card.confirmed { background: #f6fef9; }
.receipt-card.conflict { border-color: #e5484d; background: #fff8f8; }
.receipt-top { display: flex; justify-content: space-between; align-items: center; gap: 8px; }
.kind-tag { font-size: 12px; background: #eef2ff; border-radius: 10px; padding: 1px 8px; white-space: nowrap; }
.receipt-line { font-size: 12px; margin: 4px 0; line-height: 1.5; }
.receipt-line span { color: var(--muted); }
.receipt-line .ver { float: right; color: #94a3b8; }
.reason, .scope { color: #334155; }
.receipt-conflict { font-size: 12px; color: #c81e1e; margin: 4px 0; }
.confirm-row { display: flex; gap: 8px; margin-top: 6px; }
.confirm-row input { width: 110px; padding: 5px 8px; border: 1px solid var(--border); border-radius: 6px; }
.confirmed-line { font-size: 12px; color: #067647; margin: 6px 0 0; }
code { background: #f1f5f9; padding: 0 4px; border-radius: 4px; }
</style>
