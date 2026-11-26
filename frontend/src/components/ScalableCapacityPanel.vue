<template>
  <section
    class="cap-panel"
    :data-testid="`capacity-panel-${row.id}`"
    data-module="scalable_pool"
  >
    <header class="cap-head">
      <div>
        <h4>
          容量评估与确认
          <span class="cap-strategy">{{ row['策略编号'] }} · {{ row['Worker类型'] }}</span>
        </h4>
        <p class="cap-effective" :data-testid="`effective-capacity-${row.id}`">
          有效容量：<strong>{{ effectiveInstances ?? '—' }}</strong> 实例
          <template v-if="effectiveBasis?.receipt_id != null">
            （依据回执 #{{ effectiveBasis.receipt_id }} 已确认）
          </template>
          <template v-else>（初始/历史容量，尚无新确认回执）</template>
        </p>
      </div>
      <div class="cap-head-actions">
        <button
          type="button"
          class="btn"
          :data-testid="`action-${row.id}-查询新评估`"
          @click="queryNew"
        >查询新评估</button>
        <button
          type="button"
          class="btn ghost"
          :data-testid="`refresh-details-${row.id}`"
          @click="refreshAll"
        >刷新详情</button>
      </div>
    </header>

    <!-- 评估申请 -->
    <form class="cap-apply" :data-testid="`evaluation-form-${row.id}`" @submit.prevent="submitEvaluation">
      <label>
        <span>预测情况</span>
        <select v-model="predictionKind" :data-testid="`prediction-kind-${row.id}`">
          <option value="ok">有效预测（填写负载）</option>
          <option value="missing">没有预测（缺预测）</option>
          <option value="zero">预测为零（零预测）</option>
          <option value="failed">请求失败（预测失败）</option>
          <option value="timeout">请求超时（先超时）</option>
        </select>
      </label>
      <label v-if="predictionKind === 'ok'">
        <span>预测负载</span>
        <input
          v-model="loadValue"
          type="number"
          min="0"
          :data-testid="`predicted-load-${row.id}`"
          placeholder="predicted_load_next_hour"
        />
      </label>
      <label>
        <span>关联调度计划ID</span>
        <input v-model="planId" type="number" :data-testid="`plan-id-${row.id}`" placeholder="可选" />
      </label>
      <button
        type="submit"
        class="btn primary"
        :disabled="busy"
        :data-testid="`action-${row.id}-申请评估`"
      >申请评估 / 容量调整请求</button>
    </form>

    <!-- 绑定调度计划 -->
    <form class="cap-bind" @submit.prevent="bindPlan">
      <label>
        <span>同步到调度计划ID</span>
        <input v-model="bindPlanId" type="number" :data-testid="`bind-plan-input-${row.id}`" placeholder="plan id" />
      </label>
      <button type="submit" class="btn" :data-testid="`action-${row.id}-绑定调度计划`">
        绑定调度计划并同步有效容量
      </button>
    </form>

    <p v-if="banner" class="cap-banner ok" :data-testid="`banner-${row.id}`">{{ banner }}</p>
    <p v-if="errorMsg" class="cap-banner err" :data-testid="`error-${row.id}`">{{ errorMsg }}</p>
    <p v-if="newReceiptHint" class="cap-banner info" :data-testid="`new-receipt-hint-${row.id}`">
      已查询到新回执：
      <a href="javascript:void(0)" @click="highlight(newReceiptHint)">#{{ newReceiptHint }}</a>
    </p>

    <!-- 冲突原因 + 可查的新回执；旧选择保留 -->
    <div
      v-if="conflict"
      class="cap-conflict"
      role="alert"
      :data-testid="`conflict-banner-${row.id}`"
    >
      <strong>确认失败：容量依据冲突</strong>
      <p class="conflict-reason" :data-testid="`conflict-reason-${row.id}`">
        冲突原因：{{ conflict.conflict_reason || conflict.message }}
      </p>
      <button
        type="button"
        class="link"
        :data-testid="`goto-new-receipt-${row.id}`"
        @click="highlight(conflict.new_receipt_id)"
      >查看可查的新回执 #{{ conflict.new_receipt_id }}</button>
      <span class="preserve-hint">（原回执选择与填写的实际容量已保留）</span>
    </div>

    <!-- 评估详情：三态分开显示 -->
    <div class="cap-groups">
      <div class="cap-group missing" :data-testid="`section-missing-${row.id}`">
        <h5>缺预测 · 没有预测 <em>{{ groups.missing.length }}</em></h5>
        <p v-if="!groups.missing.length" class="empty-mini">暂无缺预测回执</p>
        <ReceiptCard
          v-for="r in groups.missing"
          :key="`m-${r.receipt_id}`"
          :r="r"
          :row-id="Number(row.id)"
          :draft="draftOf(r.receipt_id)"
          :selected="selectedId === r.receipt_id"
          :highlight="highlightId === r.receipt_id"
          @select="selectedId = r.receipt_id"
          @draft="setDraft"
          @confirm="confirmReceipt"
        />
      </div>

      <div class="cap-group zero" :data-testid="`section-zero-${row.id}`">
        <h5>零预测 · 预测为零 <em>{{ groups.zero.length }}</em></h5>
        <p v-if="!groups.zero.length" class="empty-mini">暂无零预测回执</p>
        <ReceiptCard
          v-for="r in groups.zero"
          :key="`z-${r.receipt_id}`"
          :r="r"
          :row-id="Number(row.id)"
          :draft="draftOf(r.receipt_id)"
          :selected="selectedId === r.receipt_id"
          :highlight="highlightId === r.receipt_id"
          @select="selectedId = r.receipt_id"
          @draft="setDraft"
          @confirm="confirmReceipt"
        />
      </div>

      <div class="cap-group failed" :data-testid="`section-failed-${row.id}`">
        <h5>请求失败 · 预测失败/超时 <em>{{ groups.failed.length }}</em></h5>
        <p v-if="!groups.failed.length" class="empty-mini">暂无请求失败回执</p>
        <ReceiptCard
          v-for="r in groups.failed"
          :key="`f-${r.receipt_id}`"
          :r="r"
          :row-id="Number(row.id)"
          :draft="draftOf(r.receipt_id)"
          :selected="selectedId === r.receipt_id"
          :highlight="highlightId === r.receipt_id"
          @select="selectedId = r.receipt_id"
          @draft="setDraft"
          @confirm="confirmReceipt"
        />
      </div>

      <div class="cap-group ok" :data-testid="`section-ok-${row.id}`">
        <h5>有效预测回执 <em>{{ groups.ok.length }}</em></h5>
        <p v-if="!groups.ok.length" class="empty-mini">暂无有效预测回执</p>
        <ReceiptCard
          v-for="r in groups.ok"
          :key="`o-${r.receipt_id}`"
          :r="r"
          :row-id="Number(row.id)"
          :draft="draftOf(r.receipt_id)"
          :selected="selectedId === r.receipt_id"
          :highlight="highlightId === r.receipt_id"
          @select="selectedId = r.receipt_id"
          @draft="setDraft"
          @confirm="confirmReceipt"
        />
      </div>
    </div>

    <!-- 近义动作名兼容（按当前选中回执确认；未选则确认最新回执） -->
    <div class="cap-alias-actions">
      <span>兼容操作名：</span>
      <button type="button" class="link" :data-testid="`action-${row.id}-确认实际容量变更`" @click="confirmSelected('确认实际容量变更')">确认实际容量变更</button>
      <button type="button" class="link" :data-testid="`action-${row.id}-确认评估`" @click="confirmSelected('确认评估')">确认评估</button>
      <button type="button" class="link" :data-testid="`action-${row.id}-确认容量变更`" @click="confirmSelected('确认容量变更')">确认容量变更</button>
      <button type="button" class="link" :data-testid="`action-${row.id}-确认回执`" @click="confirmSelected('确认回执')">确认回执</button>
    </div>

    <!-- 历史容量确认记录 -->
    <div class="cap-history" :data-testid="`history-${row.id}`">
      <h5>历史容量确认记录（兼容历史伸缩记录，共 {{ history.length }} 条）</h5>
      <table v-if="history.length" class="mini-table">
        <thead>
          <tr><th>#</th><th>确认时间</th><th>实际实例</th><th>来源</th><th>关联回执</th></tr>
        </thead>
        <tbody>
          <tr v-for="(h, i) in history" :key="`${h.confirmation_id}-${i}`">
            <td>{{ h.confirmation_id }}</td>
            <td>{{ h.confirmed_at || '—' }}</td>
            <td>{{ h.actual_instances }}</td>
            <td>{{ h.source }}</td>
            <td>{{ h.receipt_id ?? '—' }}</td>
          </tr>
        </tbody>
      </table>
      <p v-else class="empty-mini">暂无容量确认记录</p>
    </div>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, reactive, ref, watch } from 'vue'
import { getJson, runAction } from '@/api/backend-service'
import type { EntryRow } from '@/data/types'
import ReceiptCard from './ReceiptCard.vue'

const props = defineProps<{ row: EntryRow }>()
const emit = defineEmits<{ (e: 'changed'): void }>()

const rowId = Number(props.row.id)
const busy = ref(false)
const banner = ref('')
const errorMsg = ref('')
const predictionKind = ref<'ok' | 'missing' | 'zero' | 'failed' | 'timeout'>('ok')
const loadValue = ref('12')
const planId = ref('')
const bindPlanId = ref('')
const selectedId = ref<number | null>(null)
const highlightId = ref<number | null>(null)
const conflict = ref<Record<string, any> | null>(null)
const newReceiptHint = ref<number | null>(null)
const drafts = reactive<Record<string, string>>({})

const details = ref<Record<string, any>>({ missing: [], zero: [], failed: [], ok: [] })
const history = ref<any[]>([])
const effective = ref<Record<string, any> | null>(null)

const rowAny = computed(() => props.row as any)
const receiptsFromRow = computed<any[]>(
  () => rowAny.value.evaluation_receipts || rowAny.value.receipts || [],
)

const groups = computed(() => {
  const source: any[] = details.value?.all?.length ? details.value.all : receiptsFromRow.value
  const g = { missing: [] as any[], zero: [] as any[], failed: [] as any[], ok: [] as any[] }
  for (const r of source) {
    const kind = String(r.kind || r.prediction_kind || 'missing')
    if (kind === 'zero') g.zero.push(r)
    else if (kind === 'failed') g.failed.push(r)
    else if (kind === 'ok') g.ok.push(r)
    else g.missing.push(r)
  }
  return g
})

const effectiveInstances = computed(
  () => effective.value?.instances ?? rowAny.value.effective_capacity ?? rowAny.value._current_instances,
)
const effectiveBasis = computed(() => effective.value ?? rowAny.value.effective_capacity_info ?? null)

function draftOf(rid: number): string {
  if (drafts[String(rid)] !== undefined) return drafts[String(rid)]
  const r = receiptsFromRow.value.find((x) => x.receipt_id === rid)
  return String(r?.actual_instances ?? r?.target_instances ?? '')
}
function setDraft(payload: { rid: number; value: string }) {
  drafts[String(payload.rid)] = payload.value
}

async function refreshAll() {
  errorMsg.value = ''
  try {
    const [d, h, e] = await Promise.all([
      getJson(`/api/scalable_pool/${rowId}/evaluations`),
      getJson(`/api/scalable_pool/${rowId}/confirmations`),
      getJson(`/api/scalable_pool/${rowId}/effective-capacity`),
    ])
    details.value = d || {}
    history.value = h?.items || []
    effective.value = e || null
  } catch (err) {
    errorMsg.value = err instanceof Error ? err.message : '详情读取失败'
  }
}

async function submitEvaluation() {
  busy.value = true
  errorMsg.value = ''
  banner.value = ''
  try {
    const extra: Record<string, unknown> = { prediction_kind: predictionKind.value }
    if (predictionKind.value === 'ok') extra.predicted_load_next_hour = Number(loadValue.value || 0)
    if (predictionKind.value === 'timeout') extra.timeout = true
    if (planId.value) extra.scheduler_plan_id = Number(planId.value)
    const res = await runAction('scalable_pool', rowId, '申请评估', extra)
    if (!res.ok) throw new Error(res.message || '评估失败')
    banner.value = res.message || '已出具评估回执'
    const rid = res.result?.receipt?.receipt_id
    if (rid != null) {
      selectedId.value = rid
      drafts[String(rid)] = String(res.result?.receipt?.target_instances ?? '')
    }
    await refreshAll()
    emit('changed')
  } catch (err) {
    errorMsg.value = err instanceof Error ? err.message : '评估失败'
  } finally {
    busy.value = false
  }
}

async function bindPlan() {
  if (!bindPlanId.value) return
  errorMsg.value = ''
  try {
    const res = await runAction('scalable_pool', rowId, '绑定调度计划', {
      scheduler_plan_id: Number(bindPlanId.value),
    })
    if (!res.ok) throw new Error(res.message)
    banner.value = '已绑定并同步有效容量'
    emit('changed')
  } catch (err) {
    errorMsg.value = err instanceof Error ? err.message : '绑定失败'
  }
}

async function queryNew() {
  errorMsg.value = ''
  try {
    const res = await runAction('scalable_pool', rowId, '查询新评估')
    const rid = res.result?.receipt?.receipt_id
    newReceiptHint.value = rid ?? null
    if (rid != null) highlight(rid)
    await refreshAll()
  } catch (err) {
    errorMsg.value = err instanceof Error ? err.message : '查询失败'
  }
}

function highlight(rid: number | null | undefined) {
  if (rid == null) return
  highlightId.value = Number(rid)
  const el = document.getElementById(`receipt-card-${rowId}-${rid}`)
  el?.scrollIntoView({ behavior: 'smooth', block: 'center' })
}

async function confirmReceipt(payload: { rid: number; actual: string; action?: string }) {
  const action = payload.action || '确认实际容量变更'
  selectedId.value = payload.rid
  drafts[String(payload.rid)] = payload.actual
  busy.value = true
  errorMsg.value = ''
  conflict.value = null
  try {
    const res = await runAction('scalable_pool', rowId, action, {
      receipt_id: payload.rid,
      actual_instances: payload.actual === '' ? undefined : Number(payload.actual),
    })
    if (res.result?.conflict) {
      // 旧评估确认失败：保留选择，给出冲突原因和可查的新回执
      conflict.value = res.result
      await refreshAll()
      return
    }
    if (!res.ok) throw new Error(res.message || '确认失败')
    banner.value = res.message || '实际容量已确认'
    conflict.value = null
    await refreshAll()
    emit('changed')
  } catch (err) {
    errorMsg.value = err instanceof Error ? err.message : '确认失败'
  } finally {
    busy.value = false
  }
}

async function confirmSelected(action: string) {
  let rid = selectedId.value
  if (rid == null) {
    const all = details.value?.all || receiptsFromRow.value
    rid = all.length ? all[all.length - 1].receipt_id : null
  }
  if (rid == null) {
    errorMsg.value = '没有可确认的评估回执，请先申请评估'
    return
  }
  await confirmReceipt({ rid, actual: draftOf(rid), action })
}

onMounted(refreshAll)
// 父级通用动作会 reload 出行对象；行引用变化时同步最新回执/历史。
watch(() => props.row, () => { void refreshAll() })
defineExpose({ refreshAll })
</script>

<style scoped>
.cap-panel { border: 1px solid var(--border); border-radius: 10px; background: #fbfdff; padding: 12px 14px; margin: 12px 0; }
.cap-head { display: flex; justify-content: space-between; gap: 12px; align-items: flex-start; flex-wrap: wrap; }
.cap-head h4 { margin: 0; font-size: 15px; }
.cap-strategy { color: var(--muted); font-weight: 400; font-size: 12px; margin-left: 8px; }
.cap-effective { margin: 6px 0 0; font-size: 13px; }
.cap-head-actions { display: flex; gap: 8px; }
.cap-apply, .cap-bind { display: flex; gap: 10px; align-items: flex-end; flex-wrap: wrap; margin-top: 10px; }
.cap-apply label, .cap-bind label { display: flex; flex-direction: column; gap: 4px; font-size: 12px; color: var(--muted); }
.cap-apply input, .cap-apply select, .cap-bind input { padding: 6px 8px; border: 1px solid var(--border); border-radius: 6px; min-width: 150px; }
.cap-groups { display: grid; grid-template-columns: repeat(auto-fit, minmax(260px, 1fr)); gap: 10px; margin-top: 12px; }
.cap-group { border: 1px solid var(--border); border-radius: 8px; padding: 8px 10px; background: #fff; }
.cap-group h5 { margin: 2px 0 8px; font-size: 13px; display: flex; justify-content: space-between; }
.cap-group h5 em { font-style: normal; background: #eef2ff; border-radius: 10px; padding: 0 8px; font-size: 12px; }
.cap-group.failed { border-color: #f0b4b4; }
.cap-group.zero { border-color: #e7d9a8; }
.cap-group.missing { border-color: #c7d2fe; }
.empty-mini { color: var(--muted); font-size: 12px; margin: 4px 0; }
.cap-conflict { margin-top: 10px; border: 1px solid #e5484d; background: #fff5f5; border-radius: 8px; padding: 8px 12px; font-size: 13px; }
.cap-conflict strong { color: #c81e1e; }
.conflict-reason { margin: 6px 0; }
.preserve-hint { color: var(--muted); font-size: 12px; margin-left: 8px; }
.cap-banner { border-radius: 6px; padding: 6px 10px; font-size: 13px; margin: 8px 0 0; }
.cap-banner.ok { background: #ecfdf3; color: #067647; border: 1px solid #abefc6; }
.cap-banner.err { background: #fef3f2; color: #b42318; border: 1px solid #fecdca; }
.cap-banner.info { background: #eff8ff; color: #175cd3; border: 1px solid #b2ddff; }
.cap-alias-actions { display: flex; gap: 12px; align-items: center; margin-top: 10px; font-size: 12px; color: var(--muted); flex-wrap: wrap; }
.cap-history { margin-top: 12px; }
.cap-history h5 { margin: 0 0 6px; font-size: 13px; }
.mini-table { width: 100%; border-collapse: collapse; background: #fff; font-size: 12px; }
.mini-table th, .mini-table td { border: 1px solid var(--border); padding: 4px 8px; text-align: left; }
</style>
