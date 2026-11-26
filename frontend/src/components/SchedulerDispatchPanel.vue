<template>
  <section
    class="dispatch-panel"
    :data-testid="`dispatch-panel-${row.id}`"
    data-module="scheduler"
  >
    <header class="dp-head">
      <h4>
        调度派发面板
        <span class="dp-plan">{{ row['计划编号'] }} · {{ row['关联任务组'] }}</span>
      </h4>
      <button type="button" class="btn ghost" :data-testid="`refresh-quota-${row.id}`" @click="refresh">
        刷新容量
      </button>
    </header>

    <div class="dp-metrics">
      <div class="dp-metric" :data-testid="`effective-capacity-${row.id}`">
        <span>有效容量</span>
        <strong>{{ quota.effective_capacity ?? '—' }}</strong>
        <em v-if="quota.capacity_confirmed">已确认依据 #{{ quota.basis?.receipt_id ?? '?' }}</em>
        <em v-else class="warn">未确认容量，回退并发上限</em>
      </div>
      <div class="dp-metric" :data-testid="`active-tasks-${row.id}`">
        <span>在途任务</span>
        <strong>{{ quota.active_tasks ?? 0 }}</strong>
      </div>
      <div class="dp-metric" :data-testid="`dispatch-limit-${row.id}`">
        <span>执行上限（总派发上限）</span>
        <strong>{{ quota.dispatch_limit ?? '—' }}</strong>
      </div>
      <div class="dp-metric highlight" :data-testid="`claim-limit-${row.id}`">
        <span>下一次领取上限（可派发余量）</span>
        <strong>{{ quota.next_claim_limit ?? '—' }}</strong>
      </div>
      <div class="dp-metric">
        <span>队列深度</span>
        <strong>{{ quota.queue_depth ?? 0 }}</strong>
      </div>
    </div>
    <p class="dp-formula">
      余量 = 已确认有效容量 {{ quota.effective_capacity ?? '—' }} − 在途 {{ quota.active_tasks ?? 0 }}
      ＝ <b>{{ quota.next_claim_limit ?? '—' }}</b>；派发只采用已经确认的容量。
    </p>

    <div class="dp-actions">
      <form class="dp-inline" @submit.prevent="syncCapacity">
        <input v-model="poolId" type="number" :data-testid="`pool-id-${row.id}`" placeholder="伸缩策略ID" />
        <button type="submit" class="btn" :data-testid="`action-${row.id}-同步有效容量`">
          绑定/同步有效容量
        </button>
      </form>

      <form class="dp-inline" @submit.prevent="enqueue">
        <input v-model="missionId" type="number" :data-testid="`mission-id-${row.id}`" placeholder="任务ID" />
        <input v-model="priority" type="number" min="0" :data-testid="`priority-${row.id}`" placeholder="优先级" style="width:84px" />
        <button type="submit" class="btn" :data-testid="`action-${row.id}-加入队列`">加入队列</button>
      </form>

      <button
        type="button"
        class="btn primary"
        :disabled="busy"
        :data-testid="`action-${row.id}-派发下一个`"
        @click="dispatchNext"
      >派发下一个任务（领取）</button>
    </div>

    <p
      v-if="resultMsg"
      class="dp-result"
      :class="{ blocked: lastBlocked }"
      :data-testid="`dispatch-result-${row.id}`"
    >{{ resultMsg }}</p>

    <!-- 调度待办：与伸缩详情同步有效容量 -->
    <div class="dp-todos" :data-testid="`todos-${row.id}`">
      <h5>调度待办 · 有效容量同步</h5>
      <ul>
        <li v-for="(t, i) in todos" :key="i" :data-testid="`todo-${row.id}-${t.type}`">
          {{ t.message }}
        </li>
        <li v-if="!todos.length" class="empty-mini">暂无待办</li>
      </ul>
    </div>
  </section>
</template>

<script setup lang="ts">
import { onMounted, ref, watch } from 'vue'
import { getJson, runAction } from '@/api/backend-service'
import type { EntryRow } from '@/data/types'

const props = defineProps<{ row: EntryRow }>()
const emit = defineEmits<{ (e: 'changed'): void }>()
const rowId = Number(props.row.id)

const quota = ref<Record<string, any>>({})
const todos = ref<any[]>([])
const poolId = ref('')
const missionId = ref('')
const priority = ref('5')
const busy = ref(false)
const resultMsg = ref('')
const lastBlocked = ref(false)

async function refresh() {
  try {
    quota.value = await getJson(`/api/scheduler/${rowId}/dispatch-quota`)
    const t = await getJson(`/api/scheduler/${rowId}/todos`)
    todos.value = t?.items || []
  } catch {
    // 接口不可用时退回行内字段
    const r = props.row as any
    quota.value = {
      effective_capacity: r.effective_capacity,
      dispatch_limit: r.dispatch_upper_limit,
      next_claim_limit: r.next_claim_limit,
      active_tasks: r._active_tasks ?? 0,
      queue_depth: r._task_queue?.length ?? 0,
    }
  }
}

async function syncCapacity() {
  if (!poolId.value) return
  resultMsg.value = ''
  const res = await runAction('scheduler', rowId, '同步有效容量', { pool_id: Number(poolId.value) })
  resultMsg.value = res.message || (res.ok ? '有效容量已同步' : '同步失败')
  await refresh()
  emit('changed')
}

async function enqueue() {
  if (!missionId.value) return
  const res = await runAction('scheduler', rowId, '加入队列', {
    mission_id: Number(missionId.value),
    priority: Number(priority.value || 1),
  })
  resultMsg.value = res.ok ? `任务 ${missionId.value} 已入队` : res.message
  missionId.value = ''
  emit('changed')
}

async function dispatchNext() {
  busy.value = true
  resultMsg.value = ''
  try {
    const res = await runAction('scheduler', rowId, '派发下一个')
    lastBlocked.value = res.result?.blocked === true || res.ok === false
    resultMsg.value = res.message || (res.ok ? '已派发' : '暂不能派发')
    await refresh()
    emit('changed')
  } finally {
    busy.value = false
  }
}

onMounted(refresh)
watch(() => props.row, () => { void refresh() })
defineExpose({ refresh })
</script>

<style scoped>
.dispatch-panel { border: 1px solid var(--border); border-radius: 10px; background: #f8fafc; padding: 12px 14px; margin: 12px 0; }
.dp-head { display: flex; justify-content: space-between; align-items: center; }
.dp-head h4 { margin: 0; font-size: 15px; }
.dp-plan { color: var(--muted); font-weight: 400; font-size: 12px; margin-left: 8px; }
.dp-metrics { display: grid; grid-template-columns: repeat(auto-fit, minmax(170px, 1fr)); gap: 8px; margin-top: 10px; }
.dp-metric { background: #fff; border: 1px solid var(--border); border-radius: 8px; padding: 8px 10px; display: flex; flex-direction: column; gap: 2px; }
.dp-metric span { font-size: 12px; color: var(--muted); }
.dp-metric strong { font-size: 22px; line-height: 1.2; }
.dp-metric em { font-style: normal; font-size: 11px; color: #067647; }
.dp-metric em.warn { color: #b54708; }
.dp-metric.highlight { border-color: var(--brand); background: #f0f6ff; }
.dp-metric.highlight strong { color: var(--brand); }
.dp-formula { font-size: 12px; color: #475569; margin: 8px 0 0; }
.dp-actions { display: flex; gap: 14px; align-items: flex-end; flex-wrap: wrap; margin-top: 10px; }
.dp-inline { display: flex; gap: 6px; }
.dp-inline input { padding: 6px 8px; border: 1px solid var(--border); border-radius: 6px; width: 150px; }
.dp-result { margin-top: 8px; font-size: 13px; color: #067647; }
.dp-result.blocked { color: #b42318; }
.dp-todos { margin-top: 10px; }
.dp-todos h5 { margin: 0 0 4px; font-size: 13px; }
.dp-todos ul { margin: 0; padding-left: 18px; font-size: 12px; color: #334155; }
.empty-mini { color: var(--muted); }
</style>
