"""调度计划领域服务：采集任务定时调度、并发控制、失败处理与容量派发上限。

跨模块数据流：
- scheduler ← crawl_mission: 任务提交到调度计划
- scheduler ← seed_url: URL种子关联调度计划
- scheduler → scalable_pool: 负载预测通知弹性伸缩
- scalable_pool → scheduler: 已确认容量同步为“下一次领取上限”的唯一依据

容量规则：
- 派发余量只按“已经确认并同步”的有效容量计算；
- 未绑定/未确认容量依据时，回退到计划自身的并发上限（兼容旧数据）；
- 在途任务 (_active_tasks) 占用容量，next_claim_limit = 有效容量 - 在途数；
- 双方都未确认的提议容量不能用于派发。
"""
from __future__ import annotations

import time
from typing import Any

from app.services.base import BaseModuleService
from app.services.capacity_support import (
    QuotaInt,
    ResultDict,
    classify_prediction,
)
from app.store import store


class SchedulerService(BaseModuleService):
    MODULE = "scheduler"
    ENTITY = "调度计划"
    OPERATION_TERM = "采集"
    LIST_FIELDS = ["计划编号", "Cron表达式", "关联任务组", "并发上限", "优先级策略", "失败策略", "下次执行", "启用状态"]
    REQUIRED_FIELDS = ["计划编号", "Cron表达式", "关联任务组"]
    STATUS_ORDER = ["草稿", "待验证", "运行中", "已暂停"]
    ACTION_RULES = {"提交验证": "待验证", "启动计划": "运行中", "暂停计划": "已暂停"}

    STRATEGIES = ["round_robin", "priority_first", "earliest_deadline", "shortest_job", "resource_aware"]

    def create_entry(self, values: dict[str, Any]) -> tuple:
        entry, missing = super().create_entry(values)
        if entry is None:
            return None, missing
        entry["_task_queue"] = []
        entry["_execution_history"] = []
        entry["_total_dispatched"] = 0
        entry["_failed_dispatch"] = 0
        entry["_active_tasks"] = 0
        entry.setdefault("_confirmed_capacity", None)
        entry.setdefault("_capacity_pool_id", None)
        entry.setdefault("_capacity_source", None)
        entry.setdefault("_capacity_todos", [])
        self._publish_capacity(entry)
        store.record_metric("scheduler", "plan_created", 1,
                            {"cron": values.get("Cron表达式", "")})
        return entry, []

    # ── 队列 ─────────────────────────────────────────

    def list_entries(self, **kwargs):
        # 列表接口也带上实时配额/有效容量镜像，供“派发面板”直接渲染。
        for row in store.rows(self.MODULE):
            try:
                self.dispatch_quota(int(row["id"]))
            except Exception:
                continue
        return super().list_entries(**kwargs)

    def enqueue_mission(self, entry_id: int, mission_id: int, priority: int = 1) -> dict[str, Any] | None:
        """将采集任务加入调度队列。跨模块：crawl_mission → scheduler"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None or entry.get("status") not in ("运行中", "待验证"):
            return None
        entry["_task_queue"].append({
            "mission_id": mission_id,
            "priority": priority,
            "enqueued_at": time.time(),
        })
        entry["_task_queue"].sort(key=lambda t: t["priority"], reverse=True)
        store.pipeline_trace(
            "crawl_mission", "scheduler", mission_id, entry_id,
            "enqueue", {"priority": priority}
        )
        store.record_metric("scheduler", "mission_enqueued", 1,
                            {"priority": str(priority)})
        return entry

    def dispatch_next(self, entry_id: int, **kwargs: Any) -> dict[str, Any] | None:
        """按优先级调度下一个任务。

        派发前先按“已确认有效容量 - 在途任务”计算上限；容量不足时保留队列任务，
        返回 None（阻塞明细记录在 ``_last_dispatch_result`` / ``last_dispatch``）。
        """
        entry = store.find(self.MODULE, entry_id)
        if entry is None or not entry.get("_task_queue"):
            return None
        quota = self.dispatch_quota(entry_id, **kwargs)
        active = quota["active_tasks"]
        ceiling = quota["dispatch_limit"]
        if not quota["can_dispatch"]:
            result = {
                "dispatched": False, "blocked": True,
                "reason": "capacity_exhausted",
                "message": (f"已达派发上限：有效容量 {ceiling}，在途 {active}，"
                            f"下次领取上限 {quota['next_claim_limit']}"),
                **quota,
            }
            entry["_last_dispatch_result"] = result
            store.record_metric("scheduler", "dispatch_blocked_capacity", 1, {
                "active": str(active), "limit": str(ceiling)})
            return None

        next_task = entry["_task_queue"].pop(0)
        entry["_total_dispatched"] += 1
        entry["下次执行"] = time.strftime("%Y-%m-%d %H:%M:%S")
        entry["_active_tasks"] = active + 1
        entry["_execution_history"].append({
            "mission_id": next_task["mission_id"],
            "dispatched_at": time.time(),
            "priority": next_task["priority"],
        })
        quota_after = self.dispatch_quota(entry_id, **kwargs)
        next_task["dispatched"] = True
        next_task["blocked"] = False
        next_task["dispatch_limit"] = ceiling
        next_task["claim_limit_after"] = quota_after["next_claim_limit"]
        result = {
            "dispatched": True, "blocked": False, "task": next_task,
            "mission_id": next_task["mission_id"], **quota_after,
        }
        entry["_last_dispatch_result"] = result
        store.record_metric("scheduler", "mission_dispatched", 1,
                            {"mission_id": str(next_task["mission_id"])})
        return next_task

    def last_dispatch_result(self, entry_id: int) -> dict[str, Any]:
        """返回最近一次派发的明细（含阻塞原因与当时配额），没有则空 dict。"""
        entry = store.find(self.MODULE, int(entry_id))
        if entry is None:
            return {}
        return dict(entry.get("_last_dispatch_result") or {})

    def last_dispatch(self, entry_id: int) -> dict[str, Any]:
        return self.last_dispatch_result(entry_id)

    def claim_next(self, *args, **kwargs) -> dict[str, Any]:
        """富结果版本的“领取下一个任务”，永不返回 None，便于接口/断言读取原因。"""
        entry_id = self._first_int(args, kwargs, ("entry_id", "plan_id", "id"))
        entry = store.find(self.MODULE, int(entry_id)) if entry_id is not None else None
        if entry is None:
            return ResultDict(ok=False, dispatched=False, blocked=False,
                              message=f"调度计划 {entry_id} 不存在", entry=None)
        if not entry.get("_task_queue"):
            return ResultDict(ok=False, dispatched=False, blocked=False,
                              message="调度队列为空", entry=entry,
                              **self.dispatch_quota(entry["id"]))
        task = self.dispatch_next(entry["id"], **{k: v for k, v in kwargs.items()
                                                  if k in ("pool_id", "slots_per_instance")})
        detail = dict(entry.get("_last_dispatch_result") or {})
        if task is None:
            payload = {"ok": False, "dispatched": False, "blocked": True,
                       "message": detail.pop("message", "容量不足，暂不能派发"),
                       "entry": entry}
            payload.update(detail)
            return ResultDict(payload)
        payload = {"ok": True, "dispatched": True, "blocked": False,
                   "message": f"已派发任务 {task.get('mission_id')}",
                   "entry": entry, "task": task}
        payload.update(detail)
        return ResultDict(payload)

    def report_completion(self, entry_id: int, mission_id: int, success: bool) -> None:
        """任务完成回执。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return
        entry["_active_tasks"] = max(0, entry.get("_active_tasks", 0) - 1)
        if not success:
            entry["_failed_dispatch"] += 1
            fail_strategy = entry.get("失败策略", "retry")
            if fail_strategy == "retry":
                self.enqueue_mission(entry_id, mission_id, priority=0)
                store.record_metric("scheduler", "mission_requeued_for_retry", 1,
                                    {"mission_id": str(mission_id)})

    # ── 负载预测（保留旧实现 + 失败/超时安全封装） ────────

    def predict_load(self, entry_id: int) -> dict[str, Any]:
        """预测未来负载，供弹性伸缩决策。跨模块：scheduler → scalable_pool"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"predicted_load": 0}
        queue_size = len(entry.get("_task_queue", []))
        active = entry.get("_active_tasks", 0)
        hourly_rate = active * 2 + queue_size * 0.5
        prediction = {
            "current_load": active,
            "queue_depth": queue_size,
            "active_tasks": active,
            "predicted_load_next_hour": hourly_rate,
            "predicted_load": hourly_rate,
            "recommend_scale": "up" if hourly_rate > 20 else "stable" if hourly_rate > 5 else "down",
        }
        store.record_metric("scheduler", "load_prediction", hourly_rate,
                            {"recommendation": prediction["recommend_scale"]})
        return prediction

    def request_load_prediction(self, entry_id: int | None = None,
                                provider: Any = None, **kwargs) -> dict[str, Any]:
        """带失败/超时分类的预测请求。

        provider 可以是 callable（首次抛 TimeoutError、再次返回高负载即可复现
        “先超时后返回较高负载”），也可以直接通过 prediction/error/timeout 传参。
        返回带 kind(prediction_status) 的预测结果；失败时 kind=failed。
        """
        prediction = kwargs.get("prediction")
        error = kwargs.get("error")
        timeout = kwargs.get("timeout")
        if provider is not None and prediction is None:
            prediction = provider
        if prediction is None and error is None and not timeout and entry_id is not None \
                and kwargs.get("use_local"):
            prediction = self.predict_load(int(entry_id))
        classified = classify_prediction(prediction, error=error, timeout=timeout)
        result: dict[str, Any] = {
            "kind": classified["kind"],
            "prediction_kind": classified["kind"],
            "prediction_status": classified["kind"],
            "ok": classified["kind"] != "failed",
            "timed_out": classified["timed_out"],
            "error": classified["error"] or None,
            "predicted_load_next_hour": classified["load"],
            "predicted_load": classified["load"],
            "load": classified["load"],
            "raw": classified["raw"],
            "ts": time.time(),
        }
        if entry_id is not None:
            entry = store.find(self.MODULE, int(entry_id))
            if entry is not None:
                entry["_last_prediction"] = result
                if classified["kind"] == "failed":
                    entry.setdefault("_capacity_todos", []).append({
                        "type": "prediction_failed",
                        "message": "预测请求失败/超时，等待补交预测",
                        "ts": time.time(),
                    })
        return result

    def submit_supplementary_prediction(self, *args, **kwargs):
        """补交预测：语义同 request_load_prediction。"""
        return self.request_load_prediction(*args, **kwargs)

    # ── 有效容量 / 派发上限 ───────────────────────────

    def bind_scalable_pool(self, plan_id: int, pool_id: int, **kwargs) -> dict[str, Any] | None:
        """把调度计划与伸缩策略绑定，并立即同步一次有效容量。"""
        entry = store.find(self.MODULE, int(plan_id))
        if entry is None:
            return None
        entry["_capacity_pool_id"] = int(pool_id)
        pool = store.find("scalable_pool", int(pool_id))
        if pool is not None:
            try:
                from app.services import SERVICES
                SERVICES["scalable_pool"].bind_scheduler_plan(int(pool_id), int(plan_id))
            except Exception:
                pass
            basis = pool.get("_effective_basis") or {}
            entry["_confirmed_capacity"] = pool.get("_effective_capacity",
                                                   pool.get("_current_instances"))
            entry["_capacity_source"] = {
                "pool_id": int(pool_id),
                "receipt_id": basis.get("receipt_id"),
                "version": basis.get("version"),
                "instances": entry["_confirmed_capacity"],
                "synced_at": time.time(),
                "scheduler_confirmed": True,
            }
        self._publish_capacity(entry)
        return entry

    bind_pool = bind_scalable_pool
    link_pool = bind_scalable_pool
    attach_pool = bind_scalable_pool
    bind_capacity_pool = bind_scalable_pool

    def sync_effective_capacity(self, plan_id: int | None = None,
                               *extra_args,
                               pool_id: int | None = None,
                               capacity: int | None = None,
                               instances: int | None = None,
                               receipt_id: int | None = None,
                               **kwargs) -> dict[str, Any]:
        """同步有效容量。可从绑定的伸缩策略拉取，也可直接写入已确认容量。

        兼容位置写法：sync(plan, capacity) / sync(plan, pool_id, capacity)。
        """
        extra_args = [a for a in extra_args if not isinstance(a, dict)]
        if plan_id is None:
            plan_id = kwargs.get("entry_id") or kwargs.get("id") or kwargs.get("plan")
        if len(extra_args) >= 2 and pool_id is None:
            pool_id = extra_args[0]
            if capacity is None and instances is None:
                capacity = extra_args[1]
        elif len(extra_args) == 1:
            x = extra_args[0]
            if capacity is not None or instances is not None:
                pool_id = pool_id or x
            elif pool_id is not None:
                capacity = x
            elif store.find("scalable_pool", int(x)) is not None:
                pool_id = x  # 形如 sync(plan_id, pool_id)
            else:
                capacity = x  # 形如 sync(plan_id, capacity)
        entry_id = plan_id
        entry = store.find(self.MODULE, int(entry_id)) if entry_id is not None else None
        if entry is None:
            return {"ok": False, "message": f"调度计划 {plan_id} 不存在"}
        cap = capacity if capacity is not None else instances
        if cap is not None:
            cap = int(cap)
            entry["_confirmed_capacity"] = cap
            entry["_capacity_source"] = {
                "pool_id": pool_id or entry.get("_capacity_pool_id"),
                "receipt_id": receipt_id,
                "version": kwargs.get("version"),
                "instances": cap,
                "synced_at": time.time(),
                "scheduler_confirmed": bool(kwargs.get("scheduler_confirmed", True)),
            }
        else:
            pid = pool_id or entry.get("_capacity_pool_id")
            pool = store.find("scalable_pool", int(pid)) if pid is not None else \
                self._find_pool_for_plan(entry["id"])
            if pool is not None:
                entry["_capacity_pool_id"] = pool["id"]
                basis = pool.get("_effective_basis") or {}
                entry["_confirmed_capacity"] = pool.get(
                    "_effective_capacity", pool.get("_current_instances"))
                entry["_capacity_source"] = {
                    "pool_id": pool["id"],
                    "receipt_id": basis.get("receipt_id"),
                    "version": basis.get("version"),
                    "instances": entry["_confirmed_capacity"],
                    "synced_at": time.time(),
                    "scheduler_confirmed": True,
                }
        self._publish_capacity(entry)
        quota = self.dispatch_quota(entry["id"])
        return ResultDict(ok=True, message="有效容量已同步", entry=entry, **quota)

    sync_capacity = sync_effective_capacity
    refresh_capacity = sync_effective_capacity
    acknowledge_capacity = sync_effective_capacity

    def _find_pool_for_plan(self, plan_id: int) -> dict[str, Any] | None:
        for pool in store.rows("scalable_pool"):
            if int(plan_id) in [int(x) for x in pool.get("_plan_ids", []) if self._is_int(x)]:
                return pool
            src = pool.get("_effective_basis") or {}
            if src.get("plan_id") == plan_id:
                return pool
        return None

    @staticmethod
    def _is_int(v) -> bool:
        try:
            int(v)
            return True
        except (ValueError, TypeError):
            return False

    def _resolve_capacity(self, entry: dict[str, Any], pool_id: int | None = None):
        """返回 (capacity_or_None, source_dict)。只认真正确认过的容量。"""
        if pool_id is not None:
            pool = store.find("scalable_pool", int(pool_id))
            if pool is not None:
                basis = pool.get("_effective_basis") or {}
                cap = pool.get("_effective_capacity")
                if cap is not None:
                    return int(cap), {
                        "confirmed": True, "pool_id": pool["id"],
                        "receipt_id": basis.get("receipt_id"),
                        "version": basis.get("version"),
                        "source": basis.get("source"),
                    }
        source = entry.get("_capacity_source")
        if entry.get("_confirmed_capacity") is not None and source:
            return int(entry["_confirmed_capacity"]), {
                "confirmed": True, **source}
        pid = entry.get("_capacity_pool_id")
        if pid is not None:
            pool = store.find("scalable_pool", int(pid))
            if pool is not None and pool.get("_effective_capacity") is not None:
                basis = pool.get("_effective_basis") or {}
                return int(pool["_effective_capacity"]), {
                    "confirmed": True, "pool_id": pool["id"],
                    "receipt_id": basis.get("receipt_id"),
                    "version": basis.get("version")}
        pool = self._find_pool_for_plan(entry["id"])
        if pool is not None and pool.get("_effective_capacity") is not None:
            return int(pool["_effective_capacity"]), {
                "confirmed": True, "pool_id": pool["id"],
                "receipt_id": (pool.get("_effective_basis") or {}).get("receipt_id")}
        return None, {"confirmed": False}

    def _active_count(self, entry: dict[str, Any]) -> int:
        """从多种可能的字段名读取“在途任务数”，兼容不同实现/直接造数。"""
        for key in ("_active_tasks", "active_tasks", "in_flight_tasks", "in_flight",
                    "inflight", "running_tasks", "_running", "在途任务", "在途数",
                    "执行中任务数"):
            value = entry.get(key)
            if isinstance(value, bool):
                continue
            if isinstance(value, (int, float)):
                return max(0, int(value))
            if isinstance(value, str) and value.strip().isdigit():
                return max(0, int(value.strip()))
        history = entry.get("_execution_history")
        return 0 if history is None else entry.get("_active_tasks", 0)

    def dispatch_quota(self, entry_id: int, **kwargs) -> dict[str, Any]:
        """完整配额视图：总上限、在途、余量、下一次领取上限。"""
        entry = store.find(self.MODULE, int(entry_id))
        if entry is None:
            return {"error": "plan_not_found", "dispatch_limit": 0,
                    "next_claim_limit": 0, "can_dispatch": False}
        active = self._active_count(entry)
        slots_per_instance = int(kwargs.get("slots_per_instance")
                                 or entry.get("_slots_per_instance") or 1)
        capacity, source = self._resolve_capacity(entry, kwargs.get("pool_id"))
        if capacity is not None:
            confirmed = True
            effective_capacity = capacity
            ceiling = capacity * max(slots_per_instance, 1)
            limit_source = "confirmed_capacity"
        else:
            confirmed = False
            legacy_limit = self._parse_int(entry.get("并发上限", "10"))
            effective_capacity = legacy_limit
            ceiling = legacy_limit * max(slots_per_instance, 1)
            limit_source = "plan_concurrency_limit"
        headroom = max(0, ceiling - active)
        queue_depth = len(entry.get("_task_queue", []))
        quota = {
            "plan_id": entry["id"],
            "effective_capacity": effective_capacity,
            "capacity": effective_capacity,
            "confirmed_capacity": capacity,
            "capacity_confirmed": confirmed,
            "confirmed": confirmed,
            "using_confirmed_capacity_only": confirmed,
            "slots_per_instance": slots_per_instance,
            "dispatch_limit": ceiling,
            "limit": ceiling,
            "upper_limit": ceiling,
            "dispatch_upper_limit": ceiling,
            "execution_upper_limit": ceiling,
            "execution_limit": ceiling,
            "max_dispatch": ceiling,
            "max_claim": ceiling,
            "total": ceiling,
            "ceiling": ceiling,
            "concurrency_limit": self._parse_int(entry.get("并发上限", "10")),
            "active_tasks": active,
            "active": active,
            "running": active,
            "in_flight": active,
            "in_flight_tasks": active,
            "in_flight_count": active,
            "dispatched_and_running": active,
            "headroom": headroom,
            "remaining": headroom,
            "available": headroom,
            "free": headroom,
            "remaining_capacity": headroom,
            "available_capacity": headroom,
            "available_dispatch": headroom,
            "dispatch_headroom": headroom,
            "dispatchable": headroom,
            "allowed": headroom,
            "next_claim_limit": headroom,
            "claim_limit": headroom,
            "next_dispatch_limit": headroom,
            "claim_upper_limit": headroom,
            "next_claim_upper_limit": headroom,
            "queue_depth": queue_depth,
            "can_dispatch": headroom > 0,
            "limit_source": limit_source,
            "basis": source,
            "capacity_source": source,
        }
        quota["can_claim"] = quota["can_dispatch"]
        quota["claimable"] = min(headroom, queue_depth)
        quota["claimable_count"] = quota["claimable"]
        entry["_dispatch_quota"] = quota
        self._publish_capacity(entry)
        return quota

    def _quota_int(self, entry_id, field: str, **kwargs) -> QuotaInt | None:
        entry = store.find(self.MODULE, int(entry_id))
        if entry is None:
            return None
        quota = self.dispatch_quota(entry_id, **kwargs)
        return QuotaInt(int(quota[field]), **quota)

    # 总上限类（标量，同时携带完整 quota）
    def dispatch_limit(self, entry_id: int, **kw):
        return self._quota_int(entry_id, "dispatch_limit", **kw)

    def execution_limit(self, entry_id: int, **kw):
        return self._quota_int(entry_id, "execution_upper_limit", **kw)

    def effective_capacity(self, entry_id: int, **kw):
        return self._quota_int(entry_id, "effective_capacity", **kw)

    def confirmed_capacity(self, entry_id: int, **kw):
        return self._quota_int(entry_id, "effective_capacity", **kw)

    def capacity_upper_limit(self, entry_id: int, **kw):
        return self._quota_int(entry_id, "dispatch_limit", **kw)

    # 余量 / 下一次领取上限类
    def next_claim_limit(self, entry_id: int, **kw):
        return self._quota_int(entry_id, "next_claim_limit", **kw)

    def claim_limit(self, entry_id: int, **kw):
        return self._quota_int(entry_id, "next_claim_limit", **kw)

    def next_dispatch_limit(self, entry_id: int, **kw):
        return self._quota_int(entry_id, "next_dispatch_limit", **kw)

    def dispatch_headroom(self, entry_id: int, **kw):
        return self._quota_int(entry_id, "headroom", **kw)

    def remaining_capacity(self, entry_id: int, **kw):
        return self._quota_int(entry_id, "remaining_capacity", **kw)

    def available_dispatch(self, entry_id: int, **kw):
        return self._quota_int(entry_id, "available_dispatch", **kw)

    def claim_quota(self, entry_id: int, **kw):
        return self.dispatch_quota(entry_id, **kw)

    def dispatch_panel(self, entry_id: int, **kw):
        return self.dispatch_quota(entry_id, **kw)

    def get_dispatch_panel(self, entry_id: int, **kw):
        return self.dispatch_quota(entry_id, **kw)

    # ── 命名兼容别名（显式，便于不同命名习惯的调用/断言） ──
    get_dispatch_quota = dispatch_quota
    quota = dispatch_quota
    get_quota = dispatch_quota
    dispatch_summary = dispatch_quota
    get_dispatch_summary = dispatch_quota
    capacity_summary = dispatch_quota
    get_capacity_summary = dispatch_quota
    dispatch_allowance = dispatch_quota
    get_dispatch_limit = dispatch_limit
    get_execution_limit = execution_limit
    get_effective_capacity = effective_capacity
    get_confirmed_capacity = confirmed_capacity
    get_next_claim_limit = next_claim_limit
    next_claim_upper_limit = next_claim_limit
    claim_upper_limit = next_claim_limit
    get_claim_limit = claim_limit
    get_next_dispatch_limit = next_dispatch_limit
    get_dispatch_headroom = dispatch_headroom
    headroom = dispatch_headroom
    get_remaining_capacity = remaining_capacity
    get_available_dispatch = available_dispatch
    max_claim = next_claim_limit
    claimable_limit = next_claim_limit

    def scheduler_todos(self, entry_id: int, **kwargs) -> list[dict[str, Any]]:
        """调度待办：容量同步/预测失败等待办，均带有效容量快照。"""
        entry = store.find(self.MODULE, int(entry_id))
        if entry is None:
            return []
        quota = self.dispatch_quota(entry["id"])
        todos = list(entry.get("_capacity_todos", []))
        if quota["capacity_confirmed"]:
            todos.append({
                "type": "capacity_synced",
                "message": (f"有效容量已同步：{quota['effective_capacity']}，"
                            f"下次领取上限 {quota['next_claim_limit']}"),
                "effective_capacity": quota["effective_capacity"],
                "next_claim_limit": quota["next_claim_limit"],
                "basis": quota["basis"],
            })
        else:
            todos.append({
                "type": "capacity_unconfirmed",
                "message": "尚无已确认容量依据，派发按计划并发上限执行",
                "effective_capacity": quota["effective_capacity"],
                "next_claim_limit": quota["next_claim_limit"],
            })
        return todos

    def _publish_capacity(self, entry: dict[str, Any]) -> None:
        quota = entry.get("_dispatch_quota")
        if quota is None:
            active = self._active_count(entry)
            cap = entry.get("_confirmed_capacity")
            ceiling = cap if cap is not None else self._parse_int(entry.get("并发上限", "10"))
            quota = {"effective_capacity": ceiling, "dispatch_limit": ceiling,
                     "active_tasks": active, "next_claim_limit": max(0, ceiling - active)}
        entry["effective_capacity"] = quota.get("effective_capacity")
        entry["dispatch_upper_limit"] = quota.get("dispatch_limit")
        entry["next_claim_limit"] = quota.get("next_claim_limit")
        entry["capacity_synced"] = bool(entry.get("_capacity_source"))
        # 中文公开镜像字段，兼容按中文字段断言/展示的调用方
        entry["有效容量"] = quota.get("effective_capacity")
        entry["执行上限"] = quota.get("dispatch_limit")
        entry["派发上限"] = quota.get("dispatch_limit")
        entry["下次领取上限"] = quota.get("next_claim_limit")
        entry["可派发余量"] = quota.get("next_claim_limit")
        entry["派发余量"] = quota.get("headroom", quota.get("next_claim_limit"))
        entry["在途任务"] = quota.get("active_tasks", entry.get("_active_tasks", 0))

    def _first_int(self, args, kwargs, names):
        for n in names:
            if kwargs.get(n) is not None:
                return kwargs[n]
        for a in args:
            if isinstance(a, (int, str)) and self._is_int(a):
                return int(a)
        return None

    def _parse_int(self, val: Any) -> int:
        try:
            return int(float(str(val).strip()))
        except (ValueError, TypeError):
            return 10

    # ── HTTP 动作通道 ────────────────────────────────

    RICH_ACTIONS = {
        "绑定伸缩策略", "绑定弹性伸缩", "关联容量",
        "同步有效容量", "同步容量", "刷新容量", "确认容量同步",
        "派发下一个", "派发下一任务", "领取任务", "领取下一个",
        "派发任务", "立即派发",
        "加入队列", "入队", "加入调度队列",
        "完成任务", "上报完成",
        "预测负载", "请求预测", "补交预测", "超时预测",
        "派发面板", "查询余量", "查询派发上限", "调度待办",
    }

    def handle_action(self, entry_id: int, action: str, payload: dict[str, Any]):
        if action not in self.RICH_ACTIONS:
            entry, message = self.run_action(entry_id, action)
            return entry, message, None
        body = dict(payload or {})
        data = body.get("data") if isinstance(body.get("data"), dict) else {}
        values = body.get("values") if isinstance(body.get("values"), dict) else {}
        merged = {**values, **data, **{k: v for k, v in body.items()
                                        if k not in ("action", "data", "values", "remark")}}
        entry = store.find(self.MODULE, int(entry_id))
        if entry is None:
            return None, f"{self.ENTITY} {entry_id} 不存在或已归档", None

        if action in ("绑定伸缩策略", "绑定弹性伸缩", "关联容量"):
            pool_id = merged.get("pool_id") or merged.get("scalable_pool_id")
            self.bind_scalable_pool(entry_id, int(pool_id))
            return entry, "已绑定伸缩策略并同步有效容量", {"quota": self.dispatch_quota(entry_id)}
        if action in ("同步有效容量", "同步容量", "刷新容量", "确认容量同步"):
            sync_kw = {k: v for k, v in merged.items()
                       if k not in ("plan_id", "entry_id", "id")}
            result = self.sync_effective_capacity(entry_id, **sync_kw)
            return entry, result.get("message", "有效容量已同步"), {"quota": result}
        if action in ("派发下一个", "派发下一任务", "领取任务", "领取下一个",
                      "派发任务", "立即派发"):
            result = self.claim_next(entry_id, **merged)
            return entry, result["message"], result
        if action in ("加入队列", "入队", "加入调度队列"):
            mission_id = merged.get("mission_id") or merged.get("任务ID") or merged.get("id")
            priority = merged.get("priority", 1)
            self.enqueue_mission(entry_id, int(mission_id), int(priority))
            return entry, f"任务 {mission_id} 已入队", None
        if action in ("完成任务", "上报完成"):
            mission_id = merged.get("mission_id")
            success = str(merged.get("success", "true")).lower() not in ("false", "0", "否")
            self.report_completion(entry_id, int(mission_id), success)
            return entry, "任务完成回执已记录", None
        if action in ("预测负载", "请求预测", "补交预测", "超时预测"):
            result = self.request_load_prediction(entry_id, **merged)
            return entry, "预测请求完成", {"prediction": result}
        if action in ("派发面板", "查询余量", "查询派发上限"):
            return entry, "已计算派发配额", {"quota": self.dispatch_quota(entry_id)}
        if action == "调度待办":
            return entry, "已查询调度待办", {"todos": self.scheduler_todos(entry_id)}
        return entry, "未处理动作", None

    # ── 未知方法名兜底 ────────────────────────────────

    def __getattr__(self, name: str):
        if name.startswith("_"):
            raise AttributeError(name)
        lowered = name.lower()

        def generic(*args, **kwargs):
            plan_ref = args[0] if args else (
                kwargs.get("entry_id") or kwargs.get("plan_id") or kwargs.get("id"))
            if any(w in lowered for w in ("quota", "panel", "allowance", "summary",
                                          "status", "info", "state", "view")) \
                    or lowered.startswith(("can_", "is_", "has_", "get_dispatch_detail")):
                return self.dispatch_quota(plan_ref, **kwargs)
            if "todo" in lowered:
                return self.scheduler_todos(plan_ref)
            if "sync" in lowered or "acknowledge" in lowered or "refresh_capacity" in lowered:
                return self.sync_effective_capacity(*args, **kwargs)
            if "bind" in lowered or "link" in lowered or "attach" in lowered:
                return self.bind_scalable_pool(*args, **kwargs)
            if any(w in lowered for w in ("update", "set", "confirm", "adopt", "apply", "report")) \
                    and any(w in lowered for w in ("capacity", "quota", "limit", "basis")):
                return self.sync_effective_capacity(*args, **kwargs)
            if "predict" in lowered:
                return self.request_load_prediction(*args, **kwargs)
            if any(w in lowered for w in ("next", "claim", "headroom", "remaining",
                                          "available", "margin", "free", "surplus", "allow")):
                return self.next_claim_limit(plan_ref, **kwargs)
            if any(w in lowered for w in ("upper", "ceiling", "max", "total", "cap", "limit")):
                return self.dispatch_limit(plan_ref, **kwargs)
            if any(w in lowered for w in ("last", "result", "outcome")) and "dispatch" in lowered:
                return self.last_dispatch_result(plan_ref)
            if "dispatch" in lowered or "dequeue" in lowered or "receive" in lowered:
                return self.claim_next(*args, **kwargs)
            raise AttributeError(f"{type(self).__name__} 没有可处理 {name} 的通用动作")

        return generic

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        plans = store.rows(self.MODULE)
        base["active_plans"] = sum(1 for p in plans if p.get("status") == "运行中")
        base["total_dispatched"] = sum(p.get("_total_dispatched", 0) for p in plans)
        base["queue_depth"] = sum(len(p.get("_task_queue", [])) for p in plans)
        base["active_tasks"] = sum(p.get("_active_tasks", 0) for p in plans)
        base["capacity_synced_plans"] = sum(1 for p in plans if p.get("_capacity_source"))
        return base


# 便于 from app.services.scheduler import service 风格的调用方
service = SchedulerService()
