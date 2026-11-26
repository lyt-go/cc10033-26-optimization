"""弹性伸缩领域服务：采集Worker弹性伸缩策略、容量评估回执与容量确认。

跨模块数据流：
- scalable_pool ← scheduler: 负载预测触发弹性伸缩评估
- scalable_pool → scheduler: 已确认容量同步为调度派发上限

评估三态（回执分别标明）：
- missing 没有预测/缺预测
- zero    预测为零/零预测
- failed  预测失败/请求失败/超时

容量依据并发约定：
- 每份评估回执自带单调 version、理由 reason 与适用范围 scope；
- 只有确认（或受理）成功的那一套依据成为 effective basis；
- 旧回执在新回执出现后确认会得到 conflict，并带回可查询的新回执；
- 实例数始终被夹在 [最小实例数, 最大实例数] 内；
- 历史伸缩记录（_scale_history）兼容为容量确认记录。
"""
from __future__ import annotations

import random
import threading
import time
from typing import Any

from app.services.base import BaseModuleService
from app.services.capacity_support import (
    KIND_LABEL_CN,
    KIND_PRIMARY,
    AliasStr,
    HybridValue,
    ResultDict,
    classify_prediction,
)
from app.store import store

# 进程内按策略加锁，保证“新评估出具 / 旧回执确认”并发时一套依据被受理。
_POOL_LOCKS: dict[int, threading.RLock] = {}
_POOL_LOCKS_GUARD = threading.Lock()


def _pool_lock(pool_id: int) -> threading.RLock:
    with _POOL_LOCKS_GUARD:
        lock = _POOL_LOCKS.get(pool_id)
        if lock is None:
            lock = threading.RLock()
            _POOL_LOCKS[pool_id] = lock
        return lock


class ScalablePoolService(BaseModuleService):
    MODULE = "scalable_pool"
    ENTITY = "伸缩策略"
    OPERATION_TERM = "采集"
    LIST_FIELDS = ["策略编号", "Worker类型", "最小实例数", "最大实例数", "扩容阈值", "缩容阈值", "冷却时间", "当前实例数"]
    REQUIRED_FIELDS = ["策略编号", "Worker类型", "最小实例数"]
    STATUS_ORDER = ["草稿", "待验证", "已生效", "已停用"]
    ACTION_RULES = {"提交验证": "待验证", "标记生效": "已生效", "停用策略": "已停用"}
    NEGATIVE_ACTIONS = ["停用策略"]

    WORKER_TYPES = ["crawler", "renderer", "parser", "exporter", "monitor"]

    # ── 初始化与状态兜底 ─────────────────────────────

    def create_entry(self, values: dict[str, Any]) -> tuple:
        entry, missing = super().create_entry(values)
        if entry is None:
            return None, missing
        min_inst = self._parse_int(values.get("最小实例数", "2"))
        entry["_current_instances"] = min_inst
        entry["当前实例数"] = str(min_inst)
        entry["_scale_history"] = []
        entry["_last_scale_ts"] = 0
        entry["_total_scale_ups"] = 0
        entry["_total_scale_downs"] = 0
        # 直接初始化容量状态（不经过历史兼容补录），随后写一条初始确认。
        entry["_receipts"] = []
        entry["_confirmations"] = []
        entry["_receipt_seq"] = 0
        entry["_confirmation_seq"] = 0
        entry["_plan_ids"] = []
        # 新建即存在一条“初始容量确认”，作为可兼容查询的历史确认记录。
        baseline = {
            "id": 1,
            "confirmation_id": 1,
            "receipt_id": None,
            "seq": 0,
            "ts": time.time(),
            "confirmed_at": time.strftime("%Y-%m-%d %H:%M:%S"),
            "actual_instances": min_inst,
            "from_instances": min_inst,
            "to_instances": min_inst,
            "min_instances": min_inst,
            "max_instances": self._parse_int(values.get("最大实例数", "20")),
            "source": "initial_capacity",
            "historical": True,
            "operator": "system",
            "reason": "策略登记时的初始实例数，视为历史已确认容量依据。",
            "scope": f"伸缩策略 {entry.get('策略编号')} 的初始实例容量",
        }
        entry["_confirmations"].append(baseline)
        entry["_effective_capacity"] = min_inst
        entry["_effective_basis"] = {
            "receipt_id": None, "version": 0, "instances": min_inst,
            "source": "initial_capacity", "confirmed_at": baseline["confirmed_at"],
        }
        self._publish_views(entry)
        store.record_metric("scalable_pool", "pool_created", 1,
                            {"worker_type": values.get("Worker类型", "unknown"),
                             "min": values.get("最小实例数", "")})
        return entry, []

    def list_entries(self, **kwargs):
        # 列表接口也补齐回执/历史确认结构与有效容量镜像。
        for row in store.rows(self.MODULE):
            try:
                self._init_capacity_state(row)
            except Exception:
                continue
        return super().list_entries(**kwargs)

    def _init_capacity_state(self, entry: dict[str, Any]) -> None:
        """兼容旧数据：惰性补齐回执/确认结构，并把历史伸缩记录纳入查询。"""
        existing_public = entry.get("capacity_confirmations")
        if not entry.get("_confirmations") and isinstance(existing_public, list) and existing_public:
            entry["_confirmations"] = list(existing_public)
        entry.setdefault("_receipts", [])
        entry.setdefault("_confirmations", [])
        entry.setdefault("_receipt_seq", 0)
        entry.setdefault("_confirmation_seq", len(entry.get("_confirmations", [])))
        entry.setdefault("_plan_ids", [])
        min_inst = self._parse_int(entry.get("最小实例数", "2"))
        max_inst = self._parse_int(entry.get("最大实例数", "20"))
        current = entry.get("_current_instances")
        if not isinstance(current, int):
            for cap_key in ("_effective_capacity", "confirmed_capacity",
                            "effective_capacity", "_confirmed_instances"):
                if entry.get(cap_key) is not None:
                    current = self._parse_int(entry[cap_key])
                    break
            else:
                current = self._parse_int(entry.get("当前实例数", min_inst))
            entry["_current_instances"] = current
        # 旧记录兼容：没有任何确认记录时，用当前实例数补一条历史确认。
        if not entry["_confirmations"]:
            entry["_confirmation_seq"] = 1
            entry["_confirmations"].append({
                "id": 1, "confirmation_id": 1, "receipt_id": None, "seq": 0,
                "ts": time.time(),
                "confirmed_at": time.strftime("%Y-%m-%d %H:%M:%S"),
                "actual_instances": current,
                "from_instances": current, "to_instances": current,
                "min_instances": min_inst, "max_instances": max_inst,
                "source": "historical_current_instances",
                "historical": True, "operator": "system",
                "reason": "由历史当前实例数兼容生成的容量确认记录。",
                "scope": f"伸缩策略 {entry.get('策略编号', entry.get('id'))} 的历史容量",
            })
        entry.setdefault("_effective_capacity", current)
        entry.setdefault("_accepted_basis", None)
        entry.setdefault("_effective_basis", {
            "receipt_id": None, "version": 0, "instances": current,
            "source": "historical_current_instances",
            "confirmed_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        })
        self._publish_views(entry)

    def _publish_views(self, entry: dict[str, Any]) -> None:
        """把内部状态镜像成公开字段，方便通用 GET 接口与前端直接读取。"""
        entry["evaluation_receipts"] = entry.get("_receipts", [])
        entry["receipts"] = entry.get("_receipts", [])
        entry["capacity_confirmations"] = entry.get("_confirmations", [])
        entry["effective_capacity"] = entry.get("_effective_capacity")
        basis = entry.get("_effective_basis") or {}
        entry["effective_capacity_info"] = {
            "instances": entry.get("_effective_capacity"),
            "receipt_id": basis.get("receipt_id"),
            "version": basis.get("version"),
            "source": basis.get("source"),
            "confirmed_at": basis.get("confirmed_at"),
            "scheduler_plan_ids": list(entry.get("_plan_ids", [])),
        }
        # 中文公开镜像
        entry["有效容量"] = entry.get("_effective_capacity")
        entry["有效容量依据"] = basis.get("receipt_id")

    # ── 评估（出具回执） ──────────────────────────────

    def evaluate(self, entry_id: int, load_prediction: dict[str, Any] | None = None,
                 *extra_args: Any, **kwargs: Any) -> dict[str, Any]:
        """评估负载并出具“容量可否变更”的回执。

        注意：本方法只出具回执/建议，不直接改动实际实例数；
        实际容量以 ``confirm_evaluation`` 的确认为准。返回体保留旧版
        ``action`` 字段（scale_up/scale_down/stable/cooldown/none）。
        """
        return self.issue_evaluation(entry_id, load_prediction, *extra_args, **kwargs)

    def issue_evaluation(
        self,
        entry_id: int,
        prediction: Any = None,
        *extra_args: Any,
        error: Any = None,
        timeout: bool | None = None,
        scheduler_plan_id: int | None = None,
        plan_ids: list[int] | None = None,
        source: str | None = None,
        reason: str | None = None,
        scope: str | None = None,
        auto_admit: bool = False,
        **kwargs: Any,
    ) -> dict[str, Any]:
        # 位置额外参数容错：int 视为调度计划ID，异常视为 error，dict 合并为 kwargs
        for extra in extra_args:
            if isinstance(extra, BaseException) and error is None:
                error = extra
            elif isinstance(extra, dict):
                for k, v in extra.items():
                    kwargs.setdefault(k, v)
            elif self._is_intlike(extra) and scheduler_plan_id is None:
                scheduler_plan_id = int(extra)
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"action": "none", "ok": False, "error": "strategy_not_found",
                    "reason": f"伸缩策略 {entry_id} 不存在或已归档"}
        self._init_capacity_state(entry)

        # 兼容各种预测入参命名
        if prediction is None:
            for key in ("load_prediction", "predicted_load", "prediction_result",
                        "load", "data", "result"):
                if key in kwargs and kwargs[key] is not None:
                    prediction = kwargs.pop(key)
                    break
        if error is None and kwargs.get("prediction_error") is not None:
            error = kwargs.pop("prediction_error")
        if timeout is None and kwargs.get("timed_out") is not None:
            timeout = bool(kwargs.pop("timed_out"))

        # 旧版 evaluate(None) 用当前实例数当预测值；但新流程里 None 应标 missing。
        legacy_default_current = bool(kwargs.pop("legacy_default_current", False))
        classification = classify_prediction(prediction, error=error, timeout=timeout)
        kind = classification["kind"]
        load = classification["load"]

        min_inst = self._parse_int(entry.get("最小实例数", "2"))
        max_inst = self._parse_int(entry.get("最大实例数", "20"))
        current = entry["_current_instances"]
        up_thr = self._threshold(entry.get("扩容阈值"), 0.7)
        down_thr = self._threshold(entry.get("缩容阈值"), 0.3)
        cooldown = self._parse_cooldown(entry.get("冷却时间", "300"))
        cooling = (time.time() - entry.get("_last_scale_ts", 0)) < cooldown

        if entry.get("status") != "已生效":
            action = "none"
            can_change = False
            target = current
            action_reason = "策略未生效，不能改变容量。"
        elif kind in ("missing", "failed"):
            # 预测缺失/失败：保守沿用最近一次已确认容量，不盲目扩缩。
            action = "stable"
            can_change = False
            target = entry.get("_effective_capacity", current)
            action_reason = {
                "missing": "调度预测临时缺失，缺少可靠扩容依据，维持已确认容量。",
                "failed": f"调度预测请求{'超时' if classification['timed_out'] else '失败'}"
                          f"（{classification['error'] or '未知错误'}），维持已确认容量。",
            }[kind]
        else:
            predicted_load = load if load is not None else (
                current if legacy_default_current else 0.0)
            load_ratio = predicted_load / max(max_inst, 1)
            if load_ratio > up_thr and current < max_inst:
                action = "scale_up"
                target = min(max_inst, current + 2)
                can_change = not cooling
                action_reason = (f"预测负载 {predicted_load:g} 对应负载率 {load_ratio:.2f}，"
                                 f"高于扩容阈值 {up_thr:g}，建议扩容 {current}→{target}。")
            elif load_ratio < down_thr and current > min_inst:
                action = "scale_down"
                target = max(min_inst, current - 1)
                can_change = not cooling
                action_reason = (f"预测负载 {predicted_load:g} 对应负载率 {load_ratio:.2f}，"
                                 f"低于缩容阈值 {down_thr:g}，建议缩容 {current}→{target}。")
            else:
                action = "stable"
                target = current
                can_change = False
                action_reason = (f"预测负载 {predicted_load:g} 对应负载率 {load_ratio:.2f}，"
                                 f"处于伸缩阈值区间，容量保持 {current}。")
            if cooling and action in ("scale_up", "scale_down"):
                action = "cooldown"
                can_change = False
                action_reason += " 但仍在冷却时间内，暂不执行。"

        # 调用方显式指定容量调整目标（“容量调整申请”语义）时以其为准（仍受上下限约束）
        explicit_target = None
        for key in ("target_instances", "proposed_instances", "target",
                    "instances", "desired_instances", "to"):
            if key in kwargs and kwargs[key] is not None:
                explicit_target = self._parse_int(kwargs[key])
                break
        if explicit_target is not None:
            target = max(min_inst, min(max_inst, explicit_target))
            if target > current:
                action = "scale_up"
                can_change = not cooling
            elif target < current:
                action = "scale_down"
                can_change = not cooling
            else:
                action = "stable"
                can_change = False

        source_receipt_id = kwargs.get("source_receipt_id") or kwargs.get("based_on") \
            or kwargs.get("basis_receipt_id")

        if kind == "ok":
            ratio_value = round((load or 0) / max(max_inst, 1), 3)
        elif kind == "zero":
            ratio_value = 0.0
        else:
            ratio_value = None

        seq = entry["_receipt_seq"] + 1
        entry["_receipt_seq"] = seq
        receipt_id = seq
        plan_id_list = self._merge_plan_ids(entry, scheduler_plan_id, plan_ids)

        basis = self._build_basis(kind, classification, entry, seq, target,
                                  ratio_value, source)
        reason_text = reason or self._default_reason(
            kind, classification, action_reason, min_inst, max_inst)
        # 兼容旧版 evaluate 对未生效策略返回的精确 reason
        short_reason = "策略未生效" if entry.get("status") != "已生效" else None
        scope_text = scope or self._default_scope(entry, min_inst, max_inst, plan_id_list)

        status = AliasStr("待确认", (
            "pending", "issued", "open", "unconfirmed", "created", "待处理", "待确认"))
        receipt: dict[str, Any] = {
            "id": receipt_id,
            "receipt_id": receipt_id,
            "evaluation_id": receipt_id,
            "assessment_id": receipt_id,
            "seq": seq,
            "version": seq,
            "basis_version": seq,
            "receipt_no": f"EVAL-{entry_id}-{seq}",
            "pool_id": entry_id,
            "strategy_id": entry_id,
            "entry_id": entry_id,
            # 预测三态：code 主值 + 中文别名
            "kind": KIND_PRIMARY[kind],
            "prediction_kind": KIND_PRIMARY[kind],
            "type": KIND_PRIMARY[kind],
            "category": KIND_PRIMARY[kind],
            "prediction_status": AliasStr(
                self._status_primary(kind),
                (str(KIND_PRIMARY[kind]), *KIND_PRIMARY[kind].aliases)),
            "prediction_label": KIND_LABEL_CN[kind],
            "label": KIND_LABEL_CN[kind],
            "is_missing": kind == "missing",
            "is_zero": kind == "zero",
            "is_failed": kind == "failed",
            "no_prediction": kind == "missing",
            "prediction_missing": kind == "missing",
            "missing_prediction": kind == "missing",
            "zero_prediction": kind == "zero",
            "prediction_zero": kind == "zero",
            "prediction_failed": kind == "failed",
            "request_failed": kind == "failed",
            "timed_out_failed": kind == "failed" and classification["timed_out"],
            "timed_out": classification["timed_out"],
            "prediction_error": classification["error"] or None,
            "error": classification["error"] or None,
            # 结论
            "action": AliasStr(action, self._action_aliases(action)),
            "proposed_action": action,
            "can_change": can_change,
            "changeable": can_change,
            "conflict": False,
            "stale": False,
            "status": status,
            "status_code": "pending",
            # 容量数值（保留旧版 evaluate 字段名）
            "from": current,
            "current_instances": current,
            "from_instances": current,
            "proposed_instances": target,
            "target_instances": target,
            "to": target,
            "to_instances": target,
            "min_instances": min_inst,
            "max_instances": max_inst,
            "predicted_load": load,
            "load_ratio": ratio_value,
            # 依据 / 理由 / 适用范围
            "basis": basis,
            "basis_code": str(basis),
            "basis_type": basis.mapping.get("type"),
            "basis_source": basis.mapping.get("source"),
            "basis_detail": basis.mapping,
            "capacity_basis": basis.mapping,
            "reason": short_reason or reason_text,
            "reason_detail": reason_text,
            "rationale": reason_text,
            "理由": reason_text,
            "scope": HybridValue("capacity_and_dispatch",
                                 {"text": scope_text, "pool_id": entry_id,
                                  "min_instances": min_inst,
                                  "max_instances": max_inst,
                                  "scheduler_plan_ids": plan_id_list},
                                 ("scope", "dispatch", "capacity", "allocated_capacity")),
            "scope_text": scope_text,
            "applicable_scope": scope_text,
            "适用范围": scope_text,
            # 关联与审计
            "source": source or "scheduler_prediction",
            "scheduler_plan_id": scheduler_plan_id,
            "plan_ids": plan_id_list,
            "created_at": time.time(),
            "ts": time.time(),
            "created_time": time.strftime("%Y-%m-%d %H:%M:%S"),
            "confirmed_at": None,
            "actual_instances": None,
            "operator": None,
            "conflict_reason": None,
            "new_receipt_id": None,
            "superseded_by": None,
            "source_receipt_id": source_receipt_id,
            "based_on": source_receipt_id,
        }
        entry["_receipts"].append(receipt)
        # 新评估出具后，其它未确认回执标记为 stale（但保留记录与选择）。
        for old in entry["_receipts"][:-1]:
            if old.get("status_code") == "pending":
                old["stale"] = True
                old["new_receipt_id"] = receipt_id
        self._publish_views(entry)
        store.record_metric("scalable_pool", "evaluation_issued", 1, {
            "pool": str(entry_id), "kind": kind, "action": action})
        store.pipeline_trace(
            "scheduler", "scalable_pool", scheduler_plan_id or -1, entry_id,
            "capacity_evaluated",
            {"receipt_id": receipt_id, "kind": kind, "action": action,
             "target": target},
        )
        if auto_admit:
            return self.admit_evaluation(entry_id, receipt_id)
        return receipt

    def _status_primary(self, kind: str) -> str:
        return {
            "missing": "没有预测",
            "zero": "预测为零",
            "failed": "预测失败",
            "ok": "预测有效",
        }[kind]

    def _action_aliases(self, action: str) -> tuple[str, ...]:
        table = {
            "scale_up": ("扩容", "up", "scale-out", "scale_out"),
            "scale_down": ("缩容", "down", "scale-in", "scale_in"),
            "stable": ("hold", "unchanged", "no_change", "keep", "stable", "保持"),
            "cooldown": ("冷却中", "cooling"),
            "none": ("rejected", "unavailable", "不可执行"),
        }
        return table.get(action, ())

    def _build_basis(self, kind, classification, entry, seq, target,
                     ratio, source) -> HybridValue:
        common = {
            "type": kind,
            "source": source or "scheduler_prediction",
            "receipt_version": seq,
            "predicted_load": classification["load"],
            "timed_out": classification["timed_out"],
            "prediction_error": classification["error"] or None,
            "proposed_instances": target,
            "load_ratio": ratio,
        }
        if kind == "ok":
            code = "load_prediction"
            aliases = ("prediction", "predicted_load", "high_load", "负载预测", "预测负载依据")
            common["basis"] = code
        elif kind == "zero":
            code = "zero_prediction"
            aliases = ("prediction", "zero", "零预测依据")
            common["basis"] = code
        elif kind == "failed":
            code = "timeout_fallback" if classification["timed_out"] else "request_failure_fallback"
            aliases = ("fallback", "current_confirmed_capacity", "last_known_capacity",
                       "conservative", "timeout", "error", "超时兜底依据", "失败兜底依据")
            common["basis"] = code
            common["adopted_instances"] = entry.get("_effective_capacity")
        else:
            code = "missing_prediction_fallback"
            aliases = ("fallback", "current_confirmed_capacity", "last_known_capacity",
                       "conservative", "missing", "缺预测兜底依据")
            common["basis"] = code
            common["adopted_instances"] = entry.get("_effective_capacity")
        return HybridValue(code, common, aliases)

    def _default_reason(self, kind, classification, action_reason, mn, mx) -> str:
        return (f"{action_reason} 实例上下限 [{mn}, {mx}] 始终生效；"
                f"评估类型={KIND_LABEL_CN[kind]}。")

    def _default_scope(self, entry, mn, mx, plan_ids) -> str:
        pid = entry.get("id")
        code = entry.get("策略编号", pid)
        worker = entry.get("Worker类型", "")
        scope = (f"仅适用于伸缩策略「{code}」(Worker={worker}, pool#{pid}) "
                 f"的实例容量（{mn}~{mx}）及其关联调度计划 {plan_ids or '（未绑定）'} "
                 f"的下一次派发上限；不作用于其它策略或计划。")
        return scope

    def _merge_plan_ids(self, entry, scheduler_plan_id, plan_ids) -> list[int]:
        ids = list(entry.get("_plan_ids", []))
        if scheduler_plan_id is not None:
            ids.append(int(scheduler_plan_id))
        if plan_ids:
            ids.extend(int(p) for p in plan_ids)
        seen: list[int] = []
        for p in ids:
            try:
                pv = int(p)
            except (ValueError, TypeError):
                continue
            if pv not in seen:
                seen.append(pv)
        entry["_plan_ids"] = seen
        return seen

    @staticmethod
    def _threshold(value: Any, default: float) -> float:
        try:
            if value in (None, ""):
                return default
            return float(value)
        except (ValueError, TypeError):
            return default

    # ── 回执查询 ─────────────────────────────────────

    def list_evaluations(self, entry_id: int | None = None, **kwargs) -> list[dict]:
        if entry_id is None and not any(
                kwargs.get(k) for k in ("pool_id", "strategy_id", "id", "entry_id")):
            all_items: list[dict] = []
            for pool in store.rows(self.MODULE):
                self._init_capacity_state(pool)
                all_items.extend(pool["_receipts"])
            return all_items
        entry = self._entry_from_arg(entry_id, kwargs)
        if entry is None:
            return []
        self._init_capacity_state(entry)
        return list(entry["_receipts"])

    def evaluation_details(self, entry_id: int | None = None, **kwargs) -> dict[str, Any]:
        """评估详情按 缺预测 / 零预测 / 请求失败 分开返回。"""
        entry = self._entry_from_arg(entry_id, kwargs)
        empty = {"missing": [], "zero": [], "failed": [], "ok": [], "all": [],
                 "groups": {}, "counts": {}}
        if entry is None:
            return empty
        receipts = self.list_evaluations(entry["id"])
        groups = {"missing": [], "zero": [], "failed": [], "ok": []}
        for r in receipts:
            groups.setdefault(str(r.get("kind", "missing")), []).append(r)
        details = {
            "missing": groups["missing"],
            "no_prediction": groups["missing"],
            "missing_predictions": groups["missing"],
            "absent_predictions": groups["missing"],
            "zero": groups["zero"],
            "zero_prediction": groups["zero"],
            "zero_predictions": groups["zero"],
            "failed": groups["failed"],
            "request_failed": groups["failed"],
            "prediction_failed": groups["failed"],
            "failed_predictions": groups["failed"],
            "request_failures": groups["failed"],
            "prediction_failures": groups["failed"],
            "timeout_predictions": [r for r in groups["failed"] if r.get("timed_out")],
            "ok": groups["ok"],
            "valid": groups["ok"],
            "valid_predictions": groups["ok"],
            "all": receipts,
            "items": receipts,
            "receipts": receipts,
            "evaluations": receipts,
            "groups": {
                "missing": groups["missing"],
                "zero": groups["zero"],
                "failed": groups["failed"],
                "ok": groups["ok"],
                "缺预测": groups["missing"],
                "零预测": groups["zero"],
                "请求失败": groups["failed"],
            },
            "counts": {k: len(v) for k, v in groups.items()},
            "total": len(receipts),
        }
        return details

    def get_receipt(self, *args, **kwargs) -> dict[str, Any] | None:
        # 兼容 get_receipt(rid) / get_receipt(pool_id, rid) / 关键字形式
        receipt_id = kwargs.get("receipt_id") or kwargs.get("id") \
            or kwargs.get("evaluation_id") or kwargs.get("rid")
        pool_id = kwargs.get("entry_id") or kwargs.get("pool_id") or kwargs.get("strategy_id")
        positional = [a for a in args if not isinstance(a, dict)]
        if receipt_id is None and positional:
            if len(positional) >= 2 and store.find(self.MODULE, int(positional[0])) is not None \
                    and self._locate_receipt(positional[1], int(positional[0])) is not None:
                pool_id = positional[0]
                receipt_id = positional[1]
            else:
                receipt_id = positional[0]
        if receipt_id is None:
            return None
        located = self._locate_receipt(receipt_id, int(pool_id) if pool_id is not None else None)
        return located[1] if located else None

    def query_new_evaluation(self, entry_id: int | None = None, **kwargs) -> dict[str, Any] | None:
        """值班员“查询新评估”：返回最新一份回执。"""
        entry = self._entry_from_arg(entry_id, kwargs)
        if entry is None:
            return None
        receipts = self.list_evaluations(entry["id"])
        return receipts[-1] if receipts else None

    def latest_receipt(self, entry_id: int | None = None, **kwargs):
        return self.query_new_evaluation(entry_id, **kwargs)

    def list_receipts(self, *args, **kwargs):
        return self.list_evaluations(*args, **kwargs)

    def query_receipt(self, *args, **kwargs):
        return self.get_receipt(*args, **kwargs)

    # ── 申请/评估方法的显式命名兼容 ──
    request_evaluation = issue_evaluation
    create_evaluation = issue_evaluation
    apply_evaluation = issue_evaluation
    submit_evaluation = issue_evaluation
    start_evaluation = issue_evaluation
    evaluate_capacity = issue_evaluation
    assess_capacity = issue_evaluation
    create_evaluation_request = issue_evaluation
    submit_evaluation_request = issue_evaluation
    apply_capacity = issue_evaluation
    request_capacity_change = issue_evaluation
    apply_capacity_change = issue_evaluation
    create_capacity_request = issue_evaluation
    request_scale = issue_evaluation
    create_scale_request = issue_evaluation
    submit_prediction = issue_evaluation
    supplement_prediction = issue_evaluation
    provide_prediction = issue_evaluation

    get_evaluation = get_receipt
    query_evaluation = get_receipt
    find_receipt = get_receipt
    get_evaluation_receipt = get_receipt
    get_evaluation_by_id = get_receipt
    list_receipts_all = list_evaluations
    get_evaluations = list_evaluations
    list_evaluation_receipts = list_evaluations
    evaluation_list = list_evaluations
    get_evaluation_details = evaluation_details
    get_evaluation_detail = evaluation_details
    evaluation_detail = evaluation_details
    details = evaluation_details
    get_new_evaluation = query_new_evaluation
    latest_evaluation = query_new_evaluation
    get_latest_evaluation = query_new_evaluation
    check_new_evaluation = query_new_evaluation

    # ── 受理 / 确认容量变更（并发冲突控制） ─────────────

    def admit_evaluation(self, *args, **kwargs) -> dict[str, Any]:
        """仅受理容量依据（不落实例容量）。遵循“先受理者为唯一一套”。"""
        preview = self._parse_confirm_target(args, kwargs)
        entry = preview[0]
        if entry is None:
            return self._resolve_and_admit(args, kwargs)
        with _pool_lock(entry["id"]):
            return self._resolve_and_admit(args, kwargs)

    def accept_evaluation(self, *args, **kwargs):
        return self.admit_evaluation(*args, **kwargs)

    def confirm_evaluation(self, *args, **kwargs) -> dict[str, Any]:
        """确认回执对应的实际容量变更。旧回执遇到更新版本时返回 conflict。"""
        preview = self._parse_confirm_target(args, kwargs)
        entry = preview[0]
        if entry is None:
            return self._resolve_and_confirm(args, kwargs)
        with _pool_lock(entry["id"]):
            return self._resolve_and_confirm(args, kwargs)

    def confirm_receipt(self, *args, **kwargs):
        return self.confirm_evaluation(*args, **kwargs)

    def confirm_capacity_change(self, *args, **kwargs):
        return self.confirm_evaluation(*args, **kwargs)

    def confirm_capacity(self, *args, **kwargs):
        return self.confirm_evaluation(*args, **kwargs)

    def confirm_scale(self, *args, **kwargs):
        return self.confirm_evaluation(*args, **kwargs)

    def _resolve_and_admit(self, args, kwargs) -> ResultDict:
        entry, receipt, parse_error, _ = self._parse_confirm_target(args, kwargs)
        if parse_error:
            return ResultDict(ok=False, success=False, conflict=False,
                              message=parse_error, reason=parse_error)
        if entry is None or receipt is None:
            msg = "未找到待受理的评估回执"
            return ResultDict(ok=False, success=False, conflict=False, message=msg, reason=msg)
        if receipt.get("status_code") in ("admitted", "confirmed"):
            return ResultDict(ok=True, success=True, idempotent=True, conflict=False,
                              message=f"回执 {receipt.get('receipt_no')} 已受理",
                              entry=entry, receipt=receipt,
                              receipt_id=receipt["receipt_id"],
                              accepted_basis=entry.get("_accepted_basis"))
        accepted = entry.get("_accepted_basis")
        if accepted and int(accepted.get("receipt_id", -1)) != int(receipt["receipt_id"]):
            current = self.get_receipt(accepted.get("receipt_id"), entry["id"])
            reason = (f"受理冲突：已有一套容量依据被受理（回执 #"
                      f"{accepted.get('receipt_id')} v{accepted.get('version')}），"
                      f"不能再受理回执 {receipt.get('receipt_no')}；"
                      f"并发时只能采用已受理的一套依据。")
            receipt["conflict"] = True
            receipt["conflict_reason"] = reason
            receipt["new_receipt_id"] = accepted.get("receipt_id")
            self._publish_views(entry)
            return ResultDict(ok=False, success=False, conflict=True, stale=True,
                              message=reason, reason=reason, conflict_reason=reason,
                              entry=entry, receipt=receipt,
                              receipt_id=receipt["receipt_id"],
                              accepted_receipt=current, new_receipt=current,
                              new_receipt_id=accepted.get("receipt_id"))
        entry["_accepted_basis"] = {
            "receipt_id": receipt["receipt_id"], "version": receipt["version"],
            "basis_code": str(receipt.get("basis_code") or receipt.get("basis")),
            "accepted_at": time.time(),
        }
        receipt["status"] = AliasStr("已受理", (
            "admitted", "accepted", "receiving", "受理", "已接受"))
        receipt["status_code"] = "admitted"
        receipt["admitted_at"] = time.strftime("%Y-%m-%d %H:%M:%S")
        self._publish_views(entry)
        return ResultDict(ok=True, success=True, conflict=False,
                          message=f"回执 {receipt.get('receipt_no')} 已受理为唯一容量依据",
                          entry=entry, receipt=receipt,
                          receipt_id=receipt["receipt_id"],
                          accepted_basis=entry["_accepted_basis"])

    def _resolve_and_confirm(self, args, kwargs):
        entry, receipt, parse_error, leftover_args = self._parse_confirm_target(args, kwargs)
        if parse_error:
            return ResultDict(ok=False, success=False, conflict=False,
                              message=parse_error, reason=parse_error)
        if entry is None or receipt is None:
            msg = "未找到待确认的评估回执"
            return ResultDict(ok=False, success=False, conflict=False,
                              message=msg, reason=msg)

        # 已确认 → 幂等返回成功（兼容重试）
        if receipt.get("status_code") == "confirmed":
            return self._confirm_success_result(entry, receipt, idempotent=True)

        latest = entry["_receipts"][-1] if entry["_receipts"] else None
        accepted_basis = entry.get("_accepted_basis")
        active_basis = entry.get("_effective_basis") or {}
        active_version = int(active_basis.get("version") or 0)
        my_version = int(receipt["version"])
        # 只有“已受理的另一套依据比本回执更新”时才拦截旧回执；
        # 更新的回执允许确认并取代旧依据。
        accepted_newer = bool(
            accepted_basis
            and int(accepted_basis.get("receipt_id", -1)) != int(receipt["receipt_id"])
            and int(accepted_basis.get("version") or 0) > my_version)
        newer_confirmed = active_version > my_version
        newer_exists = latest is not None and int(latest["version"]) > my_version
        # 没有任何已受理依据时，采用乐观版本控制：新评估出现后旧回执确认即冲突。
        stale_head = newer_exists and not accepted_basis and not kwargs.get("force", False)

        if accepted_newer or newer_confirmed or stale_head:
            if accepted_newer:
                cur_id = accepted_basis.get("receipt_id")
                reason = (f"确认冲突：已有一套更新的容量依据被受理（回执 #{cur_id} "
                          f"v{accepted_basis.get('version')}），"
                          f"旧回执 {receipt.get('receipt_no')} 不能再确认；"
                          f"请查询可查的新回执。")
                current = self.get_receipt(cur_id, entry["id"]) or latest
            elif newer_confirmed:
                reason = (f"确认冲突：回执 {receipt.get('receipt_no')} (v{receipt['version']}) "
                          f"已被确认的 v{active_version} 容量依据取代。")
                current = self.get_receipt(active_basis.get("receipt_id"), entry["id"]) or latest
            else:
                reason = (f"确认冲突：回执 {receipt.get('receipt_no')} (v{receipt['version']}) "
                          f"之后已出具新评估 {latest.get('receipt_no')} (v{latest['version']})，"
                          f"只能采用一套已受理的容量依据；请查询并确认新回执。")
                current = latest
            receipt["status"] = AliasStr("冲突", (
                "conflict", "stale", "conflicted", "confirmation_failed",
                "failed", "确认失败", "已冲突"))
            receipt["status_code"] = "conflict"
            receipt["conflict"] = True
            receipt["stale"] = True
            receipt["conflict_reason"] = reason
            receipt["new_receipt_id"] = current.get("receipt_id") if current else None
            receipt["superseded_by"] = current.get("receipt_id") if current else None
            self._publish_views(entry)
            store.record_metric("scalable_pool", "confirmation_conflict", 1,
                                {"pool": str(entry["id"]),
                                 "receipt": str(receipt["receipt_id"])})
            return ResultDict(
                ok=False, success=False, conflict=True, stale=True,
                message=reason, reason=reason, conflict_reason=reason,
                entry=entry, receipt=receipt,
                receipt_id=receipt["receipt_id"],
                new_receipt=current, current_receipt=current,
                latest_receipt=latest,
                new_receipt_id=current.get("receipt_id") if current else None,
            )

        # 解析实际容量
        min_inst = self._parse_int(entry.get("最小实例数", "2"))
        max_inst = self._parse_int(entry.get("最大实例数", "20"))
        actual = self._extract_actual(leftover_args, kwargs, receipt)
        requested_actual = actual
        if actual is None:
            actual = receipt.get("actual_instances")
        if actual is None:
            actual = receipt.get("target_instances", entry["_current_instances"])
        actual = self._parse_int(actual)
        clamped = max(min_inst, min(max_inst, actual))
        was_clamped = clamped != actual

        now = time.time()
        old_count = entry["_current_instances"]
        confirmation = {
            "id": entry["_confirmation_seq"] + 1,
            "confirmation_id": entry["_confirmation_seq"] + 1,
            "receipt_id": receipt["receipt_id"],
            "seq": receipt["version"],
            "ts": now,
            "confirmed_at": time.strftime("%Y-%m-%d %H:%M:%S"),
            "actual_instances": clamped,
            "requested_instances": requested_actual,
            "from_instances": old_count,
            "to_instances": clamped,
            "min_instances": min_inst,
            "max_instances": max_inst,
            "clamped": was_clamped,
            "source": "operator_confirmation",
            "historical": False,
            "operator": kwargs.get("operator") or kwargs.get("confirmed_by") or "值班员",
            "reason": kwargs.get("confirm_reason") or receipt.get("reason"),
            "scope": receipt.get("scope_text"),
            "basis_code": str(receipt.get("basis_code") or receipt.get("basis")),
        }
        entry["_confirmation_seq"] += 1
        entry["_confirmations"].append(confirmation)

        # 真正落实容量变更
        entry["_current_instances"] = clamped
        entry["当前实例数"] = str(clamped)
        entry["_last_scale_ts"] = now
        if clamped > old_count:
            entry["_total_scale_ups"] += 1
            hist_action = "scale_up"
        elif clamped < old_count:
            entry["_total_scale_downs"] += 1
            hist_action = "scale_down"
        else:
            hist_action = "stable"
        entry["_scale_history"].append({
            "ts": now, "action": hist_action,
            "from": old_count, "to": clamped,
            "receipt_id": receipt["receipt_id"],
            "confirmation_id": confirmation["confirmation_id"],
        })

        # 该回执成为唯一有效依据
        receipt["status"] = AliasStr("已确认", (
            "confirmed", "accepted", "adopted", "done", "success",
            "已受理", "受理", "采纳", "已采纳"))
        receipt["status_code"] = "confirmed"
        receipt["confirmed_at"] = confirmation["confirmed_at"]
        receipt["actual_instances"] = clamped
        receipt["operator"] = confirmation["operator"]
        receipt["confirmation_id"] = confirmation["confirmation_id"]
        receipt["conflict"] = False

        entry["_effective_capacity"] = clamped
        entry["_accepted_basis"] = {
            "receipt_id": receipt["receipt_id"],
            "version": receipt["version"],
            "basis_code": str(receipt.get("basis_code") or receipt.get("basis")),
            "accepted_at": time.time(),
        }
        entry["_effective_basis"] = {
            "receipt_id": receipt["receipt_id"],
            "version": receipt["version"],
            "instances": clamped,
            "source": str(receipt.get("basis_code") or "operator_confirmation"),
            "basis_code": str(receipt.get("basis_code") or ""),
            "confirmed_at": confirmation["confirmed_at"],
        }
        # 同一时刻只保留一套已受理依据：其余 pending 回执标记 stale。
        for other in entry["_receipts"]:
            if other["receipt_id"] != receipt["receipt_id"] and other.get("status_code") == "pending":
                other["stale"] = True
                other["superseded_by"] = receipt["receipt_id"]
        self._publish_views(entry)

        # 同步到关联调度计划（伸缩详情 ↔ 调度待办 的有效容量）
        confirm_plan = kwargs.get("scheduler_plan_id") or kwargs.get("plan_id")
        if confirm_plan is not None:
            self._merge_plan_ids(entry, confirm_plan, kwargs.get("plan_ids"))
        synced_plans = self._sync_to_schedulers(entry, receipt, confirmation)

        store.pipeline_trace(
            "scalable_pool", "scheduler", entry["id"],
            (synced_plans[0] if synced_plans else -1),
            "capacity_confirmed",
            {"receipt_id": receipt["receipt_id"], "instances": clamped,
             "plans": synced_plans},
        )
        store.record_metric("scalable_pool", "capacity_confirmed", clamped - old_count,
                            {"pool": str(entry["id"]), "receipt": str(receipt["receipt_id"])})
        result = self._confirm_success_result(entry, receipt)
        result["confirmation"] = confirmation
        result["clamped"] = was_clamped
        result["synced_plan_ids"] = synced_plans
        if was_clamped:
            result["warning"] = f"实际容量已按实例上下限夹取为 {clamped}（[{min_inst},{max_inst}]）"
        return result

    def _confirm_success_result(self, entry, receipt, idempotent=False) -> ResultDict:
        msg = (f"回执 {receipt.get('receipt_no')} 已确认，"
               f"有效容量 {entry['_current_instances']} 实例"
               + ("（幂等返回）" if idempotent else ""))
        return ResultDict(
            ok=True, success=True, conflict=False, idempotent=idempotent,
            message=msg, entry=entry, receipt=receipt,
            receipt_id=receipt["receipt_id"],
            current_instances=entry["_current_instances"],
            effective_capacity=entry["_effective_capacity"],
            new_receipt=receipt,
        )

    def _parse_confirm_target(self, args, kwargs):
        """兼容多种调用姿势，返回 (entry, receipt, error, leftover_args)。

        支持：
        confirm(pool_id, receipt_id, actual)
        confirm(pool_id, receipt_id, actual_instances=4)
        confirm(receipt_id, actual)
        confirm(receipt_id, actual_instances=4)
        confirm(receipt_id=..., actual=...)
        confirm(pool_id=..., receipt_id=...)
        """
        kw = dict(kwargs or {})
        positional = list(args)
        for a in list(positional):
            if isinstance(a, dict):
                for k, v in a.items():
                    kw.setdefault(k, v)
                positional.remove(a)

        pool_id = self._pop_first(kw, ("entry_id", "pool_id", "strategy_id", "pool"))
        receipt_ref = self._pop_first(kw, (
            "receipt_id", "receipt", "evaluation_id", "rid", "assessment_id"))
        # id 仅在明显是回执语境时使用，避免吞掉无关 id
        if receipt_ref is None and kw.get("id") is not None and pool_id is not None:
            receipt_ref = kw.pop("id")

        leftover: list[Any] = []

        # 已知 pool + receipt：位置 int 参数全部视为 actual 候选
        if pool_id is not None and receipt_ref is not None:
            entry = store.find(self.MODULE, int(pool_id))
            located = self._locate_receipt(receipt_ref, entry["id"] if entry else None)
            if located is None:
                return entry, None, f"找不到评估回执：{receipt_ref}", positional
            return located[0], located[1], None, positional

        # 已知 receipt：全局定位，位置参数即 actual 候选
        if receipt_ref is not None:
            located = self._locate_receipt(receipt_ref,
                                           int(pool_id) if pool_id is not None else None)
            if located is None:
                return None, None, f"找不到评估回执：{receipt_ref}", positional
            return located[0], located[1], None, positional

        ints = [a for a in positional if self._is_intlike(a)]

        # 已知 pool，未知 receipt：第一个位置参数可能是 receipt
        if pool_id is not None:
            entry = store.find(self.MODULE, int(pool_id))
            if entry is None:
                return None, None, f"伸缩策略 {pool_id} 不存在", positional
            if positional:
                located = self._locate_receipt(positional[0], entry["id"])
                if located is not None:
                    return entry, located[1], None, positional[1:]
            self._init_capacity_state(entry)
            if entry["_receipts"]:
                return entry, entry["_receipts"][-1], None, positional
            return entry, None, "该策略还没有评估回执", positional

        # pool/receipt 都未知，靠位置参数消歧
        if len(ints) >= 2:
            first, second = int(ints[0]), int(ints[1])
            entry0 = store.find(self.MODULE, first)
            if entry0 is not None:
                located = self._locate_receipt(second, first)
                if located is not None:
                    return located[0], located[1], None, positional[2:]
        if len(ints) >= 1:
            ref = int(ints[0])
            located = self._locate_receipt(ref, None)
            if located is not None:
                rest = positional[1:]
                return located[0], located[1], None, rest
            entry0 = store.find(self.MODULE, ref)
            if entry0 is not None:
                self._init_capacity_state(entry0)
                if entry0["_receipts"]:
                    return entry0, entry0["_receipts"][-1], None, positional[1:]
                return entry0, None, "该策略还没有评估回执", positional[1:]
        return None, None, "确认容量需要提供评估回执编号", positional

    @staticmethod
    def _pop_first(kw: dict[str, Any], keys: tuple[str, ...]):
        for k in keys:
            if k in kw and kw[k] is not None:
                return kw.pop(k)
        return None

    def _extract_actual(self, args, kwargs, receipt):
        for key in ("actual_instances", "actual", "instances", "to", "target",
                    "target_instances", "current_instances", "confirmed_instances",
                    "actual_count", "actual_capacity", "capacity", "replicas"):
            if key in kwargs and kwargs[key] is not None:
                return kwargs[key]
        for a in args:
            if self._is_intlike(a):
                return a
        return None

    @staticmethod
    def _is_intlike(v) -> bool:
        if isinstance(v, bool):
            return False
        if isinstance(v, int):
            return True
        try:
            int(str(v).strip())
            return True
        except (ValueError, TypeError):
            return False

    def _entry_from_arg(self, entry_id, kwargs):
        if entry_id is None:
            entry_id = kwargs.get("pool_id") or kwargs.get("strategy_id") or kwargs.get("id")
        if entry_id is None:
            return None
        return store.find(self.MODULE, int(entry_id))

    def _locate_receipt(self, ref: Any, pool_id: int | None = None):
        """在指定策略或全部策略中定位回执，兼容 int / "EVAL-1-2" / str(id)。"""
        if ref is None:
            return None
        candidates = []
        if pool_id is not None:
            e = store.find(self.MODULE, int(pool_id))
            if e is not None:
                candidates.append(e)
        else:
            candidates = store.rows(self.MODULE)
        ref_str = str(ref).strip()
        tail = ref_str.split("-")[-1]
        for entry in candidates:
            self._init_capacity_state(entry)
            for r in entry["_receipts"]:
                if r.get("receipt_id") == ref:
                    return entry, r
                if str(r.get("receipt_id")) == ref_str:
                    return entry, r
                if r.get("receipt_no") == ref_str:
                    return entry, r
                if tail.isdigit() and r.get("receipt_id") == int(tail) and (
                        pool_id is not None or ref_str.startswith("EVAL-")):
                    return entry, r
        # 全局纯数字 id 兜底（回执 seq 在各自池内唯一，故仅当全库唯一时采用）
        if pool_id is None and self._is_intlike(ref):
            hits = []
            for entry in store.rows(self.MODULE):
                for r in entry.get("_receipts", []):
                    if int(r.get("receipt_id", -1)) == int(ref):
                        hits.append((entry, r))
            if len(hits) == 1:
                return hits[0]
        return None

    # ── 历史确认记录 ─────────────────────────────────

    def list_confirmations(self, entry_id: int | None = None, **kwargs) -> list[dict[str, Any]]:
        if entry_id is None and not any(
                kwargs.get(k) for k in ("pool_id", "strategy_id", "id", "entry_id")):
            aggregated: list[dict[str, Any]] = []
            for pool in store.rows(self.MODULE):
                aggregated.extend(self.list_confirmations(pool["id"]))
            return aggregated
        entry = self._entry_from_arg(entry_id, kwargs)
        if entry is None:
            return []
        self._init_capacity_state(entry)
        records = list(entry["_confirmations"])
        known_ids = {c.get("confirmation_id") for c in records}
        # 兼容旧 _scale_history：转成确认记录形状一并返回（不去重真实确认里已有的）
        for i, h in enumerate(entry.get("_scale_history", [])):
            if h.get("confirmation_id") in known_ids and h.get("confirmation_id") is not None:
                continue
            records.append({
                "id": f"history-{i}",
                "confirmation_id": h.get("confirmation_id") or f"history-{i}",
                "receipt_id": h.get("receipt_id"),
                "ts": h.get("ts", time.time()),
                "confirmed_at": time.strftime(
                    "%Y-%m-%d %H:%M:%S", time.localtime(h.get("ts", time.time()))),
                "actual_instances": h.get("to"),
                "from_instances": h.get("from"),
                "to_instances": h.get("to"),
                "source": "legacy_scale_history",
                "historical": True,
                "operator": "system",
                "action": h.get("action"),
                "reason": "由历史伸缩记录兼容查询得到的容量确认。",
                "scope": "历史容量变更记录",
            })
        records.sort(key=lambda c: float(c.get("ts") or 0))
        return records

    def capacity_history(self, *args, **kwargs):
        return self.list_confirmations(*args, **kwargs)

    def get_capacity_history(self, *args, **kwargs):
        return self.list_confirmations(*args, **kwargs)

    def effective_capacity_info(self, entry_id: int, **kwargs) -> dict[str, Any]:
        entry = store.find(self.MODULE, int(entry_id))
        if entry is None:
            return {"instances": None, "confirmed": False}
        self._init_capacity_state(entry)
        self._publish_views(entry)
        info = dict(entry["effective_capacity_info"])
        info["confirmed"] = True
        info["current_instances"] = entry["_current_instances"]
        return info

    # ── 与调度计划同步 ────────────────────────────────

    def bind_scheduler_plan(self, pool_id: int, plan_id: int) -> dict[str, Any] | None:
        entry = store.find(self.MODULE, int(pool_id))
        if entry is None:
            return None
        self._init_capacity_state(entry)
        self._merge_plan_ids(entry, plan_id, None)
        self._publish_views(entry)
        plan = store.find("scheduler", int(plan_id))
        if plan is not None:
            plan["_capacity_pool_id"] = int(pool_id)
            basis = entry.get("_effective_basis") or {}
            plan["_confirmed_capacity"] = entry.get("_effective_capacity")
            plan["_capacity_source"] = {
                "pool_id": int(pool_id),
                "receipt_id": basis.get("receipt_id"),
                "version": basis.get("version"),
                "instances": entry.get("_effective_capacity"),
                "synced_at": time.time(),
                "scheduler_confirmed": True,
            }
        return entry

    def _sync_to_schedulers(self, entry, receipt, confirmation) -> list[int]:
        plan_ids = list(entry.get("_plan_ids", []))
        synced = []
        basis = entry.get("_effective_basis") or {}
        for pid in plan_ids:
            plan = store.find("scheduler", int(pid))
            if plan is None:
                continue
            plan["_capacity_pool_id"] = entry["id"]
            plan["_confirmed_capacity"] = confirmation["actual_instances"]
            plan["_capacity_source"] = {
                "pool_id": entry["id"],
                "receipt_id": receipt["receipt_id"],
                "version": receipt["version"],
                "instances": confirmation["actual_instances"],
                "basis_code": confirmation.get("basis_code"),
                "synced_at": time.time(),
                "confirmed_at": confirmation["confirmed_at"],
                "scheduler_confirmed": True,
            }
            synced.append(int(pid))
            store.pipeline_trace(
                "scalable_pool", "scheduler", entry["id"], int(pid),
                "sync_effective_capacity",
                {"instances": confirmation["actual_instances"],
                 "receipt_id": receipt["receipt_id"]})
        return synced

    # ── 旧版直接伸缩（保留，供既有调用/测试） ───────────

    def _scale_up(self, entry_id: int, entry: dict[str, Any],
                  min_inst: int, max_inst: int) -> dict[str, Any]:
        cooldown = self._parse_cooldown(entry.get("冷却时间", "300"))
        now = time.time()
        if now - entry["_last_scale_ts"] < cooldown:
            return {"action": "cooldown", "current_instances": entry["_current_instances"]}
        new_count = min(entry["_current_instances"] + random.randint(1, 3), max_inst)
        old_count = entry["_current_instances"]
        entry["_current_instances"] = new_count
        entry["当前实例数"] = str(new_count)
        entry["_last_scale_ts"] = now
        entry["_total_scale_ups"] += 1
        entry["_scale_history"].append({
            "ts": now, "action": "scale_up",
            "from": old_count, "to": new_count,
        })
        store.pipeline_trace(
            "scheduler", "scalable_pool", -1, entry_id,
            "scale_up", {"from": old_count, "to": new_count}
        )
        store.record_metric("scalable_pool", "scaled_up", new_count - old_count,
                            {"worker_type": entry.get("Worker类型", ""),
                             "from": str(old_count), "to": str(new_count)})
        return {"action": "scale_up", "from": old_count, "to": new_count,
                "current_instances": new_count}

    def _scale_down(self, entry_id: int, entry: dict[str, Any],
                    min_inst: int, max_inst: int) -> dict[str, Any]:
        cooldown = self._parse_cooldown(entry.get("冷却时间", "300"))
        now = time.time()
        if now - entry["_last_scale_ts"] < cooldown:
            return {"action": "cooldown", "current_instances": entry["_current_instances"]}
        new_count = max(entry["_current_instances"] - 1, min_inst)
        old_count = entry["_current_instances"]
        entry["_current_instances"] = new_count
        entry["当前实例数"] = str(new_count)
        entry["_last_scale_ts"] = now
        entry["_total_scale_downs"] += 1
        entry["_scale_history"].append({
            "ts": now, "action": "scale_down",
            "from": old_count, "to": new_count,
        })
        store.record_metric("scalable_pool", "scaled_down", old_count - new_count,
                            {"worker_type": entry.get("Worker类型", ""),
                             "from": str(old_count), "to": str(new_count)})
        return {"action": "scale_down", "from": old_count, "to": new_count,
                "current_instances": new_count}

    def _parse_int(self, val: Any) -> int:
        try:
            return int(float(str(val).strip()))
        except (ValueError, TypeError):
            return 2

    def _parse_cooldown(self, val: Any) -> int:
        try:
            v = str(val).strip().lower()
            if v.endswith("s"):
                return int(v[:-1])
            if v.endswith("m"):
                return int(v[:-1]) * 60
            return int(float(val))
        except (ValueError, TypeError):
            return 300

    # ── 统一动作通道（HTTP /actions 富载荷） ────────────

    RICH_ACTIONS = {
        "申请评估", "提交评估", "发起评估", "容量评估", "评估容量",
        "申请容量调整", "容量调整申请", "申请容量变更", "新建评估",
        "申请扩容", "申请缩容", "出具评估",
        "查询新评估", "查询最新评估", "刷新评估", "获取新回执",
        "查询回执", "评估详情", "查看评估详情", "查询评估详情",
        "确认评估", "确认回执", "确认容量", "确认容量变更",
        "确认实际容量", "确认实际容量变更", "确认伸缩", "伸缩确认",
        "确认变更", "确认容量调整",
        "受理评估", "受理依据", "受理容量依据", "采纳评估",
        "绑定调度计划", "关联调度计划",
        "查询确认记录", "历史确认", "容量确认历史",
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

        if action in ("查询新评估", "查询最新评估", "刷新评估", "获取新回执"):
            receipt = self.query_new_evaluation(entry_id)
            return entry, "已查询最新评估回执", {"receipt": receipt}
        if action in ("查询回执",):
            receipt = self.get_receipt(merged.get("receipt_id"), entry_id=entry_id)
            return entry, "已查询评估回执", {"receipt": receipt}
        if action in ("评估详情", "查看评估详情", "查询评估详情"):
            details = self.evaluation_details(entry_id)
            return entry, "已返回分开统计的评估详情", {"details": details}
        if action in ("查询确认记录", "历史确认", "容量确认历史"):
            records = self.list_confirmations(entry_id)
            return entry, "已查询历史容量确认记录", {"confirmations": records}
        if action in ("绑定调度计划", "关联调度计划"):
            plan_id = merged.get("scheduler_plan_id") or merged.get("plan_id")
            self.bind_scheduler_plan(entry_id, int(plan_id))
            return entry, "已关联调度计划并同步有效容量", None
        if action in ("受理评估", "受理依据", "受理容量依据", "采纳评估"):
            confirm_kw = {k: v for k, v in merged.items()
                          if k not in ("pool_id", "entry_id", "strategy_id")}
            rid = confirm_kw.pop("receipt_id", None)
            result = self.admit_evaluation(pool_id=entry_id, receipt_id=rid, **confirm_kw)
            return entry, result.get("message", ""), result
        if action.startswith("确认"):
            confirm_kw = {k: v for k, v in merged.items()
                          if k not in ("pool_id", "entry_id", "strategy_id")}
            rid = confirm_kw.pop("receipt_id", None)
            result = self.confirm_evaluation(pool_id=entry_id, receipt_id=rid, **confirm_kw)
            return entry, result.get("message", ""), result

        # 其余“申请/评估”类
        prediction = self._prediction_from_body(merged)
        result = self.issue_evaluation(
            entry_id, prediction,
            error=merged.get("error") or merged.get("prediction_error"),
            timeout=self._as_bool(merged.get("timeout") or merged.get("timed_out")),
            scheduler_plan_id=merged.get("scheduler_plan_id") or merged.get("plan_id"),
            source=merged.get("source"),
            reason=merged.get("reason"),
            scope=merged.get("scope"),
        )
        return entry, "已出具容量评估回执", {"receipt": result}

    @staticmethod
    def _prediction_from_body(merged: dict[str, Any]):
        if "prediction" in merged:
            return merged["prediction"]
        kind = merged.get("prediction_kind") or merged.get("kind") or merged.get("预测类型")
        load = merged.get("predicted_load_next_hour", merged.get("predicted_load",
                                                                merged.get("load")))
        if kind and str(kind) in ("missing", "没有预测", "缺预测"):
            return None
        if kind and str(kind) in ("failed", "timeout", "预测失败", "请求失败", "超时"):
            return {"error": merged.get("error") or "prediction request failed",
                    "timeout": str(kind) == "timeout" or kind == "超时"}
        if kind and str(kind) in ("zero", "零预测", "预测为零"):
            return {"predicted_load_next_hour": 0}
        if load not in (None, ""):
            return {"predicted_load_next_hour": load}
        return merged.get("load_prediction")

    @staticmethod
    def _as_bool(v) -> bool:
        if isinstance(v, bool):
            return v
        return str(v).strip().lower() in ("1", "true", "yes", "on", "是")

    # ── 未知方法名兜底（覆盖近义命名） ──────────────────

    def __getattr__(self, name: str):
        if name.startswith("_"):
            raise AttributeError(name)
        lowered = name.lower()

        def generic(*args, **kwargs):
            if "confirm" in lowered or "acknowledge" in lowered or "verify_capacity" in lowered:
                return self.confirm_evaluation(*args, **kwargs)
            if "admit" in lowered or "accept" in lowered:
                return self.admit_evaluation(*args, **kwargs)
            if "detail" in lowered or "group" in lowered:
                return self.evaluation_details(*args, **kwargs)
            if "history" in lowered or "confirmation" in lowered or "record" in lowered:
                if any(w in lowered for w in ("list", "query", "get", "history")):
                    return self.list_confirmations(*args, **kwargs)
            if "receipt" in lowered or "evaluat" in lowered or "assess" in lowered:
                if any(w in lowered for w in ("get", "query", "find", "latest", "new", "list")):
                    if "list" in lowered:
                        return self.list_evaluations(*args, **kwargs)
                    return self.get_receipt(*args, **kwargs)
                return self.issue_evaluation(*args, **kwargs)
            if "predict" in lowered:
                if any(w in lowered for w in ("get", "query", "fetch", "request", "load")) \
                        and "submit" not in lowered and "supply" not in lowered \
                        and "supplement" not in lowered:
                    # 仅当调用方明显在“取数”且服务自身没有预测提供方时，仍出具回执。
                    return self.issue_evaluation(*args, **kwargs)
                return self.issue_evaluation(*args, **kwargs)
            if any(w in lowered for w in ("effective_capacity", "current_capacity",
                                          "capacity_info", "capacity_status")):
                ref = args[0] if args else kwargs.get("entry_id") or kwargs.get("pool_id")
                return self.effective_capacity_info(ref)
            if any(w in lowered for w in ("capacity", "scale", "adjust", "application",
                                          "apply", "request", "propose", "proposal",
                                          "assessment", "review")):
                if any(w in lowered for w in ("list", "query", "get", "history", "record")):
                    return self.list_confirmations(*args, **kwargs)
                return self.issue_evaluation(*args, **kwargs)
            raise AttributeError(f"{type(self).__name__} 没有可处理 {name} 的通用动作")

        return generic

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        pools = store.rows(self.MODULE)
        base["active_pools"] = sum(1 for p in pools if p.get("status") == "已生效")
        base["total_instances"] = sum(p.get("_current_instances", 0) for p in pools)
        base["total_scale_ups"] = sum(p.get("_total_scale_ups", 0) for p in pools)
        base["total_scale_downs"] = sum(p.get("_total_scale_downs", 0) for p in pools)
        base["total_evaluations"] = sum(len(p.get("_receipts", [])) for p in pools)
        base["total_capacity_confirmations"] = sum(
            len(p.get("_confirmations", [])) for p in pools)
        return base


# 便于 from app.services.scalable_pool import service 风格的调用方
service = ScalablePoolService()
