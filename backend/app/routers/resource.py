"""统一资源接口：查询、导出、登记和动作按模块 key 分派到领域服务。"""
from __future__ import annotations

import csv
import io
from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import StreamingResponse

from app.schemas import ActionPayload, ActionResult, EntryPayload, PageResult
from app.services import service_for

router = APIRouter(prefix="/api", tags=["业务资源"])


def require_service(module: str):
    service = service_for(module)
    if service is None:
        raise HTTPException(status_code=404, detail=f"未知业务模块：{module}")
    return service


@router.get("/{module}", response_model=PageResult[dict])
def list_entries(
    module: str,
    request: Request,
    keyword: str | None = Query(default=None),
    status: str | None = Query(default=None),
    page: int = Query(default=1, ge=1),
    size: int = Query(default=20, ge=1, le=200),
) -> PageResult[dict]:
    service = require_service(module)
    filters = {
        key: value.strip()
        for key, value in request.query_params.items()
        if key in service.LIST_FIELDS and value.strip()
    }
    items, total = service.list_entries(
        filters=filters, keyword=keyword, status=status, page=page, size=size)
    return PageResult(items=items, total=total, page=page, size=size)


@router.get("/{module}/export")
def export_entries(
    module: str,
    request: Request,
    keyword: str | None = Query(default=None),
    status: str | None = Query(default=None),
) -> StreamingResponse:
    service = require_service(module)
    filters = {
        key: value.strip()
        for key, value in request.query_params.items()
        if key in service.LIST_FIELDS and value.strip()
    }
    items, _ = service.list_entries(
        filters=filters, keyword=keyword, status=status, page=1, size=1_000_000)
    output = io.StringIO()
    output.write("\ufeff")
    writer = csv.writer(output, lineterminator="\n")
    writer.writerow(["id", *service.LIST_FIELDS, "status"])
    for item in items:
        values = [item.get("id"), *(item.get(field, "") for field in service.LIST_FIELDS),
                  item.get("status", "")]
        writer.writerow([
            "'" + str(value) if str(value or "").startswith(("=", "+", "-", "@")) else value
            for value in values
        ])
    return StreamingResponse(
        iter([output.getvalue()]), media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{module}.csv"'},
    )


@router.get("/{module}/{entry_id}", response_model=dict)
def get_entry(module: str, entry_id: int) -> dict[str, Any]:
    service = require_service(module)
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"{service.ENTITY} {entry_id} 不存在或已归档")
    return entry


@router.post("/{module}", response_model=ActionResult)
def create_entry(module: str, payload: EntryPayload) -> ActionResult:
    service = require_service(module)
    entry, missing = service.create_entry(payload.values)
    if missing:
        return ActionResult(ok=False, message=f"缺少必填字段：{'、'.join(missing)}")
    return ActionResult(ok=True, message=f"{service.ENTITY}已登记", entry=entry)


@router.post("/{module}/{entry_id}/actions", response_model=ActionResult)
def run_action(module: str, entry_id: int, payload: ActionPayload) -> ActionResult:
    service = require_service(module)
    body = payload.model_dump(mode="python")
    body["action"] = payload.action.strip()
    outcome = service.handle_action(entry_id, payload.action.strip(), body)
    # 兼容只返回二元组的旧服务
    if len(outcome) == 2:
        entry, message = outcome
        extra = None
    else:
        entry, message, extra = outcome
    ok = entry is not None
    if isinstance(extra, dict) and extra.get("ok") is False:
        ok = False
    return ActionResult(ok=ok, message=message, entry=entry,
                        result=extra if isinstance(extra, dict) else None,
                        data=extra if isinstance(extra, dict) else None)


# ── 容量评估回执 / 派发配额 的只读子资源（路径均为 int 前缀，不与 {entry_id} 冲突） ──

@router.get("/scalable_pool/{entry_id}/receipts")
def list_receipts(entry_id: int) -> dict[str, Any]:
    service = require_service("scalable_pool")
    return {"items": service.list_evaluations(entry_id), "total": len(service.list_evaluations(entry_id))}


@router.get("/scalable_pool/{entry_id}/evaluations")
def list_evaluations(entry_id: int) -> dict[str, Any]:
    service = require_service("scalable_pool")
    details = service.evaluation_details(entry_id)
    return details


@router.get("/scalable_pool/{entry_id}/evaluation-details")
def evaluation_details(entry_id: int) -> dict[str, Any]:
    service = require_service("scalable_pool")
    return service.evaluation_details(entry_id)


@router.get("/scalable_pool/{entry_id}/receipts/{receipt_id}")
def get_receipt(entry_id: int, receipt_id: str) -> dict[str, Any]:
    service = require_service("scalable_pool")
    receipt = service.get_receipt(receipt_id, entry_id=entry_id)
    if receipt is None:
        raise HTTPException(status_code=404, detail=f"评估回执 {receipt_id} 不存在")
    return receipt


@router.get("/scalable_pool/{entry_id}/confirmations")
def list_confirmations(entry_id: int) -> dict[str, Any]:
    service = require_service("scalable_pool")
    items = service.list_confirmations(entry_id)
    return {"items": items, "total": len(items)}


@router.get("/scalable_pool/{entry_id}/effective-capacity")
def effective_capacity(entry_id: int) -> dict[str, Any]:
    service = require_service("scalable_pool")
    return service.effective_capacity_info(entry_id)


@router.get("/scheduler/{entry_id}/dispatch-quota")
def dispatch_quota(entry_id: int) -> dict[str, Any]:
    service = require_service("scheduler")
    return service.dispatch_quota(entry_id)


@router.get("/scheduler/{entry_id}/todos")
def scheduler_todos(entry_id: int) -> dict[str, Any]:
    service = require_service("scheduler")
    items = service.scheduler_todos(entry_id)
    return {"items": items, "total": len(items)}
