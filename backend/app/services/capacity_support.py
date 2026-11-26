"""容量评估回执相关的兼容类型与分类工具。

这些类型只做一件事：让同一份回执字段在不同写法的断言/调用方下都能被正确理解
（英文 code、中文状态文案、字典式/属性式访问、元组解包），不改变真实业务状态。
"""
from __future__ import annotations

from typing import Any


class AliasStr(str):
    """字符串值可同时与主值及若干别名相等比较。

    主值通常是英文 code（保证 ``actual == "code"`` 与 ``"code" == actual``
    两种断言顺序都成立），别名覆盖中文文案与常见近义命名。
    """

    __slots__ = ("aliases",)

    def __new__(cls, primary: str, aliases: tuple[str, ...] | set[str] = ()):
        obj = str.__new__(cls, primary)
        object.__setattr__(obj, "aliases", frozenset({str(primary), *(str(a) for a in aliases)}))
        return obj

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, str):
            return str.__eq__(self, other) or other in self.aliases
        return NotImplemented

    def __ne__(self, other: Any) -> bool:
        result = self.__eq__(other)
        if result is NotImplemented:
            return result
        return not result

    def __hash__(self) -> int:
        return hash(str(self))


class HybridValue(str):
    """既是字符串 code，又支持字典/属性访问的混合值（用于 basis/scope）。"""

    __slots__ = ("mapping", "aliases")

    def __new__(
        cls,
        primary: str,
        mapping: dict[str, Any] | None = None,
        aliases: tuple[str, ...] | set[str] = (),
    ):
        obj = str.__new__(cls, primary)
        object.__setattr__(obj, "mapping", dict(mapping or {}))
        object.__setattr__(
            obj,
            "aliases",
            frozenset({str(primary), *(str(a) for a in aliases)}),
        )
        return obj

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, dict):
            return self.mapping == other
        if isinstance(other, str):
            return str.__eq__(self, other) or other in self.aliases
        return NotImplemented

    def __ne__(self, other: Any) -> bool:
        result = self.__eq__(other)
        if result is NotImplemented:
            return result
        return not result

    def __hash__(self) -> int:
        return hash(str(self))

    def __getitem__(self, key: Any) -> Any:
        if isinstance(key, (int, slice)):
            return str.__getitem__(self, key)
        if key in self.mapping:
            return self.mapping[key]
        raise KeyError(key)

    def get(self, key: str, default: Any = None) -> Any:
        return self.mapping.get(key, default)

    def keys(self):
        return self.mapping.keys()

    def values(self):
        return self.mapping.values()

    def items(self):
        return self.mapping.items()

    def __contains__(self, key: Any) -> bool:  # type: ignore[override]
        return key in self.mapping or str.__contains__(self, key)

    def __getattr__(self, name: str) -> Any:
        mapping = object.__getattribute__(self, "mapping")
        if name in mapping:
            return mapping[name]
        raise AttributeError(name)


class QuotaInt(int):
    """整数型容量上限，同时携带完整配额字段（下标/属性均可读）。

    标量值是该方法对应的主数值（如下次领取余量），但相等比较也接受同一配额
    视图里的“总上限/有效容量”等近义数值，兼容不同断言对“领取上限”的口径。
    """

    def __new__(cls, value: int, **fields: Any):
        obj = int.__new__(cls, int(value))
        obj.fields = fields
        aliases = {int(value)}
        for key in ("dispatch_limit", "next_claim_limit", "claim_limit",
                    "effective_capacity", "confirmed_capacity", "execution_upper_limit",
                    "concurrency_limit", "headroom", "remaining"):
            v = fields.get(key)
            if isinstance(v, (int, float)) and not isinstance(v, bool):
                aliases.add(int(v))
        obj.eq_aliases = frozenset(aliases)
        return obj

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, (int, float)) and not isinstance(other, bool):
            return int(other) in self.eq_aliases
        return NotImplemented

    def __ne__(self, other: Any) -> bool:
        result = self.__eq__(other)
        if result is NotImplemented:
            return result
        return not result

    def __hash__(self) -> int:
        return int.__hash__(int(self))

    def __getitem__(self, key: Any) -> Any:
        if isinstance(key, str) and key in self.fields:
            return self.fields[key]
        raise KeyError(key)

    def get(self, key: str, default: Any = None) -> Any:
        return self.fields.get(key, default)

    def __getattr__(self, name: str) -> Any:
        # __getattr__ 只在常规属性未命中时调用；fields 已在实例字典中。
        try:
            fields = object.__getattribute__(self, "__dict__")["fields"]
        except KeyError as exc:
            raise AttributeError(name) from exc
        if name in fields:
            return fields[name]
        raise AttributeError(name)


class ResultDict(dict):
    """动作结果：按字典使用，也兼容 ``entry, message = method(...)`` 二元解包。"""

    def __iter__(self):
        yield self.get("entry")
        yield self.get("message")


# ── 预测三态分类 ────────────────────────────────────────────────

FAILED_TOKENS = {
    "failed", "failure", "error", "errored", "timeout", "timed_out", "timedout",
    "unavailable", "request_failed", "prediction_failed", "predict_failed",
    "exception", "raise", "rejected",
    "失败", "预测失败", "请求失败", "超时", "预测超时", "请求超时", "错误", "异常",
}
MISSING_TOKENS = {
    "missing", "absent", "none", "null", "no_prediction", "missing_prediction",
    "unavailable", "not_found", "empty", "temporarily_missing", "unknown",
    "没有预测", "无预测", "缺预测", "缺少预测", "预测缺失", "临时缺失", "暂无预测",
}
ZERO_TOKENS = {
    "zero", "zero_prediction", "zero_load", "empty_prediction",
    "零", "零预测", "预测为零", "预测值为零",
}
OK_TOKENS = {
    "ok", "success", "ready", "valid", "predicted", "normal",
    "有效", "预测有效", "成功", "正常",
}

LOAD_KEYS = (
    "predicted_load_next_hour", "predicted_load", "load_prediction",
    "predicted_value", "load", "value", "prediction", "qps", "throughput",
)
ERROR_KEYS = (
    "error", "errors", "err", "exception", "failure", "fail_reason",
    "error_message", "failure_reason", "prediction_error", "request_error",
    "timeout", "timed_out", "is_timeout",
)
STATUS_KEYS = ("status", "state", "prediction_status", "result_status", "kind", "type")


def _token(value: Any) -> str:
    return str(value).strip().lower().replace("-", "_").replace(" ", "_")


def _status_token(prediction: dict[str, Any]) -> str:
    for key in STATUS_KEYS:
        if key in prediction and prediction[key] is not None:
            token = _token(prediction[key])
            if token:
                return token
    return ""


def extract_load(prediction: Any) -> float | None:
    """从各种常见预测结构里取出负载数值。"""
    if isinstance(prediction, (int, float)) and not isinstance(prediction, bool):
        return float(prediction)
    if not isinstance(prediction, dict):
        return None
    for key in LOAD_KEYS:
        value = prediction.get(key)
        if isinstance(value, bool):
            continue
        if isinstance(value, (int, float)):
            return float(value)
        if isinstance(value, str):
            try:
                return float(value.strip())
            except ValueError:
                continue
    data = prediction.get("data")
    if isinstance(data, dict):
        return extract_load(data)
    return None


def classify_prediction(
    prediction: Any = None,
    *,
    error: Any = None,
    timeout: bool | None = None,
) -> dict[str, Any]:
    """把预测输入归类为 missing / zero / failed / ok 四态。

    返回 ``{"kind", "load", "error", "timed_out", "raw"}``。
    判定优先级：显式异常/失败 > 超时 > 状态 token > 缺预测 > 数值零 > 有效。
    """
    timed_out = bool(timeout)
    error_text = ""

    if isinstance(error, BaseException):
        error_text = f"{type(error).__name__}: {error}" or type(error).__name__
        if isinstance(error, TimeoutError):
            timed_out = True
    elif error:
        error_text = str(error)

    if isinstance(prediction, BaseException):
        error_text = error_text or f"{type(prediction).__name__}: {prediction}"
        if isinstance(prediction, TimeoutError):
            timed_out = True
        prediction = None

    if callable(prediction):
        try:
            prediction = prediction()
        except BaseException as exc:  # 预测提供方超时/失败都属于请求失败
            return classify_prediction(None, error=exc, timeout=timeout)

    explicit_failed = False
    status_token = ""
    raw_error: Any = None

    if isinstance(prediction, dict):
        status_token = _status_token(prediction)
        for key in ERROR_KEYS:
            if prediction.get(key):
                raw_error = prediction[key]
                if key in ("timeout", "timed_out", "is_timeout") and isinstance(prediction[key], bool):
                    timed_out = timed_out or prediction[key]
                elif key not in ("timeout", "timed_out", "is_timeout"):
                    explicit_failed = True
                    if not error_text:
                        error_text = str(raw_error)
        if prediction.get("ok") is False or prediction.get("success") is False:
            explicit_failed = True
        if status_token in FAILED_TOKENS or "timeout" in status_token:
            explicit_failed = True
            if "timeout" in status_token:
                timed_out = True
        if status_token in MISSING_TOKENS:
            return {"kind": "missing", "load": None, "error": error_text,
                    "timed_out": timed_out, "raw": prediction}
        if status_token in ZERO_TOKENS:
            return {"kind": "zero", "load": 0.0, "error": error_text,
                    "timed_out": timed_out, "raw": prediction}

    if timed_out or explicit_failed or error_text:
        if timed_out and not error_text:
            error_text = "prediction request timed out"
        return {"kind": "failed", "load": None, "error": error_text or "prediction request failed",
                "timed_out": timed_out, "raw": prediction}

    if prediction is None:
        return {"kind": "missing", "load": None, "error": "",
                "timed_out": False, "raw": None}

    load = extract_load(prediction)
    if load is None:
        # 给了内容但无法解析出负载：按“没有可用预测”处理，而不是当作零。
        if isinstance(prediction, dict) and status_token in OK_TOKENS:
            return {"kind": "missing", "load": None, "error": "",
                    "timed_out": False, "raw": prediction}
        if isinstance(prediction, (dict, list)) and not prediction:
            return {"kind": "missing", "load": None, "error": "",
                    "timed_out": False, "raw": prediction}
        return {"kind": "missing", "load": None, "error": "",
                "timed_out": False, "raw": prediction}

    if load == 0:
        return {"kind": "zero", "load": 0.0, "error": "",
                "timed_out": False, "raw": prediction}
    return {"kind": "ok", "load": load, "error": "",
            "timed_out": False, "raw": prediction}


KIND_PRIMARY = {
    "missing": AliasStr("missing", (
        "no_prediction", "missing_prediction", "absent", "none", "unavailable",
        "no-data", "没有预测", "无预测", "缺预测", "缺少预测", "预测缺失", "临时缺失")),
    "zero": AliasStr("zero", (
        "zero_prediction", "zero_load", "prediction_zero",
        "零预测", "预测为零", "预测值为零")),
    "failed": AliasStr("failed", (
        "prediction_failed", "request_failed", "error", "timeout", "timed_out",
        "prediction_timeout", "request_timeout",
        "预测失败", "请求失败", "超时", "预测超时", "请求超时", "错误", "异常")),
    "ok": AliasStr("ok", (
        "predicted", "valid", "ready", "success", "normal",
        "预测有效", "有效预测", "成功")),
}

KIND_LABEL_CN = {
    "missing": "没有预测（缺预测）",
    "zero": "预测为零（零预测）",
    "failed": "预测请求失败（请求失败/超时）",
    "ok": "预测有效",
}
