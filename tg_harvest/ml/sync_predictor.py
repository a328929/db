from __future__ import annotations

import json
import math
import os
import sqlite3
import threading
import zlib
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from tg_harvest.domain.coerce import enabled_int, optional_int
from tg_harvest.runtime.paths import runtime_dir
from tg_harvest.storage.connection import synchronized_write

MODEL_KEY = "temporal_batch_predictor"
MODEL_VERSION = "temporal-batch-predictor-v3-lite"
UTC_TEXT_FORMAT = "%Y-%m-%d %H:%M:%S"
SEQUENCE_LENGTH = 32
DELAY_BUCKET_SECONDS = (15, 30, 45, 60, 120, 180, 300, 600, 1200, 1800, 3600, 7200)
SEQUENCE_NUMERIC_DIM = 10
STATIC_FEATURE_DIM = 32
CHAT_HASH_BUCKETS = 32768
CHAT_HASH_EMBED_DIM = 16
EVENT_EMBED_DIM = 8
REASON_EMBED_DIM = 12
ACCOUNT_EMBED_DIM = 8
STATUS_EMBED_DIM = 8
GRU_HIDDEN_DIM = 64
GRU_LAYER_COUNT = 1
STATIC_HIDDEN_DIM = 128
FUSION_HIDDEN_DIM = 64
FUSION_OUTPUT_DIM = 64
_MODEL_LOCK = threading.RLock()
_MODEL_CACHE_LOCK = threading.RLock()
_MODEL_CACHE: dict[tuple[str, int, str], tuple[Any, Any]] = {}

EVENT_TYPE_IDS = {
    "observation": 1,
    "probe": 2,
    "update_outcome": 3,
}
STATUS_IDS = {
    "pending": 1,
    "success": 2,
    "failed": 3,
    "changed": 4,
    "unchanged": 5,
    "backoff": 6,
}
ACCOUNT_IDS = {
    "primary": 1,
    "secondary": 2,
}
MEMBERSHIP_SCOPES = (
    "unknown",
    "none_joined",
    "both_joined",
    "single_joined_primary",
    "single_joined_secondary",
    "unobservable",
)
STATE_STATUSES = (
    "idle",
    "pending",
    "updating",
    "backoff",
    "quarantined",
    "unobservable",
    "deleted",
)


@dataclass(frozen=True)
class EncodedSample:
    seq_numeric: list[list[float]]
    seq_event_type_ids: list[int]
    seq_reason_ids: list[int]
    seq_account_ids: list[int]
    seq_status_ids: list[int]
    static_features: list[float]
    chat_hash_id: int
    delay_bucket_index: int
    added_log_target: float
    efficiency_target: float
    risk_target: float
    priority_target: float


@dataclass(frozen=True)
class ModelSuggestion:
    available: bool
    active: bool
    mode: str
    backend: str
    model_version: str
    quiet_delay_seconds: int
    bucket_confidence: float
    bucket_probabilities: list[float]
    expected_added_message_count: float
    api_efficiency: float
    risk_score: float
    priority_score: float
    reason: str = ""

    def to_prediction_dict(self) -> dict[str, Any]:
        return {
            "available": self.available,
            "active": self.active,
            "mode": self.mode,
            "backend": self.backend,
            "model_version": self.model_version,
            "quiet_delay_seconds": self.quiet_delay_seconds,
            "bucket_confidence": round(self.bucket_confidence, 6),
            "bucket_probabilities": [
                round(float(value), 6) for value in self.bucket_probabilities
            ],
            "expected_added_message_count": round(
                self.expected_added_message_count, 6
            ),
            "api_efficiency": round(self.api_efficiency, 6),
            "risk_score": round(self.risk_score, 6),
            "priority_score": round(self.priority_score, 6),
            "reason": self.reason,
        }


def default_artifact_path() -> str:
    return str(runtime_dir() / "models" / "sync_predictor.pt")


def _utc_now_text() -> str:
    return datetime.now(UTC).replace(microsecond=0).strftime(UTC_TEXT_FORMAT)


def _parse_utc_text(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.strptime(text, UTC_TEXT_FORMAT).replace(tzinfo=UTC)
    except ValueError:
        return None


def _json_dumps(value: Any) -> str:
    try:
        return json.dumps(value, ensure_ascii=False, sort_keys=True)
    except Exception:
        return "{}"


def _decode_json_dict(value: Any) -> dict[str, Any]:
    try:
        parsed = json.loads(str(value or "{}"))
    except Exception:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _row_value(row: Any, key: str, default: Any = "") -> Any:
    if row is None:
        return default
    try:
        value = row[key]
    except Exception:
        value = default
    return default if value is None else value


def _row_int(row: Any, key: str, default: int = 0) -> int:
    return int(optional_int(_row_value(row, key, default)) or default)


def _row_float(row: Any, key: str, default: float = 0.0) -> float:
    try:
        return float(_row_value(row, key, default) or default)
    except (TypeError, ValueError):
        return default


def _row_text(row: Any, key: str, default: str = "") -> str:
    return str(_row_value(row, key, default) or default).strip()


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(float(value), high))


def _scaled_log(value: Any, divisor: float) -> float:
    try:
        number = max(0.0, float(value or 0.0))
    except (TypeError, ValueError):
        number = 0.0
    return _clamp(math.log1p(number) / divisor, 0.0, 1.0)


def _scaled_linear(value: Any, divisor: float, *, high: float = 1.0) -> float:
    try:
        number = max(0.0, float(value or 0.0))
    except (TypeError, ValueError):
        number = 0.0
    return _clamp(number / divisor, 0.0, high)


def _stable_bucket_id(value: Any, modulo: int, *, offset: int = 0) -> int:
    text = str(value or "").strip().lower()
    if not text:
        return 0
    return (zlib.crc32(text.encode("utf-8")) % max(1, modulo)) + offset


def _event_type_id(value: Any) -> int:
    return EVENT_TYPE_IDS.get(
        str(value or "").strip().lower(),
        _stable_bucket_id(value, 6, offset=1),
    )


def _reason_id(value: Any) -> int:
    return _stable_bucket_id(value, 15, offset=1)


def _account_id(value: Any) -> int:
    return ACCOUNT_IDS.get(
        str(value or "").strip().lower(),
        _stable_bucket_id(value, 3, offset=1),
    )


def _status_id(value: Any) -> int:
    return STATUS_IDS.get(
        str(value or "").strip().lower(),
        _stable_bucket_id(value, 7, offset=1),
    )


def _chat_hash_id(chat_id: Any) -> int:
    return _stable_bucket_id(f"chat:{int(optional_int(chat_id) or 0)}", CHAT_HASH_BUCKETS)


def _one_hot(value: str, choices: tuple[str, ...]) -> list[float]:
    normalized = str(value or "").strip()
    return [1.0 if normalized == choice else 0.0 for choice in choices]


def _nearest_delay_bucket_index(seconds: Any) -> int:
    try:
        value = max(0.0, float(seconds or 0.0))
    except (TypeError, ValueError):
        value = 0.0
    distances = [abs(float(bucket) - value) for bucket in DELAY_BUCKET_SECONDS]
    return int(min(range(len(distances)), key=distances.__getitem__))


def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT 1
            FROM sqlite_master
            WHERE type = 'table' AND name = ?
            LIMIT 1
            """,
            (str(table_name or "").strip(),),
        )
        return cur.fetchone() is not None
    finally:
        cur.close()


def _load_torch() -> tuple[Any | None, Any | None, str]:
    try:
        import torch
        from torch import nn

        return torch, nn, ""
    except Exception as exc:
        return None, None, f"{type(exc).__name__}: {exc}"


def _make_model_class(torch: Any, nn: Any):
    class TemporalBatchPredictor(nn.Module):
        def __init__(self) -> None:
            super().__init__()
            self.event_embedding = nn.Embedding(8, EVENT_EMBED_DIM)
            self.reason_embedding = nn.Embedding(17, REASON_EMBED_DIM)
            self.account_embedding = nn.Embedding(5, ACCOUNT_EMBED_DIM)
            self.status_embedding = nn.Embedding(9, STATUS_EMBED_DIM)
            self.chat_embedding = nn.Embedding(CHAT_HASH_BUCKETS + 1, CHAT_HASH_EMBED_DIM)
            seq_input_dim = (
                SEQUENCE_NUMERIC_DIM
                + EVENT_EMBED_DIM
                + REASON_EMBED_DIM
                + ACCOUNT_EMBED_DIM
                + STATUS_EMBED_DIM
            )
            self.sequence_encoder = nn.GRU(
                input_size=seq_input_dim,
                hidden_size=GRU_HIDDEN_DIM,
                num_layers=GRU_LAYER_COUNT,
                batch_first=True,
                dropout=0.0,
                bidirectional=False,
            )
            encoded_dim = GRU_HIDDEN_DIM
            self.attention = nn.Sequential(
                nn.Linear(encoded_dim, 64),
                nn.Tanh(),
                nn.Linear(64, 1),
            )
            self.static_encoder = nn.Sequential(
                nn.Linear(STATIC_FEATURE_DIM + CHAT_HASH_EMBED_DIM, STATIC_HIDDEN_DIM),
                nn.GELU(),
                nn.LayerNorm(STATIC_HIDDEN_DIM),
                nn.Dropout(0.08),
                nn.Linear(STATIC_HIDDEN_DIM, STATIC_HIDDEN_DIM),
                nn.GELU(),
            )
            self.fusion = nn.Sequential(
                nn.Linear(encoded_dim * 2 + STATIC_HIDDEN_DIM, FUSION_HIDDEN_DIM),
                nn.GELU(),
                nn.LayerNorm(FUSION_HIDDEN_DIM),
                nn.Dropout(0.08),
                nn.Linear(FUSION_HIDDEN_DIM, FUSION_OUTPUT_DIM),
                nn.GELU(),
            )
            self.delay_head = nn.Linear(FUSION_OUTPUT_DIM, len(DELAY_BUCKET_SECONDS))
            self.added_head = nn.Linear(FUSION_OUTPUT_DIM, 1)
            self.efficiency_head = nn.Linear(FUSION_OUTPUT_DIM, 1)
            self.risk_head = nn.Linear(FUSION_OUTPUT_DIM, 1)
            self.priority_head = nn.Linear(FUSION_OUTPUT_DIM, 1)

        def forward(
            self,
            seq_numeric: Any,
            seq_event_type_ids: Any,
            seq_reason_ids: Any,
            seq_account_ids: Any,
            seq_status_ids: Any,
            static_features: Any,
            chat_hash_ids: Any,
        ) -> dict[str, Any]:
            seq_parts = [
                seq_numeric,
                self.event_embedding(seq_event_type_ids.clamp(min=0, max=7)),
                self.reason_embedding(seq_reason_ids.clamp(min=0, max=16)),
                self.account_embedding(seq_account_ids.clamp(min=0, max=4)),
                self.status_embedding(seq_status_ids.clamp(min=0, max=8)),
            ]
            seq_input = torch.cat(seq_parts, dim=-1)
            encoded_sequence, _hidden = self.sequence_encoder(seq_input)
            attention_logits = self.attention(encoded_sequence).squeeze(-1)
            attention_weights = torch.softmax(attention_logits, dim=1).unsqueeze(-1)
            attention_vector = torch.sum(encoded_sequence * attention_weights, dim=1)
            last_vector = encoded_sequence[:, -1, :]
            chat_vector = self.chat_embedding(
                chat_hash_ids.clamp(min=0, max=CHAT_HASH_BUCKETS)
            )
            static_vector = self.static_encoder(
                torch.cat([static_features, chat_vector], dim=-1)
            )
            fused = self.fusion(
                torch.cat([attention_vector, last_vector, static_vector], dim=-1)
            )
            return {
                "delay_logits": self.delay_head(fused),
                "added_log": self.added_head(fused).squeeze(-1),
                "efficiency": self.efficiency_head(fused).squeeze(-1),
                "risk_logit": self.risk_head(fused).squeeze(-1),
                "priority": self.priority_head(fused).squeeze(-1),
            }

    return TemporalBatchPredictor


def _model_state_from_db(conn: sqlite3.Connection) -> dict[str, Any]:
    if not _table_exists(conn, "sync_model_state"):
        return {}
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT model_version, backend, metrics_json, trained_at, artifact_path, state_json, updated_at
            FROM sync_model_state
            WHERE model_key = ?
            LIMIT 1
            """,
            (MODEL_KEY,),
        )
        row = cur.fetchone()
        if row is None:
            return {}
        return {
            "model_version": _row_text(row, "model_version"),
            "backend": _row_text(row, "backend"),
            "metrics": _decode_json_dict(_row_text(row, "metrics_json", "{}")),
            "trained_at": _row_text(row, "trained_at"),
            "artifact_path": _row_text(row, "artifact_path"),
            "state": _decode_json_dict(_row_text(row, "state_json", "{}")),
            "updated_at": _row_text(row, "updated_at"),
        }
    finally:
        cur.close()


@synchronized_write
def _write_model_state(
    conn: sqlite3.Connection,
    *,
    backend: str,
    metrics: dict[str, Any],
    state: dict[str, Any],
    artifact_path: str = "",
    trained_at: str = "",
    now_text: str | None = None,
) -> None:
    now = str(now_text or _utc_now_text())
    cur = conn.cursor()
    try:
        cur.execute("BEGIN IMMEDIATE")
        cur.execute(
            """
            INSERT INTO sync_model_state(
                model_key,
                model_version,
                backend,
                metrics_json,
                trained_at,
                artifact_path,
                state_json,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(model_key) DO UPDATE SET
                model_version = excluded.model_version,
                backend = excluded.backend,
                metrics_json = excluded.metrics_json,
                trained_at = excluded.trained_at,
                artifact_path = excluded.artifact_path,
                state_json = excluded.state_json,
                updated_at = excluded.updated_at
            """,
            (
                MODEL_KEY,
                MODEL_VERSION,
                str(backend or ""),
                _json_dumps(metrics),
                str(trained_at or ""),
                str(artifact_path or ""),
                _json_dumps(state),
                now,
            ),
        )
        conn.commit()
    except Exception:
        with suppress(Exception):
            conn.rollback()
        raise
    finally:
        cur.close()


def _count_outcome_samples(conn: sqlite3.Connection) -> int:
    if not _table_exists(conn, "sync_learning_events"):
        return 0
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT COUNT(*) AS c
            FROM sync_learning_events
            WHERE event_type = 'update_outcome'
            """
        )
        return _row_int(cur.fetchone(), "c")
    finally:
        cur.close()


def _recent_learning_rows(
    conn: sqlite3.Connection,
    *,
    chat_id: int,
    cutoff_event_id: int | None = None,
    cutoff_created_at: str = "",
) -> list[Any]:
    cur = conn.cursor()
    try:
        if cutoff_event_id is not None and cutoff_event_id > 0:
            cur.execute(
                """
                SELECT *
                FROM sync_learning_events
                WHERE chat_id = ?
                  AND id <= ?
                ORDER BY id DESC
                LIMIT ?
                """,
                (int(chat_id), int(cutoff_event_id), SEQUENCE_LENGTH),
            )
        else:
            cur.execute(
                """
                SELECT *
                FROM sync_learning_events
                WHERE chat_id = ?
                  AND created_at <= ?
                ORDER BY created_at DESC, id DESC
                LIMIT ?
                """,
                (int(chat_id), str(cutoff_created_at or _utc_now_text()), SEQUENCE_LENGTH),
            )
        return list(reversed(cur.fetchall()))
    finally:
        cur.close()


def _state_row(conn: sqlite3.Connection, chat_id: int) -> Any | None:
    if not _table_exists(conn, "sync_chat_state"):
        return None
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT s.*, p.event_count, p.in_flight, p.due_at, p.quiet_delay_seconds
            FROM sync_chat_state s
            LEFT JOIN sync_pending_updates p ON p.chat_id = s.chat_id
            WHERE s.chat_id = ?
            LIMIT 1
            """,
            (int(chat_id),),
        )
        return cur.fetchone()
    finally:
        cur.close()


def _encode_event_rows(
    rows: list[Any],
    *,
    synthetic_event: dict[str, Any] | None = None,
) -> tuple[list[list[float]], list[int], list[int], list[int], list[int]]:
    normalized_rows: list[dict[str, Any]] = []
    for row in rows:
        normalized_rows.append(
            {
                "event_type": _row_text(row, "event_type"),
                "reason": _row_text(row, "reason"),
                "source_account": _row_text(row, "source_account"),
                "status": _row_text(row, "status"),
                "quiet_delay_seconds": _row_int(row, "quiet_delay_seconds"),
                "priority_score": _row_float(row, "priority_score"),
                "added_message_count": _row_int(row, "added_message_count"),
                "wait_seconds": _row_int(row, "wait_seconds"),
                "api_cost": _row_float(row, "api_cost"),
                "failure_type": _row_text(row, "failure_type"),
                "created_at": _row_text(row, "created_at"),
                "features": _decode_json_dict(_row_text(row, "features_json", "{}")),
                "prediction": _decode_json_dict(_row_text(row, "prediction_json", "{}")),
                "outcome": _decode_json_dict(_row_text(row, "outcome_json", "{}")),
            }
        )
    if synthetic_event:
        normalized_rows.append(dict(synthetic_event))

    normalized_rows = normalized_rows[-SEQUENCE_LENGTH:]
    seq_numeric: list[list[float]] = []
    event_ids: list[int] = []
    reason_ids: list[int] = []
    account_ids: list[int] = []
    status_ids: list[int] = []
    previous_dt: datetime | None = None

    for item in normalized_rows:
        created_at = str(item.get("created_at") or "")
        created_dt = _parse_utc_text(created_at)
        delta_seconds = 0
        if created_dt is not None and previous_dt is not None:
            delta_seconds = max(0, int((created_dt - previous_dt).total_seconds()))
        if created_dt is not None:
            previous_dt = created_dt

        features = item.get("features") if isinstance(item.get("features"), dict) else {}
        prediction = (
            item.get("prediction") if isinstance(item.get("prediction"), dict) else {}
        )
        outcome = item.get("outcome") if isinstance(item.get("outcome"), dict) else {}
        event_count = (
            features.get("event_count")
            or outcome.get("event_count")
            or prediction.get("event_count")
            or 0
        )
        local_gap = prediction.get("local_gap") or features.get("local_gap") or 0
        failure_type = str(item.get("failure_type") or "")
        status = str(item.get("status") or "")

        seq_numeric.append(
            [
                _scaled_log(delta_seconds, 8.0),
                _scaled_linear(item.get("quiet_delay_seconds"), 7200.0),
                _scaled_linear(item.get("priority_score"), 300.0),
                _scaled_log(item.get("added_message_count"), 8.0),
                _scaled_linear(item.get("wait_seconds"), 7200.0),
                _scaled_linear(item.get("api_cost"), 1000.0),
                1.0 if failure_type else 0.0,
                1.0 if status in {"failed", "backoff"} else 0.0,
                _scaled_linear(event_count, 50.0),
                _scaled_linear(local_gap, 5000.0),
            ]
        )
        event_ids.append(_event_type_id(item.get("event_type")))
        reason_ids.append(_reason_id(item.get("reason")))
        account_ids.append(_account_id(item.get("source_account")))
        status_ids.append(_status_id(status))

    missing = SEQUENCE_LENGTH - len(seq_numeric)
    if missing > 0:
        seq_numeric = [[0.0] * SEQUENCE_NUMERIC_DIM for _ in range(missing)] + seq_numeric
        event_ids = [0] * missing + event_ids
        reason_ids = [0] * missing + reason_ids
        account_ids = [0] * missing + account_ids
        status_ids = [0] * missing + status_ids

    return seq_numeric, event_ids, reason_ids, account_ids, status_ids


def _build_static_features(
    state_row: Any | None,
    *,
    now_text: str,
    heuristic_context: dict[str, Any] | None = None,
) -> list[float]:
    context = heuristic_context or {}
    state_snapshot = context.get("state_snapshot")
    if not isinstance(state_snapshot, dict):
        state_snapshot = {}

    def state_value(key: str, default: Any = "") -> Any:
        if state_row is not None:
            return _row_value(state_row, key, default)
        return state_snapshot.get(key, default)

    membership_scope = _row_text(
        state_row,
        "membership_scope",
        str(state_snapshot.get("membership_scope") or context.get("membership_scope") or "unknown"),
    )
    status = str(state_value("status", context.get("status") or "") or "").strip()
    remote_last_id = int(optional_int(state_value("remote_last_id", 0)) or 0)
    local_last_id = int(optional_int(state_value("local_last_id", 0)) or 0)
    local_gap = max(0, remote_last_id - local_last_id)
    failure_count = int(optional_int(state_value("failure_count", 0)) or 0)
    pending_snapshot = context.get("pending_snapshot")
    if not isinstance(pending_snapshot, dict):
        pending_snapshot = {}
    pending_event_count = int(
        optional_int(state_value("event_count", pending_snapshot.get("event_count", 0)))
        or 0
    )
    in_flight = int(
        optional_int(state_value("in_flight", pending_snapshot.get("in_flight", 0)))
        or 0
    )
    quiet_delay = int(
        optional_int(
            state_value("quiet_delay_seconds", pending_snapshot.get("quiet_delay_seconds", 0))
        )
        or 0
    )
    try:
        priority = float(state_value("priority_score", pending_snapshot.get("priority_score", 0.0)) or 0.0)
    except (TypeError, ValueError):
        priority = 0.0
    model_delay = int(optional_int(state_value("model_delay_seconds", 0)) or 0)
    unavailable_count = int(optional_int(state_value("unavailable_count", 0)) or 0)

    now_dt = _parse_utc_text(now_text) or datetime.now(UTC)
    hour_angle = 2.0 * math.pi * (now_dt.hour / 24.0)
    weekday_angle = 2.0 * math.pi * (now_dt.weekday() / 7.0)
    last_event_dt = _parse_utc_text(state_value("last_event_at", ""))
    last_success_dt = _parse_utc_text(state_value("last_success_at", ""))
    last_failure_dt = _parse_utc_text(state_value("last_failure_at", ""))

    def age_feature(value: datetime | None, divisor: float) -> float:
        if value is None:
            return 1.0
        return _scaled_linear(max(0, int((now_dt - value).total_seconds())), divisor)

    features = [
        *_one_hot(membership_scope, MEMBERSHIP_SCOPES),
        *_one_hot(status, STATE_STATUSES),
        math.sin(hour_angle),
        math.cos(hour_angle),
        math.sin(weekday_angle),
        math.cos(weekday_angle),
        _scaled_linear(local_gap, 5000.0),
        _scaled_linear(failure_count, 10.0),
        _scaled_linear(unavailable_count, 10.0),
        _scaled_linear(pending_event_count, 50.0),
        float(1 if in_flight else 0),
        _scaled_linear(quiet_delay, 7200.0),
        _scaled_linear(model_delay, 7200.0),
        _scaled_linear(priority, 300.0),
        age_feature(last_event_dt, 24 * 60 * 60),
        age_feature(last_success_dt, 7 * 24 * 60 * 60),
        age_feature(last_failure_dt, 7 * 24 * 60 * 60),
    ]
    if len(features) < STATIC_FEATURE_DIM:
        features.extend([0.0] * (STATIC_FEATURE_DIM - len(features)))
    return features[:STATIC_FEATURE_DIM]


def _encoded_input_for_chat(
    conn: sqlite3.Connection,
    *,
    chat_id: int,
    now_text: str,
    cutoff_event_id: int | None = None,
    synthetic_event: dict[str, Any] | None = None,
    heuristic_context: dict[str, Any] | None = None,
    use_current_state: bool = True,
) -> dict[str, Any]:
    rows = _recent_learning_rows(
        conn,
        chat_id=int(chat_id),
        cutoff_event_id=cutoff_event_id,
        cutoff_created_at=now_text,
    )
    seq_numeric, event_ids, reason_ids, account_ids, status_ids = _encode_event_rows(
        rows,
        synthetic_event=synthetic_event,
    )
    return {
        "seq_numeric": seq_numeric,
        "seq_event_type_ids": event_ids,
        "seq_reason_ids": reason_ids,
        "seq_account_ids": account_ids,
        "seq_status_ids": status_ids,
        "chat_hash_id": _chat_hash_id(chat_id),
        "static_features": _build_static_features(
            _state_row(conn, int(chat_id)) if use_current_state else None,
            now_text=now_text,
            heuristic_context=heuristic_context,
        ),
    }


def _sample_from_outcome_row(conn: sqlite3.Connection, row: Any) -> EncodedSample | None:
    chat_id = _row_int(row, "chat_id")
    if chat_id <= 0:
        return None
    features = _decode_json_dict(_row_text(row, "features_json", "{}"))
    outcome = _decode_json_dict(_row_text(row, "outcome_json", "{}"))
    wait_seconds = _row_int(row, "wait_seconds")
    recorded_delay = _row_int(row, "quiet_delay_seconds")
    added_count = max(0, _row_int(row, "added_message_count"))
    api_cost = max(1.0, _row_float(row, "api_cost", 1.0))
    efficiency = _clamp(float(added_count) / api_cost, 0.0, 10.0) / 10.0
    failed = bool(_row_text(row, "failure_type"))
    risk = 1.0 if failed else 0.0
    quiet_delay = wait_seconds if wait_seconds > 0 else recorded_delay
    if quiet_delay <= 0:
        quiet_delay = recorded_delay
    if failed:
        quiet_delay = min(7200, max(quiet_delay, recorded_delay * 2, 60))
    elif added_count <= 0 and wait_seconds > 0:
        quiet_delay = min(7200, max(quiet_delay, recorded_delay, wait_seconds))
    elif added_count >= 5 and wait_seconds > 0:
        quiet_delay = max(15, min(quiet_delay, recorded_delay or wait_seconds))
    features.setdefault("outcome_snapshot", outcome)
    encoded = _encoded_input_for_chat(
        conn,
        chat_id=chat_id,
        now_text=_row_text(row, "created_at"),
        cutoff_event_id=_row_int(row, "id"),
        heuristic_context=features,
        use_current_state=False,
    )
    return EncodedSample(
        seq_numeric=encoded["seq_numeric"],
        seq_event_type_ids=encoded["seq_event_type_ids"],
        seq_reason_ids=encoded["seq_reason_ids"],
        seq_account_ids=encoded["seq_account_ids"],
        seq_status_ids=encoded["seq_status_ids"],
        static_features=encoded["static_features"],
        chat_hash_id=encoded["chat_hash_id"],
        delay_bucket_index=_nearest_delay_bucket_index(quiet_delay),
        added_log_target=math.log1p(float(added_count)),
        efficiency_target=efficiency,
        risk_target=risk,
        priority_target=_scaled_linear(_row_float(row, "priority_score"), 300.0),
    )


def _load_training_samples(conn: sqlite3.Connection, *, max_samples: int) -> list[EncodedSample]:
    if not _table_exists(conn, "sync_learning_events"):
        return []
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT *
            FROM sync_learning_events
            WHERE event_type = 'update_outcome'
            ORDER BY id DESC
            LIMIT ?
            """,
            (max(1, int(max_samples or 1)),),
        )
        rows = list(reversed(cur.fetchall()))
    finally:
        cur.close()

    samples: list[EncodedSample] = []
    for row in rows:
        sample = _sample_from_outcome_row(conn, row)
        if sample is not None:
            samples.append(sample)
    return samples


def _model_kind(cfg: Any) -> str:
    value = str(getattr(cfg, "sync_model_kind", "torch_lite") or "torch_lite").strip()
    return value if value in {"torch_lite", "torch"} else "torch_lite"


def _min_new_outcomes_for_training(cfg: Any, min_samples: int) -> int:
    configured = getattr(cfg, "sync_model_min_new_outcomes", None)
    if configured is not None:
        return _cfg_int(cfg, "sync_model_min_new_outcomes", 1, minimum=1)
    return max(10, min(200, max(1, int(min_samples) // 5)))


def _atomic_torch_save(torch: Any, artifact: dict[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(f"{path.name}.tmp.{os.getpid()}.{threading.get_ident()}")
    try:
        torch.save(artifact, tmp_path)
        tmp_path.replace(path)
    except Exception:
        with suppress(FileNotFoundError):
            tmp_path.unlink()
        raise


def invalidate_model_cache(artifact_path: str | None = None) -> None:
    with _MODEL_CACHE_LOCK:
        if artifact_path is None:
            _MODEL_CACHE.clear()
            return
        normalized = str(Path(artifact_path))
        stale_keys = [key for key in _MODEL_CACHE if key[0] == normalized]
        for key in stale_keys:
            _MODEL_CACHE.pop(key, None)


def _cfg_int(cfg: Any, name: str, default: int, *, minimum: int = 0) -> int:
    return max(int(minimum), int(getattr(cfg, name, default) or default))


def _cfg_float(cfg: Any, name: str, default: float, *, minimum: float = 0.0) -> float:
    try:
        value = float(getattr(cfg, name, default) or default)
    except (TypeError, ValueError):
        value = default
    return max(float(minimum), value)


def _cfg_enabled(cfg: Any, name: str, default: int = 0) -> bool:
    return enabled_int(getattr(cfg, name, default)) == 1


def _samples_to_tensors(torch: Any, samples: list[EncodedSample]) -> dict[str, Any]:
    return {
        "seq_numeric": torch.tensor(
            [sample.seq_numeric for sample in samples], dtype=torch.float32
        ),
        "seq_event_type_ids": torch.tensor(
            [sample.seq_event_type_ids for sample in samples], dtype=torch.long
        ),
        "seq_reason_ids": torch.tensor(
            [sample.seq_reason_ids for sample in samples], dtype=torch.long
        ),
        "seq_account_ids": torch.tensor(
            [sample.seq_account_ids for sample in samples], dtype=torch.long
        ),
        "seq_status_ids": torch.tensor(
            [sample.seq_status_ids for sample in samples], dtype=torch.long
        ),
        "static_features": torch.tensor(
            [sample.static_features for sample in samples], dtype=torch.float32
        ),
        "chat_hash_ids": torch.tensor(
            [sample.chat_hash_id for sample in samples], dtype=torch.long
        ),
        "delay_bucket_index": torch.tensor(
            [sample.delay_bucket_index for sample in samples], dtype=torch.long
        ),
        "added_log_target": torch.tensor(
            [sample.added_log_target for sample in samples], dtype=torch.float32
        ),
        "efficiency_target": torch.tensor(
            [sample.efficiency_target for sample in samples], dtype=torch.float32
        ),
        "risk_target": torch.tensor(
            [sample.risk_target for sample in samples], dtype=torch.float32
        ),
        "priority_target": torch.tensor(
            [sample.priority_target for sample in samples], dtype=torch.float32
        ),
    }


def _load_compatible_model_state(model: Any, raw_state: Any) -> bool:
    if not isinstance(raw_state, dict):
        return False
    current_state = model.state_dict()
    compatible_state = {
        key: value
        for key, value in raw_state.items()
        if key in current_state
        and getattr(value, "shape", None) == getattr(current_state[key], "shape", None)
    }
    if not compatible_state:
        return False
    model.load_state_dict(compatible_state, strict=False)
    return True


def _slice_tensors(tensors: dict[str, Any], indices: Any) -> dict[str, Any]:
    return {key: value[indices] for key, value in tensors.items()}


def _forward_loss(torch: Any, outputs: dict[str, Any], batch: dict[str, Any]) -> Any:
    cross_entropy = torch.nn.functional.cross_entropy(
        outputs["delay_logits"],
        batch["delay_bucket_index"],
    )
    added_loss = torch.nn.functional.smooth_l1_loss(
        outputs["added_log"],
        batch["added_log_target"],
    )
    efficiency_loss = torch.nn.functional.smooth_l1_loss(
        torch.sigmoid(outputs["efficiency"]),
        batch["efficiency_target"],
    )
    risk_loss = torch.nn.functional.binary_cross_entropy_with_logits(
        outputs["risk_logit"],
        batch["risk_target"],
    )
    priority_loss = torch.nn.functional.smooth_l1_loss(
        torch.sigmoid(outputs["priority"]),
        batch["priority_target"],
    )
    return (
        cross_entropy
        + added_loss
        + 0.5 * efficiency_loss
        + 0.75 * risk_loss
        + 0.25 * priority_loss
    )


def _evaluate(torch: Any, model: Any, tensors: dict[str, Any]) -> dict[str, float]:
    if int(tensors["delay_bucket_index"].shape[0]) <= 0:
        return {}
    with torch.no_grad():
        outputs = model(
            tensors["seq_numeric"],
            tensors["seq_event_type_ids"],
            tensors["seq_reason_ids"],
            tensors["seq_account_ids"],
            tensors["seq_status_ids"],
            tensors["static_features"],
            tensors["chat_hash_ids"],
        )
        loss = _forward_loss(torch, outputs, tensors)
        predicted_bucket = torch.argmax(outputs["delay_logits"], dim=1)
        delay_accuracy = (
            (predicted_bucket == tensors["delay_bucket_index"]).float().mean().item()
        )
        delay_mae_buckets = (
            (predicted_bucket - tensors["delay_bucket_index"]).abs().float().mean().item()
        )
        added_mae_log = (
            (outputs["added_log"] - tensors["added_log_target"]).abs().mean().item()
        )
        risk_probs = torch.sigmoid(outputs["risk_logit"])
        risk_mae = (risk_probs - tensors["risk_target"]).abs().mean().item()
    return {
        "loss": round(float(loss.item()), 6),
        "delay_accuracy": round(float(delay_accuracy), 6),
        "delay_mae_buckets": round(float(delay_mae_buckets), 6),
        "added_mae_log": round(float(added_mae_log), 6),
        "risk_mae": round(float(risk_mae), 6),
    }


def _build_readiness_state(
    *,
    cfg: Any,
    previous_state: dict[str, Any],
    sample_count: int,
    validation_count: int,
    metrics: dict[str, Any],
) -> dict[str, Any]:
    min_samples = _cfg_int(cfg, "sync_model_min_samples", 200, minimum=1)
    min_eval_samples = _cfg_int(
        cfg,
        "sync_model_min_eval_samples",
        max(20, min_samples // 5),
        minimum=1,
    )
    accuracy_threshold = _cfg_float(
        cfg,
        "sync_model_ready_delay_accuracy",
        0.45,
        minimum=0.0,
    )
    max_mae_buckets = _cfg_float(
        cfg,
        "sync_model_ready_max_delay_mae_buckets",
        1.25,
        minimum=0.0,
    )
    max_added_mae_log = _cfg_float(
        cfg,
        "sync_model_ready_max_added_mae_log",
        1.50,
        minimum=0.0,
    )
    required_runs = _cfg_int(
        cfg,
        "sync_model_ready_consecutive_runs",
        3,
        minimum=1,
    )
    ready = (
        sample_count >= min_samples
        and validation_count >= min_eval_samples
        and float(metrics.get("validation_delay_accuracy") or 0.0)
        >= accuracy_threshold
        and float(metrics.get("validation_delay_mae_buckets") or 99.0)
        <= max_mae_buckets
        and float(metrics.get("validation_added_mae_log") or 99.0)
        <= max_added_mae_log
    )
    prior_consecutive = int(previous_state.get("consecutive_ready_count") or 0)
    consecutive_ready_count = prior_consecutive + 1 if ready else 0
    auto_promote = _cfg_enabled(cfg, "sync_ai_auto_promote_enabled", 0)
    shadow_enabled = _cfg_enabled(cfg, "sync_ai_shadow", 1)
    previous_mode = str(previous_state.get("mode") or "shadow")
    mode = "active" if previous_mode == "active" else "shadow"
    if shadow_enabled:
        mode = "shadow"
    elif auto_promote and consecutive_ready_count >= required_runs:
        mode = "active"
    return {
        "mode": mode,
        "ready": ready,
        "auto_promote_enabled": auto_promote,
        "shadow_enabled": shadow_enabled,
        "consecutive_ready_count": consecutive_ready_count,
        "required_consecutive_ready_count": required_runs,
        "readiness": {
            "sample_count": sample_count,
            "min_samples": min_samples,
            "validation_count": validation_count,
            "min_eval_samples": min_eval_samples,
            "delay_accuracy_threshold": accuracy_threshold,
            "max_delay_mae_buckets": max_mae_buckets,
            "max_added_mae_log": max_added_mae_log,
        },
    }


def train_sync_model(conn: sqlite3.Connection, cfg: Any) -> dict[str, Any]:
    if not _cfg_enabled(cfg, "sync_ai_enabled", 0):
        return {"ok": True, "trained": False, "backend": "disabled"}
    if not _table_exists(conn, "sync_model_state"):
        return {"ok": False, "trained": False, "backend": "schema_missing"}
    if _model_kind(cfg) != "torch_lite":
        return {
            "ok": True,
            "trained": False,
            "backend": "unsupported_model_kind",
            "model_kind": _model_kind(cfg),
        }

    total_outcomes = _count_outcome_samples(conn)
    min_samples = _cfg_int(cfg, "sync_model_min_samples", 200, minimum=1)
    previous = _model_state_from_db(conn)
    previous_state = previous.get("state") or {}
    artifact_path = str(previous.get("artifact_path") or default_artifact_path())

    torch, nn, import_error = _load_torch()
    if torch is None or nn is None:
        state = {
            "mode": "shadow",
            "ready": False,
            "sample_count": total_outcomes,
            "min_samples": min_samples,
            "reason": "torch_unavailable",
            "error": import_error,
        }
        _write_model_state(
            conn,
            backend="torch_unavailable",
            metrics={"sample_count": total_outcomes},
            state=state,
            artifact_path=artifact_path,
        )
        return {
            "ok": True,
            "trained": False,
            "backend": "torch_unavailable",
            "sample_count": total_outcomes,
            "error": import_error,
        }

    if total_outcomes < min_samples:
        state = {
            "mode": str(previous_state.get("mode") or "shadow"),
            "ready": False,
            "sample_count": total_outcomes,
            "last_trained_outcome_count": int(previous_state.get("last_trained_outcome_count") or 0),
            "min_samples": min_samples,
            "reason": "waiting_for_samples",
        }
        _write_model_state(
            conn,
            backend="waiting_for_samples",
            metrics={"sample_count": total_outcomes},
            state=state,
            artifact_path=artifact_path,
        )
        return {
            "ok": True,
            "trained": False,
            "backend": "waiting_for_samples",
            "sample_count": total_outcomes,
            "min_samples": min_samples,
        }

    last_trained_outcomes = int(previous_state.get("last_trained_outcome_count") or 0)
    min_new_outcomes = _min_new_outcomes_for_training(cfg, min_samples)
    if last_trained_outcomes > 0 and total_outcomes - last_trained_outcomes < min_new_outcomes:
        state = dict(previous_state)
        state.update(
            {
                "ready": bool(previous_state.get("ready")),
                "sample_count": total_outcomes,
                "last_trained_outcome_count": last_trained_outcomes,
                "min_new_outcomes": min_new_outcomes,
                "reason": "waiting_for_new_outcomes",
            }
        )
        if _cfg_enabled(cfg, "sync_ai_shadow", 1):
            state["mode"] = "shadow"
        _write_model_state(
            conn,
            backend=str(previous.get("backend") or "waiting_for_new_outcomes"),
            metrics=dict(previous.get("metrics") or {}) | {"sample_count": total_outcomes},
            state=state,
            artifact_path=artifact_path,
            trained_at=str(previous.get("trained_at") or ""),
        )
        return {
            "ok": True,
            "trained": False,
            "backend": "waiting_for_new_outcomes",
            "sample_count": total_outcomes,
            "last_trained_outcome_count": last_trained_outcomes,
            "min_new_outcomes": min_new_outcomes,
        }

    max_samples = _cfg_int(cfg, "sync_model_max_train_samples", 4096, minimum=min_samples)
    samples = _load_training_samples(conn, max_samples=max_samples)
    if len(samples) < min_samples:
        return {
            "ok": True,
            "trained": False,
            "backend": "waiting_for_usable_samples",
            "sample_count": len(samples),
            "min_samples": min_samples,
        }

    with _MODEL_LOCK:
        torch.set_num_threads(max(1, _cfg_int(cfg, "sync_model_torch_threads", 1, minimum=1)))
        model_cls = _make_model_class(torch, nn)
        model = model_cls()
        previous_artifact = Path(artifact_path)
        if previous_artifact.exists():
            with suppress(Exception):
                try:
                    checkpoint = torch.load(
                        previous_artifact,
                        map_location="cpu",
                        weights_only=False,
                    )
                except TypeError:
                    checkpoint = torch.load(previous_artifact, map_location="cpu")
                if isinstance(checkpoint, dict) and checkpoint.get("model_state"):
                    _load_compatible_model_state(model, checkpoint["model_state"])

        split_at = max(1, int(len(samples) * 0.8))
        if split_at >= len(samples):
            split_at = max(1, len(samples) - 1)
        train_samples = samples[:split_at]
        validation_samples = samples[split_at:] or samples[-1:]
        train_tensors = _samples_to_tensors(torch, train_samples)
        validation_tensors = _samples_to_tensors(torch, validation_samples)
        optimizer = torch.optim.AdamW(
            model.parameters(),
            lr=_cfg_float(cfg, "sync_model_learning_rate", 0.001, minimum=0.00001),
            weight_decay=0.01,
        )
        epochs = _cfg_int(cfg, "sync_model_train_epochs", 3, minimum=1)
        batch_size = _cfg_int(cfg, "sync_model_train_batch_size", 64, minimum=4)
        sample_count = int(train_tensors["delay_bucket_index"].shape[0])
        last_loss = 0.0

        model.train()
        for _epoch in range(epochs):
            permutation = torch.randperm(sample_count)
            for start in range(0, sample_count, batch_size):
                indices = permutation[start : start + batch_size]
                batch = _slice_tensors(train_tensors, indices)
                optimizer.zero_grad(set_to_none=True)
                outputs = model(
                    batch["seq_numeric"],
                    batch["seq_event_type_ids"],
                    batch["seq_reason_ids"],
                    batch["seq_account_ids"],
                    batch["seq_status_ids"],
                    batch["static_features"],
                    batch["chat_hash_ids"],
                )
                loss = _forward_loss(torch, outputs, batch)
                loss.backward()
                torch.nn.utils.clip_grad_norm_(model.parameters(), max_norm=1.0)
                optimizer.step()
                last_loss = float(loss.item())

        model.eval()
        validation_metrics = _evaluate(torch, model, validation_tensors)
        train_metrics = _evaluate(torch, model, train_tensors)
        metrics = {
            "sample_count": len(samples),
            "outcome_sample_count": total_outcomes,
            "train_sample_count": len(train_samples),
            "validation_sample_count": len(validation_samples),
            "train_loss": round(last_loss, 6),
            "train_eval_loss": train_metrics.get("loss", 0.0),
            "validation_loss": validation_metrics.get("loss", 0.0),
            "validation_delay_accuracy": validation_metrics.get("delay_accuracy", 0.0),
            "validation_delay_mae_buckets": validation_metrics.get(
                "delay_mae_buckets",
                99.0,
            ),
            "validation_added_mae_log": validation_metrics.get("added_mae_log", 99.0),
            "validation_risk_mae": validation_metrics.get("risk_mae", 1.0),
        }
        readiness_state = _build_readiness_state(
            cfg=cfg,
            previous_state=previous_state,
            sample_count=len(samples),
            validation_count=len(validation_samples),
            metrics=metrics,
        )
        trained_at = _utc_now_text()
        artifact = {
            "model_key": MODEL_KEY,
            "model_version": MODEL_VERSION,
            "model_kind": _model_kind(cfg),
            "model_state": model.state_dict(),
            "delay_buckets": list(DELAY_BUCKET_SECONDS),
            "sequence_length": SEQUENCE_LENGTH,
            "sequence_numeric_dim": SEQUENCE_NUMERIC_DIM,
            "static_feature_dim": STATIC_FEATURE_DIM,
            "chat_hash_buckets": CHAT_HASH_BUCKETS,
            "chat_hash_embed_dim": CHAT_HASH_EMBED_DIM,
            "event_embed_dim": EVENT_EMBED_DIM,
            "reason_embed_dim": REASON_EMBED_DIM,
            "account_embed_dim": ACCOUNT_EMBED_DIM,
            "status_embed_dim": STATUS_EMBED_DIM,
            "gru_hidden_dim": GRU_HIDDEN_DIM,
            "gru_layer_count": GRU_LAYER_COUNT,
            "static_hidden_dim": STATIC_HIDDEN_DIM,
            "fusion_hidden_dim": FUSION_HIDDEN_DIM,
            "fusion_output_dim": FUSION_OUTPUT_DIM,
            "metrics": metrics,
            "state": readiness_state,
            "trained_at": trained_at,
        }
        path = Path(artifact_path)
        _atomic_torch_save(torch, artifact, path)
        invalidate_model_cache(str(path))
    backend = "torch_active" if readiness_state["mode"] == "active" else "torch_shadow"
    _write_model_state(
        conn,
        backend=backend,
        metrics=metrics,
        state=readiness_state
        | {
            "trained_sample_count": len(samples),
            "last_trained_outcome_count": total_outcomes,
            "min_new_outcomes": min_new_outcomes,
            "model_kind": _model_kind(cfg),
        },
        artifact_path=str(path),
        trained_at=trained_at,
    )
    return {
        "ok": True,
        "trained": True,
        "backend": backend,
        "mode": readiness_state["mode"],
        "sample_count": len(samples),
        "metrics": metrics,
    }


def _load_checkpoint_model(conn: sqlite3.Connection) -> tuple[Any, Any, dict[str, Any], str]:
    model_row = _model_state_from_db(conn)
    if not model_row:
        return None, None, {}, "model_state_missing"
    artifact_path = str(model_row.get("artifact_path") or default_artifact_path())
    path = Path(artifact_path)
    if not artifact_path or not path.exists():
        return None, None, model_row, "artifact_missing"
    torch, nn, import_error = _load_torch()
    if torch is None or nn is None:
        return None, None, model_row, f"torch_unavailable: {import_error}"
    try:
        mtime_ns = int(path.stat().st_mtime_ns)
    except OSError:
        return None, None, model_row, "artifact_missing"
    cache_key = (
        str(path),
        mtime_ns,
        str(model_row.get("model_version") or MODEL_VERSION),
    )
    with _MODEL_CACHE_LOCK:
        cached = _MODEL_CACHE.get(cache_key)
        if cached is not None:
            return cached[0], cached[1], model_row, ""

    with _MODEL_LOCK:
        with _MODEL_CACHE_LOCK:
            cached = _MODEL_CACHE.get(cache_key)
            if cached is not None:
                return cached[0], cached[1], model_row, ""
        try:
            checkpoint = torch.load(
                path,
                map_location="cpu",
                weights_only=False,
            )
        except TypeError:
            checkpoint = torch.load(path, map_location="cpu")
        if not isinstance(checkpoint, dict) or not checkpoint.get("model_state"):
            return None, None, model_row, "invalid_artifact"
        if str(checkpoint.get("model_version") or "") != MODEL_VERSION:
            return None, None, model_row, "incompatible_artifact_version"
        if str(checkpoint.get("model_kind") or "torch_lite") != "torch_lite":
            return None, None, model_row, "incompatible_model_kind"
        model_cls = _make_model_class(torch, nn)
        model = model_cls()
        if not _load_compatible_model_state(model, checkpoint["model_state"]):
            return None, None, model_row, "incompatible_artifact_shape"
        model.eval()
        with _MODEL_CACHE_LOCK:
            stale_keys = [key for key in _MODEL_CACHE if key[0] == str(path)]
            for key in stale_keys:
                _MODEL_CACHE.pop(key, None)
            _MODEL_CACHE[cache_key] = (torch, model)
        return torch, model, model_row, ""


def predict_sync_decision(
    conn: sqlite3.Connection,
    cfg: Any,
    *,
    chat_id: int,
    now_text: str,
    observation_reason: str,
    source_account: str,
    heuristic_delay_seconds: int,
    heuristic_priority_score: float,
    heuristic_context: dict[str, Any] | None = None,
) -> ModelSuggestion:
    if not _cfg_enabled(cfg, "sync_ai_enabled", 0):
        return ModelSuggestion(
            available=False,
            active=False,
            mode="disabled",
            backend="disabled",
            model_version=MODEL_VERSION,
            quiet_delay_seconds=int(heuristic_delay_seconds),
            bucket_confidence=0.0,
            bucket_probabilities=[],
            expected_added_message_count=0.0,
            api_efficiency=0.0,
            risk_score=0.0,
            priority_score=float(heuristic_priority_score),
            reason="ai_disabled",
        )

    torch, model, model_row, unavailable_reason = _load_checkpoint_model(conn)
    if torch is None or model is None:
        return ModelSuggestion(
            available=False,
            active=False,
            mode=str((model_row.get("state") or {}).get("mode") or "shadow")
            if model_row
            else "shadow",
            backend=str(model_row.get("backend") or "missing") if model_row else "missing",
            model_version=MODEL_VERSION,
            quiet_delay_seconds=int(heuristic_delay_seconds),
            bucket_confidence=0.0,
            bucket_probabilities=[],
            expected_added_message_count=0.0,
            api_efficiency=0.0,
            risk_score=0.0,
            priority_score=float(heuristic_priority_score),
            reason=unavailable_reason,
        )

    synthetic_event = {
        "event_type": "observation",
        "reason": str(observation_reason or "event"),
        "source_account": str(source_account or ""),
        "status": "pending",
        "quiet_delay_seconds": int(heuristic_delay_seconds or 0),
        "priority_score": float(heuristic_priority_score or 0.0),
        "added_message_count": 0,
        "wait_seconds": 0,
        "api_cost": 0.0,
        "failure_type": "",
        "created_at": now_text,
        "features": heuristic_context or {},
        "prediction": heuristic_context or {},
        "outcome": {},
    }
    encoded = _encoded_input_for_chat(
        conn,
        chat_id=int(chat_id),
        now_text=now_text,
        synthetic_event=synthetic_event,
        heuristic_context=heuristic_context,
    )
    with torch.no_grad():
        outputs = model(
            torch.tensor([encoded["seq_numeric"]], dtype=torch.float32),
            torch.tensor([encoded["seq_event_type_ids"]], dtype=torch.long),
            torch.tensor([encoded["seq_reason_ids"]], dtype=torch.long),
            torch.tensor([encoded["seq_account_ids"]], dtype=torch.long),
            torch.tensor([encoded["seq_status_ids"]], dtype=torch.long),
            torch.tensor([encoded["static_features"]], dtype=torch.float32),
            torch.tensor([encoded["chat_hash_id"]], dtype=torch.long),
        )
        probs = torch.softmax(outputs["delay_logits"], dim=1)[0]
        bucket_index = int(torch.argmax(probs).item())
        confidence = float(probs[bucket_index].item())
        expected_added = max(0.0, math.expm1(float(outputs["added_log"][0].item())))
        efficiency = float(torch.sigmoid(outputs["efficiency"])[0].item())
        risk = float(torch.sigmoid(outputs["risk_logit"])[0].item())
        priority_fraction = float(torch.sigmoid(outputs["priority"])[0].item())

    state = model_row.get("state") or {}
    mode = str(state.get("mode") or "shadow")
    auto_promote_enabled = _cfg_enabled(cfg, "sync_ai_auto_promote_enabled", 0)
    shadow_enabled = _cfg_enabled(cfg, "sync_ai_shadow", 1)
    min_confidence = _cfg_float(
        cfg,
        "sync_model_min_confidence",
        0.35,
        minimum=0.0,
    )
    context = heuristic_context or {}
    membership_scope = str(context.get("membership_scope") or "")
    suggested_delay = int(DELAY_BUCKET_SECONDS[bucket_index])
    reason = "shadow"
    active = False
    if shadow_enabled:
        reason = "shadow_forced"
    elif mode != "active":
        reason = "model_not_active"
    elif not auto_promote_enabled:
        reason = "auto_promote_disabled"
    elif confidence < min_confidence:
        reason = "low_confidence"
    elif membership_scope == "unobservable":
        reason = "unobservable"
    elif risk >= 0.55 and suggested_delay < int(heuristic_delay_seconds or 0):
        reason = "risk_blocks_shorter_delay"
    else:
        active = True
        reason = "active"
    priority = max(
        0.0,
        float(heuristic_priority_score or 0.0)
        + expected_added * 3.0
        + priority_fraction * 40.0
        - risk * 60.0,
    )
    return ModelSuggestion(
        available=True,
        active=active,
        mode=mode,
        backend=str(model_row.get("backend") or ""),
        model_version=str(model_row.get("model_version") or MODEL_VERSION),
        quiet_delay_seconds=suggested_delay,
        bucket_confidence=confidence,
        bucket_probabilities=[float(value) for value in probs.tolist()],
        expected_added_message_count=expected_added,
        api_efficiency=efficiency,
        risk_score=risk,
        priority_score=priority,
        reason=reason,
    )


def reset_artifact(path: str | None = None) -> bool:
    artifact = Path(path or default_artifact_path())
    with suppress(FileNotFoundError):
        artifact.unlink()
        invalidate_model_cache(str(artifact))
        return True
    return False
