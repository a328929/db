from __future__ import annotations

import json
import re
import urllib.error
import urllib.request
from dataclasses import dataclass
from typing import Any

_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class ManticoreError(RuntimeError):
    pass


def validate_table_name(value: str) -> str:
    table = str(value or "").strip()
    if not _IDENTIFIER_RE.fullmatch(table):
        raise ValueError("Manticore table name must be a SQL identifier")
    return table


@dataclass(frozen=True)
class ManticoreClient:
    base_url: str = "http://127.0.0.1:9308"
    table: str = "tg_messages"
    timeout_seconds: float = 5.0
    bearer_token: str = ""

    def __post_init__(self) -> None:
        object.__setattr__(self, "base_url", self.base_url.rstrip("/"))
        object.__setattr__(self, "table", validate_table_name(self.table))
        object.__setattr__(self, "timeout_seconds", max(0.2, float(self.timeout_seconds)))

    def _request(
        self, path: str, body: bytes, *, content_type: str
    ) -> Any:
        headers = {
            "Accept": "application/json",
            "Content-Type": content_type,
        }
        if self.bearer_token:
            headers["Authorization"] = f"Bearer {self.bearer_token}"
        request = urllib.request.Request(
            f"{self.base_url}{path}", body, headers=headers, method="POST"
        )
        try:
            with urllib.request.urlopen(
                request, timeout=self.timeout_seconds
            ) as response:
                payload = response.read()
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            raise ManticoreError(f"Manticore request failed: {exc}") from exc

        try:
            return json.loads(payload.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ManticoreError("Manticore returned invalid JSON") from exc

    def execute_select(self, sql: str) -> dict[str, Any]:
        payload = self._request(
            "/sql", str(sql).encode("utf-8"), content_type="text/plain; charset=utf-8"
        )
        if not isinstance(payload, dict):
            raise ManticoreError("Unexpected Manticore SELECT response")
        error = str(payload.get("error") or "").strip()
        if error:
            raise ManticoreError(error)
        return payload

    def execute_raw(self, sql: str) -> list[dict[str, Any]]:
        payload = self._request(
            "/sql?mode=raw",
            str(sql).encode("utf-8"),
            content_type="text/plain; charset=utf-8",
        )
        if not isinstance(payload, list):
            raise ManticoreError("Unexpected Manticore SQL response")
        for result in payload:
            if not isinstance(result, dict):
                raise ManticoreError("Unexpected Manticore SQL result")
            error = str(result.get("error") or "").strip()
            if error:
                raise ManticoreError(error)
        return payload

    def ensure_table(self) -> None:
        self.execute_raw(f"""
            CREATE TABLE IF NOT EXISTS {self.table} (
                content text indexed,
                chat_id bigint,
                message_id bigint,
                msg_date_ts bigint,
                type_code int,
                file_size bigint,
                duration_sec int,
                is_promo int
            )
            charset_table='non_cont'
            ngram_len='1'
            ngram_chars='cjk'
            dict='keywords'
            min_infix_len='2'
            rt_mem_limit='256M'
        """)

    def truncate_table(self) -> None:
        self.execute_raw(f"TRUNCATE TABLE {self.table}")

    def optimize_table(self) -> None:
        self.execute_raw(f"OPTIMIZE TABLE {self.table}")

    def table_status(self) -> dict[str, str]:
        results = self.execute_raw(f"SHOW TABLE {self.table} STATUS")
        if not results:
            return {}
        rows = results[0].get("data")
        if not isinstance(rows, list):
            return {}
        status: dict[str, str] = {}
        for row in rows:
            if not isinstance(row, dict):
                continue
            name = str(row.get("Variable_name") or "")
            if name:
                status[name] = str(row.get("Value") or "")
        return status

    def document_count(self) -> int:
        payload = self.execute_select(f"SELECT COUNT(*) AS count FROM {self.table}")
        hits = payload.get("hits")
        raw_hits = hits.get("hits") if isinstance(hits, dict) else None
        if not isinstance(raw_hits, list) or not raw_hits:
            return 0
        source = raw_hits[0].get("_source")
        if not isinstance(source, dict):
            return 0
        return max(0, int(source.get("count") or 0))

    def bulk(self, operations: list[dict[str, Any]]) -> None:
        if not operations:
            return
        body = b"".join(
            json.dumps(item, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
            + b"\n"
            for item in operations
        )
        payload = self._request(
            "/bulk", body, content_type="application/x-ndjson; charset=utf-8"
        )
        if not isinstance(payload, dict):
            raise ManticoreError("Unexpected Manticore bulk response")
        if payload.get("errors") or payload.get("error"):
            error = str(payload.get("error") or "bulk operation failed")
            line = payload.get("current_line")
            if line is not None:
                error = f"{error} at line {line}"
            raise ManticoreError(error)

    def replace_operation(self, document_id: int, doc: dict[str, Any]) -> dict[str, Any]:
        return {
            "replace": {
                "table": self.table,
                "id": int(document_id),
                "doc": doc,
            }
        }

    def delete_operation(self, document_id: int) -> dict[str, Any]:
        return {"delete": {"table": self.table, "id": int(document_id)}}
