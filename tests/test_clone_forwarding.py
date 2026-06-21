from pathlib import Path
from types import SimpleNamespace

import pytest

from tg_harvest.admin_jobs.clone_forwarding import (
    clone_forward_without_source_attribution,
)
from tg_harvest.admin_jobs.clone_media_copy import (
    copy_clone_media_via_relay_without_source,
)

ROOT = Path(__file__).resolve().parent.parent


class _ForwardClient:
    def __init__(self):
        self.calls = []

    def forward_messages(self, *args, **kwargs):
        self.calls.append((args, kwargs))
        return ["sent"]


class _RelaySourceClient:
    def __init__(self):
        self.forward_calls = []
        self.delete_calls = []

    def forward_messages(self, *args, **kwargs):
        self.forward_calls.append((args, kwargs))
        return [SimpleNamespace(id=7001), SimpleNamespace(id=7002)]

    def delete_messages(self, *args, **kwargs):
        self.delete_calls.append((args, kwargs))
        return True


class _RelayPartialSourceClient:
    def __init__(self):
        self.forward_calls = []
        self.delete_calls = []

    def forward_messages(self, *args, **kwargs):
        self.forward_calls.append((args, kwargs))
        return [None, SimpleNamespace(id=7002)]

    def delete_messages(self, *args, **kwargs):
        self.delete_calls.append((args, kwargs))
        return True


class _RelayTargetFailClient:
    def __init__(self):
        self.forward_calls = []

    def forward_messages(self, *args, **kwargs):
        self.forward_calls.append((args, kwargs))
        raise RuntimeError("target copy failed")


def test_clone_forward_without_source_attribution_forces_drop_author():
    client = _ForwardClient()

    result = clone_forward_without_source_attribution(
        client,
        "target",
        [1, 2],
        from_peer="source",
        as_album=True,
    )

    assert result == ["sent"]
    assert client.calls == [
        (
            ("target", [1, 2]),
            {
                "from_peer": "source",
                "drop_author": True,
            },
        )
    ]


def test_relay_copy_cleans_temporary_messages_when_target_copy_fails():
    source_client = _RelaySourceClient()
    target_client = _RelayTargetFailClient()

    with pytest.raises(RuntimeError, match="target copy failed"):
        copy_clone_media_via_relay_without_source(
            source_client=source_client,
            target_client=target_client,
            relay_entity_for_source="relay-for-source",
            relay_entity_for_target="relay-for-target",
            target_entity="target",
            message_ids=[11, 12],
            source_entity="source",
            as_album=True,
        )

    assert source_client.forward_calls == [
        (
            ("relay-for-source", [11, 12]),
            {
                "from_peer": "source",
                "drop_author": True,
            },
        )
    ]
    assert target_client.forward_calls == [
        (
            ("target", [7001, 7002]),
            {
                "from_peer": "relay-for-target",
                "drop_author": True,
            },
        )
    ]
    assert source_client.delete_calls == [
        (("relay-for-source", [7001, 7002]), {"revoke": True})
    ]


def test_relay_copy_rejects_partial_first_hop_to_keep_mapping_aligned():
    source_client = _RelayPartialSourceClient()
    target_client = _ForwardClient()

    with pytest.raises(RuntimeError, match="未完整返回消息 ID"):
        copy_clone_media_via_relay_without_source(
            source_client=source_client,
            target_client=target_client,
            relay_entity_for_source="relay-for-source",
            relay_entity_for_target="relay-for-target",
            target_entity="target",
            message_ids=[11, 12],
            source_entity="source",
            as_album=True,
        )

    assert source_client.forward_calls == [
        (
            ("relay-for-source", [11, 12]),
            {
                "from_peer": "source",
                "drop_author": True,
            },
        )
    ]
    assert target_client.calls == []
    assert source_client.delete_calls == [
        (("relay-for-source", [7002]), {"revoke": True})
    ]


def test_clone_forward_messages_calls_are_centralized():
    offenders = []
    for path in (ROOT / "tg_harvest").rglob("*.py"):
        if path.name == "clone_forwarding.py":
            continue
        source = path.read_text(encoding="utf-8")
        if "forward_messages(" in source:
            offenders.append(str(path.relative_to(ROOT)))

    assert offenders == []
