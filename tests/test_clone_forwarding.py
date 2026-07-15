from pathlib import Path
from types import SimpleNamespace

import pytest

from tg_harvest.admin_jobs.clone_forwarding import (
    CloneForwardOutcomeAmbiguousError,
    clone_forward_without_source_attribution,
)

ROOT = Path(__file__).resolve().parent.parent


class _ForwardClient:
    def __init__(self):
        self.calls = []

    def forward_messages(self, *args, **kwargs):
        self.calls.append((args, kwargs))
        return ["sent"]


class _DurableForwardClient:
    def __init__(self):
        self.request = None

    def get_input_entity(self, value):
        return f"input:{value}"

    def __call__(self, request):
        self.request = request
        return "updates"

    def _get_response_message(self, request, _updates, _target):
        return [SimpleNamespace(id=7000 + index) for index, _ in enumerate(request.id)]


def test_clone_forward_without_source_attribution_forces_drop_author_and_silence():
    client = _ForwardClient()

    result = clone_forward_without_source_attribution(
        client,
        "target",
        [1, 2],
        from_peer="source",
    )

    assert result == ["sent"]
    assert client.calls == [
        (
            ("target", [1, 2]),
            {
                "from_peer": "source",
                "drop_author": True,
                "silent": True,
            },
        )
    ]


def test_clone_forward_reuses_persisted_random_ids_when_available():
    client = _DurableForwardClient()

    result = clone_forward_without_source_attribution(
        client,
        "target",
        [11, 12],
        from_peer="source",
        random_ids=[101, 102],
    )

    assert [message.id for message in result] == [7000, 7001]
    assert client.request is not None
    assert client.request.id == [11, 12]
    assert client.request.random_id == [101, 102]
    assert client.request.drop_author is True
    assert client.request.silent is True


def test_clone_forward_turns_duplicate_random_id_into_ambiguous_outcome():
    class RandomIdDuplicateError(RuntimeError):
        pass

    class _DuplicateClient(_DurableForwardClient):
        def __call__(self, request):
            self.request = request
            raise RandomIdDuplicateError("random ID was already used")

    with pytest.raises(CloneForwardOutcomeAmbiguousError, match="避免重复消息"):
        clone_forward_without_source_attribution(
            _DuplicateClient(),
            "target",
            11,
            from_peer="source",
            random_ids=[101],
        )


def test_clone_forward_messages_calls_are_centralized():
    offenders = []
    for path in (ROOT / "tg_harvest").rglob("*.py"):
        if path.name == "clone_forwarding.py":
            continue
        source = path.read_text(encoding="utf-8")
        if "forward_messages(" in source:
            offenders.append(str(path.relative_to(ROOT)))

    assert offenders == []
