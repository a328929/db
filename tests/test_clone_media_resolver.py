from types import SimpleNamespace

from tg_harvest.admin_jobs.clone_media_resolver import clone_api_resolve_media_group


class _ResolverClient:
    def __init__(self, messages):
        self.messages = {int(message.id): message for message in messages}
        self.calls = []

    def get_messages(self, _entity, **kwargs):
        self.calls.append(dict(kwargs))
        ids = kwargs.get("ids")
        if ids is not None:
            if isinstance(ids, list):
                return [self.messages.get(int(message_id)) for message_id in ids]
            return self.messages.get(int(ids))

        min_id = int(kwargs.get("min_id") or 0)
        max_id = int(kwargs.get("max_id") or 0)
        return [
            message
            for message_id, message in sorted(self.messages.items())
            if message_id > min_id and (max_id <= 0 or message_id < max_id)
        ]


def _media_message(message_id, *, grouped_id, media_kind="photo"):
    return SimpleNamespace(
        id=int(message_id),
        grouped_id=grouped_id,
        media=object(),
        media_kind=media_kind,
    )


def test_clone_media_group_resolver_expands_beyond_initial_scan_window():
    messages = [
        _media_message(message_id, grouped_id=9001)
        for message_id in range(70, 131)
    ]
    messages.extend(
        [
            _media_message(69, grouped_id=8001),
            _media_message(131, grouped_id=8002),
        ]
    )
    client = _ResolverClient(messages)

    result = clone_api_resolve_media_group(
        client,
        "source",
        [100],
        scan_radius=25,
    )

    assert result["ok"] is True
    assert result["resolution"] == "api_group_expanded"
    assert result["message_ids"] == list(range(70, 131))
    assert result["api_window_item_count"] < result["api_expanded_item_count"]
    assert result["copy_strategy"] == "album"
    assert 69 not in result["message_ids"]
    assert 131 not in result["message_ids"]


def test_clone_media_group_resolver_stops_at_group_boundaries():
    client = _ResolverClient(
        [
            _media_message(49, grouped_id=7001),
            _media_message(50, grouped_id=7002),
            _media_message(51, grouped_id=7002),
            _media_message(52, grouped_id=7002),
            _media_message(53, grouped_id=7003),
        ]
    )

    result = clone_api_resolve_media_group(
        client,
        "source",
        [51],
        scan_radius=0,
    )

    assert result["ok"] is True
    assert result["message_ids"] == [50, 51, 52]
    assert result["grouped_id"] == 7002
    assert result["resolution"] == "api_group_expanded"
