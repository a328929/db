import unittest
from types import SimpleNamespace
from unittest.mock import patch

import tg_harvest.admin_jobs.clone_media_resolver as clone_media_resolver
from tg_harvest.admin_jobs.clone_media_resolver import (
    clone_api_resolve_media_group,
    clone_api_resolve_media_message,
)


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


class _RetryOnceResolverClient(_ResolverClient):
    def __init__(self, messages):
        super().__init__(messages)
        self.failed = False

    def get_messages(self, _entity, **kwargs):
        if not self.failed:
            self.failed = True
            raise TimeoutError("temporary timeout")
        return super().get_messages(_entity, **kwargs)


def _media_message(message_id, *, grouped_id, media_kind="photo"):
    return SimpleNamespace(
        id=int(message_id),
        grouped_id=grouped_id,
        media=object(),
        media_kind=media_kind,
    )


class CloneMediaResolverTests(unittest.TestCase):
    def test_clone_media_group_resolver_expands_beyond_initial_scan_window(self):
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

        self.assertIs(True, result["ok"])
        self.assertEqual("api_group_expanded", result["resolution"])
        self.assertEqual(list(range(70, 131)), result["message_ids"])
        self.assertLess(
            result["api_window_item_count"],
            result["api_expanded_item_count"],
        )
        self.assertEqual("album", result["copy_strategy"])
        self.assertNotIn(69, result["message_ids"])
        self.assertNotIn(131, result["message_ids"])

    def test_clone_media_group_resolver_stops_at_group_boundaries(self):
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

        self.assertIs(True, result["ok"])
        self.assertEqual([50, 51, 52], result["message_ids"])
        self.assertEqual(7002, result["grouped_id"])
        self.assertEqual("api_group_expanded", result["resolution"])

    def test_clone_media_message_resolver_retries_transient_error(self):
        client = _RetryOnceResolverClient([_media_message(51, grouped_id=7002)])

        with patch(
            "tg_harvest.admin_jobs.clone_media_resolver.call_with_bounded_retry",
            wraps=clone_media_resolver.call_with_bounded_retry,
        ) as retry_mock:
            result = clone_api_resolve_media_message(client, "source", 51)

        self.assertIs(True, result["ok"])
        self.assertEqual(51, result["message_id"])
        self.assertEqual(1, len(client.calls))
        self.assertEqual("clone-media-single-message", retry_mock.call_args.kwargs["scope"])

    def test_clone_media_group_resolver_uses_retry_scopes(self):
        client = _ResolverClient(
            [
                _media_message(49, grouped_id=7001),
                _media_message(50, grouped_id=7002),
                _media_message(51, grouped_id=7002),
                _media_message(52, grouped_id=7002),
                _media_message(53, grouped_id=7003),
            ]
        )
        scopes = []
        original_retry = clone_media_resolver.call_with_bounded_retry

        def record_retry(fn, /, *args, **kwargs):
            scopes.append(kwargs.get("scope"))
            return original_retry(fn, *args, **kwargs)

        with patch(
            "tg_harvest.admin_jobs.clone_media_resolver.call_with_bounded_retry",
            side_effect=record_retry,
        ):
            result = clone_api_resolve_media_group(client, "source", [51], scan_radius=0)

        self.assertIs(True, result["ok"])
        self.assertIn("clone-media-group-anchors", scopes)
        self.assertIn("clone-media-group-window", scopes)
        self.assertIn("clone-media-group-expand", scopes)
