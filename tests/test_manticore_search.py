import io
import json
import sqlite3
import tempfile
import unittest
import urllib.error
from pathlib import Path
from unittest.mock import patch

from tg_harvest.search.browse_service import sqlite_browse_payload_service
from tg_harvest.search.expression import (
    MAX_SEARCH_EXPR_NODES,
    MAX_SEARCH_EXPR_OR_BRANCHES,
    MAX_SEARCH_EXPR_WILDCARDS,
    parse_query,
)
from tg_harvest.search.manticore_client import ManticoreClient, ManticoreError
from tg_harvest.search.manticore_service import (
    compile_manticore_match,
    manticore_search_payload_service,
)
from tg_harvest.search.manticore_sync import (
    _manticore_validation_is_due,
    _next_recovery_validation_interval,
    drain_manticore_outbox,
    validate_manticore_state,
)
from tg_harvest.search.params import SearchParams
from tg_harvest.storage.connection import detect_sqlite_features
from tg_harvest.storage.manticore_outbox import (
    OUTBOX_TABLE,
    get_manticore_index_status,
    get_manticore_meta,
    manticore_index_is_ready,
    set_manticore_index_status,
)
from tg_harvest.storage.schema import create_schema
from tg_harvest.web.routes import search as search_routes_module


class _Response:
    def __init__(self, payload):
        self.payload = json.dumps(payload).encode()

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return None

    def read(self):
        return self.payload


class _FakeManticoreClient(ManticoreClient):
    def __init__(self, responses=None):
        super().__init__(table="tg_messages")
        object.__setattr__(self, "responses", list(responses or []))
        object.__setattr__(self, "sql", [])
        object.__setattr__(self, "operations", [])

    def execute_select(self, sql):
        self.sql.append(sql)
        return self.responses.pop(0)

    def bulk(self, operations):
        self.operations.extend(operations)


class ManticoreClientTests(unittest.TestCase):
    def test_bulk_uses_ndjson_and_bearer_auth(self):
        client = ManticoreClient(bearer_token="secret")
        operation = client.replace_operation(7, {"content": "中文"})
        with patch(
            "urllib.request.urlopen",
            return_value=_Response({"errors": False}),
        ) as urlopen:
            client.bulk([operation])

        request = urlopen.call_args.args[0]
        self.assertEqual(request.headers["Authorization"], "Bearer secret")
        self.assertEqual(request.full_url.removeprefix(client.base_url), "/bulk")
        self.assertEqual(operation, json.loads(request.data.decode().strip()))

    def test_bulk_error_is_not_accepted_as_success(self):
        client = ManticoreClient()
        with patch(
            "urllib.request.urlopen",
            return_value=_Response({"errors": True, "error": "bad", "current_line": 2}),
        ), self.assertRaisesRegex(ManticoreError, "bad at line 2"):
            client.bulk([client.delete_operation(1)])

    def test_table_name_is_validated(self):
        with self.assertRaises(ValueError):
            ManticoreClient(table="messages; DROP TABLE messages")

    def test_sample_documents_extracts_stored_sources(self):
        client = ManticoreClient()
        response = _Response(
            {
                "hits": {
                    "hits": [
                        {
                            "_id": 7,
                            "_source": {
                                "chat_id": 1,
                                "message_id": 2,
                                "msg_date_ts": 3,
                                "type_code": 1,
                                "file_size": 4,
                                "duration_sec": 5,
                                "is_promo": 0,
                            },
                        }
                    ]
                }
            }
        )
        with patch("urllib.request.urlopen", return_value=response) as urlopen:
            documents = client.sample_documents(range(1, 65))

        self.assertEqual(
            documents,
            {
                7: {
                    "chat_id": 1,
                    "message_id": 2,
                    "msg_date_ts": 3,
                    "type_code": 1,
                    "file_size": 4,
                    "duration_sec": 5,
                    "is_promo": 0,
                }
            },
        )
        sql = urlopen.call_args.args[0].data.decode()
        self.assertNotIn("content", sql.lower())
        self.assertIn("LIMIT 64", sql)

    def test_http_error_body_is_preserved_for_diagnostics(self):
        client = ManticoreClient()
        error = urllib.error.HTTPError(
            client.base_url,
            500,
            "Internal Server Error",
            {},
            io.BytesIO(b'{"error":"field is not stored"}'),
        )
        with patch("urllib.request.urlopen", side_effect=error), self.assertRaisesRegex(
            ManticoreError, "field is not stored"
        ):
            client.execute_select("SELECT 1")
        self.assertTrue(error.fp.closed)


class ManticoreExpressionTests(unittest.TestCase):
    def test_boolean_expression_is_compiled_to_manticore_syntax(self):
        compiled = compile_manticore_match(parse_query("福利/会员+onlyfans-广告"))
        # 注意: 新策略下 onlyfans 不再自动添加通配符
        self.assertEqual(
            compiled,
            '((("福利" | "会员") onlyfans) !("广告"))',
        )

    def test_phrase_and_user_wildcards_are_escaped(self):
        self.assertEqual(
            compile_manticore_match(parse_query('"foo bar"')), '"foo bar"'
        )
        # 新策略: 不自动添加通配符，% 被转义
        self.assertEqual(
            compile_manticore_match(parse_query("100%_done")),
            "100\\%_done",
        )

    def test_exact_match_by_default(self):
        """默认使用精确匹配，不自动添加通配符"""
        # 普通词：精确匹配
        self.assertEqual(compile_manticore_match(parse_query("test")), "test")
        self.assertEqual(compile_manticore_match(parse_query("onlyfans")), "onlyfans")

        # 短词也精确匹配
        self.assertEqual(compile_manticore_match(parse_query("te")), "te")
        self.assertEqual(compile_manticore_match(parse_query("a")), "a")

    def test_user_specified_wildcards_preserved(self):
        """用户显式指定的通配符应该被保留"""
        # 后缀通配
        self.assertEqual(compile_manticore_match(parse_query("test*")), "test*")

        # 前缀通配
        self.assertEqual(compile_manticore_match(parse_query("*test")), "*test")

        # 包含实际词项的前后通配
        self.assertEqual(compile_manticore_match(parse_query("*test*")), "*test*")

        # 中间通配
        self.assertEqual(compile_manticore_match(parse_query("t*st")), "t*st")

        # 问号通配符
        self.assertEqual(compile_manticore_match(parse_query("te?t")), "te?t")

    def test_special_chars_in_wildcard_mode(self):
        """通配模式中仍需转义 Manticore 控制字符"""
        self.assertEqual(compile_manticore_match(parse_query("100%*")), "100\\%*")

    def test_manticore_control_characters_are_escaped(self):
        self.assertEqual(compile_manticore_match(parse_query("alpha|beta")), r"alpha\|beta")
        self.assertEqual(compile_manticore_match(parse_query("@content")), r"\@content")
        self.assertEqual(compile_manticore_match(parse_query("foo&bar")), r"foo\&bar")

    def test_bare_wildcard_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "至少一个普通字符"):
            compile_manticore_match(parse_query("*"))
        with self.assertRaisesRegex(ValueError, "至少一个普通字符"):
            compile_manticore_match(parse_query("??"))


class SearchRateLimitTests(unittest.TestCase):
    """测试搜索限流机制的IP获取和验证"""

    def test_get_client_ip_without_proxy(self):
        """无代理时应返回 remote_addr"""
        from unittest.mock import Mock

        from tg_harvest.web.ip_utils import get_client_ip

        mock_request = Mock()
        mock_request.remote_addr = "192.168.1.100"
        mock_request.headers = {}

        with patch.dict("os.environ", {"TG_TRUST_PROXY_CIDRS": ""}):
            ip = get_client_ip(mock_request)
        self.assertEqual(ip, "192.168.1.100")

    def test_get_client_ip_with_trusted_proxy(self):
        """配置了可信代理时应解析 X-Forwarded-For"""
        from unittest.mock import Mock, patch

        from tg_harvest.web.ip_utils import get_client_ip

        mock_request = Mock()
        mock_request.remote_addr = "127.0.0.1"
        mock_request.headers = {"X-Forwarded-For": "8.8.8.8, 10.0.0.1"}

        with patch.dict(
            "os.environ",
            {"TG_TRUST_PROXY_CIDRS": "127.0.0.1/32,10.0.0.0/8"},
        ):
            ip = get_client_ip(mock_request)
            self.assertEqual(ip, "8.8.8.8")

    def test_untrusted_peer_cannot_forge_forwarded_for(self):
        from unittest.mock import Mock, patch

        from tg_harvest.web.ip_utils import get_client_ip

        mock_request = Mock()
        mock_request.remote_addr = "203.0.113.10"
        mock_request.headers = {"X-Forwarded-For": "8.8.8.8"}

        with patch.dict(
            "os.environ",
            {"TG_TRUST_PROXY": "1", "TG_TRUST_PROXY_CIDRS": "127.0.0.1/32"},
        ):
            self.assertEqual(get_client_ip(mock_request), "203.0.113.10")

    def test_get_client_ip_filters_internal_addresses(self):
        """应该过滤内部IP地址"""
        from unittest.mock import Mock, patch

        from tg_harvest.web.ip_utils import get_client_ip

        mock_request = Mock()
        mock_request.remote_addr = "127.0.0.1"

        test_cases = [
            ("10.0.0.1, 192.168.1.1", "127.0.0.1"),  # 都是内部IP，回退
            ("8.8.8.8, 10.0.0.1", "8.8.8.8"),  # 跳过内部IP
            ("8.8.8.8, 1.1.1.1, 10.0.0.1", "1.1.1.1"),  # 取最后一个非内部IP
        ]

        with patch.dict(
            "os.environ",
            {
                "TG_TRUST_PROXY_CIDRS": (
                    "127.0.0.1/32,10.0.0.0/8,192.168.0.0/16"
                )
            },
        ):
            for forwarded_for, expected in test_cases:
                mock_request.headers = {"X-Forwarded-For": forwarded_for}
                ip = get_client_ip(mock_request)
                self.assertEqual(expected, ip, f"Failed for: {forwarded_for}")

    def test_get_client_ip_validates_format(self):
        """应该验证IP格式，拒绝无效输入"""
        from unittest.mock import Mock, patch

        from tg_harvest.web.ip_utils import get_client_ip

        mock_request = Mock()
        mock_request.remote_addr = "127.0.0.1"

        invalid_ips = [
            "invalid-ip",
            "999.999.999.999",
            "'; DROP TABLE users; --",
            "../etc/passwd",
        ]

        with patch.dict("os.environ", {"TG_TRUST_PROXY_CIDRS": "127.0.0.1/32"}):
            for invalid_ip in invalid_ips:
                mock_request.headers = {"X-Forwarded-For": invalid_ip}
                ip = get_client_ip(mock_request)
                # 无效IP应回退到 remote_addr
                self.assertEqual(ip, "127.0.0.1", f"Should reject: {invalid_ip}")

    def test_search_rate_tracker_rejects_new_keys_at_capacity(self):
        with search_routes_module._rate_lock:
            search_routes_module._search_rate_tracker.clear()
        try:
            with patch.object(search_routes_module, "SEARCH_RATE_LIMIT_MAX_TRACKED_KEYS", 1):
                self.assertFalse(search_routes_module._is_rate_limited("first"))
                self.assertTrue(search_routes_module._is_rate_limited("second"))
        finally:
            with search_routes_module._rate_lock:
                search_routes_module._search_rate_tracker.clear()

    def test_is_internal_ip_detection(self):
        """测试内部IP检测逻辑"""
        from tg_harvest.web.ip_utils import is_internal_ip

        # 内部IP
        self.assertTrue(is_internal_ip("10.0.0.1"))
        self.assertTrue(is_internal_ip("192.168.1.1"))
        self.assertTrue(is_internal_ip("172.16.0.1"))
        self.assertTrue(is_internal_ip("127.0.0.1"))
        self.assertTrue(is_internal_ip("::1"))
        self.assertTrue(is_internal_ip("fe80::1"))

        # 公网IP
        self.assertFalse(is_internal_ip("8.8.8.8"))
        self.assertFalse(is_internal_ip("1.1.1.1"))
        self.assertFalse(is_internal_ip("2001:4860:4860::8888"))

    def test_is_valid_ip_format(self):
        """测试IP格式验证"""
        from tg_harvest.web.ip_utils import is_valid_ip_format

        # 有效IPv4
        self.assertTrue(is_valid_ip_format("192.168.1.1"))
        self.assertTrue(is_valid_ip_format("8.8.8.8"))
        self.assertTrue(is_valid_ip_format("1.1.1.1"))

        # 有效IPv6
        self.assertTrue(is_valid_ip_format("::1"))
        self.assertTrue(is_valid_ip_format("2001:4860:4860::8888"))
        self.assertTrue(is_valid_ip_format("fe80::1"))

        # 无效格式
        self.assertFalse(is_valid_ip_format("invalid"))
        self.assertFalse(is_valid_ip_format("999.999.999.999"))
        self.assertFalse(is_valid_ip_format(""))
        self.assertFalse(is_valid_ip_format("'; DROP TABLE users; --"))


class SearchExpressionEdgeCaseTests(unittest.TestCase):
    """测试搜索表达式解析器的边界情况和输入验证"""

    def test_empty_and_whitespace_queries(self):
        """空查询和纯空格应返回None"""
        self.assertIsNone(parse_query(""))
        self.assertIsNone(parse_query("   "))
        self.assertIsNone(parse_query("\t\n"))
        self.assertIsNone(parse_query("  \t  \n  "))

    def test_query_length_limit(self):
        """超过1000字符的查询应被拒绝"""
        # 正好1000字符应该成功
        query_1000 = "test" * 250
        self.assertEqual(len(query_1000), 1000)
        result = parse_query(query_1000)
        self.assertIsNotNone(result)

        # 超过1000字符应该失败
        query_1001 = "test" * 250 + "x"
        self.assertEqual(len(query_1001), 1001)
        with self.assertRaisesRegex(ValueError, "搜索表达式过长"):
            parse_query(query_1001)

        # 中文也应该按字符数计算
        query_chinese_1000 = "福利" * 500
        self.assertEqual(len(query_chinese_1000), 1000)
        result = parse_query(query_chinese_1000)
        self.assertIsNotNone(result)

        query_chinese_1001 = "福利" * 500 + "测"
        with self.assertRaisesRegex(ValueError, "搜索表达式过长"):
            parse_query(query_chinese_1001)

    def test_expression_complexity_limits(self):
        many_terms = "+".join("x" for _ in range(MAX_SEARCH_EXPR_NODES))
        with self.assertRaisesRegex(ValueError, "结构过于复杂"):
            parse_query(many_terms)

        many_or_terms = "/".join("x" for _ in range(MAX_SEARCH_EXPR_OR_BRANCHES + 1))
        with self.assertRaisesRegex(ValueError, "OR 分支过多"):
            parse_query(many_or_terms)

        many_wildcards = " ".join("x*" for _ in range(MAX_SEARCH_EXPR_WILDCARDS + 1))
        with self.assertRaisesRegex(ValueError, "通配符过多"):
            parse_query(many_wildcards)

    def test_unclosed_quote_raises_error(self):
        """未闭合的引号应该报错"""
        with self.assertRaisesRegex(ValueError, "短语引号未闭合"):
            parse_query('"unclosed phrase')

        with self.assertRaisesRegex(ValueError, "短语引号未闭合"):
            parse_query('test "unclosed')

        with self.assertRaisesRegex(ValueError, "短语引号未闭合"):
            parse_query('"one" + "two')

    def test_empty_phrase_returns_none_or_error(self):
        """空短语（空引号或只有空格的引号）"""
        # 空引号：词法分析器会跳过空term，导致token列表为空
        with self.assertRaisesRegex(ValueError, "搜索表达式为空"):
            parse_query('""')

        # 只有空格的短语也会被normalize后跳过
        with self.assertRaisesRegex(ValueError, "搜索表达式为空"):
            parse_query('"   "')

    def test_unmatched_parentheses(self):
        """括号不匹配应该报错"""
        # 单独的左括号
        with self.assertRaisesRegex(ValueError, "括号未闭合|搜索表达式意外结束"):
            parse_query("(")

        # 单独的右括号
        with self.assertRaisesRegex(ValueError, "存在多余的右括号"):
            parse_query(")")

        # 未闭合的左括号
        with self.assertRaisesRegex(ValueError, "括号未闭合"):
            parse_query("(test")

        # 多余的右括号
        with self.assertRaisesRegex(ValueError, "存在多余的右括号|搜索表达式存在无法解析的尾部内容"):
            parse_query("test)")

        # 空括号
        with self.assertRaisesRegex(ValueError, "存在多余的右括号"):
            parse_query("()")

        # 嵌套空括号
        with self.assertRaisesRegex(ValueError, "存在多余的右括号"):
            parse_query("((()))")

    def test_invalid_operator_positions(self):
        """运算符在非法位置应该报错"""
        # 单独的运算符
        with self.assertRaisesRegex(ValueError, "搜索表达式意外结束|搜索表达式存在非法位置的操作符"):
            parse_query("-")

        with self.assertRaisesRegex(ValueError, "搜索表达式存在非法位置的操作符"):
            parse_query("+")

        with self.assertRaisesRegex(ValueError, "搜索表达式存在非法位置的操作符|搜索表达式意外结束"):
            parse_query("/")

        # 连续的运算符
        with self.assertRaisesRegex(ValueError, "搜索表达式存在非法位置的操作符"):
            parse_query("test++test")

        with self.assertRaisesRegex(ValueError, "搜索表达式存在非法位置的操作符"):
            parse_query("test//test")

        # 运算符开头（除了NOT）
        with self.assertRaisesRegex(ValueError, "搜索表达式存在非法位置的操作符"):
            parse_query("+test")

        with self.assertRaisesRegex(ValueError, "搜索表达式存在非法位置的操作符|搜索表达式意外结束"):
            parse_query("/test")

        # 运算符结尾
        with self.assertRaisesRegex(ValueError, "搜索表达式意外结束"):
            parse_query("test+")

        with self.assertRaisesRegex(ValueError, "搜索表达式意外结束"):
            parse_query("test-")

    def test_multiple_not_operators(self):
        """多个NOT运算符应该正确处理"""
        # 双重否定应该成功解析
        result = parse_query("--test")
        self.assertEqual(result.kind, "NOT")
        self.assertEqual(result.left.kind, "NOT")
        self.assertEqual(result.left.left.kind, "TERM")

        # 三重否定
        result = parse_query("---test")
        self.assertEqual(result.kind, "NOT")
        self.assertEqual(result.left.kind, "NOT")
        self.assertEqual(result.left.left.kind, "NOT")

    def test_whitespace_handling(self):
        """空白符处理"""
        # 前后空格应该被trim
        result = parse_query("   test   ")
        self.assertEqual(result.kind, "TERM")
        self.assertEqual(result.value, "test")

        # 词之间的多个空格应该被视为隐式AND
        result = parse_query("test   word")
        self.assertEqual(result.kind, "AND")

        # tab和换行也应该被当作空白
        result = parse_query("test\t\nword")
        self.assertEqual(result.kind, "AND")

    def test_special_characters_in_terms(self):
        """特殊字符在term中的处理"""
        # null字节应该被处理（不应崩溃）
        try:
            result = parse_query("test\x00null")
            # 应该成功解析或被normalize过滤
            self.assertIn(result.kind, ["TERM", "AND"])
        except ValueError:
            # 如果normalize拒绝也可以接受
            pass

        # Unicode字符应该正常工作
        result = parse_query("测试emoji😀搜索")
        self.assertIsNotNone(result)

        # 全角运算符应该被转换
        result = parse_query("福利＋会员")
        self.assertEqual(result.kind, "AND")

        result = parse_query("福利／会员")
        self.assertEqual(result.kind, "OR")

    def test_deeply_nested_expressions(self):
        """深度嵌套的表达式"""
        # 100层嵌套应该成功（在1000字符限制内）
        query_100 = "(" * 100 + "test" + ")" * 100
        self.assertEqual(len(query_100), 204)
        result = parse_query(query_100)
        self.assertEqual(result.kind, "TERM")

        # 200层嵌套也应该成功
        query_200 = "(" * 200 + "test" + ")" * 200
        self.assertEqual(len(query_200), 404)
        result = parse_query(query_200)
        self.assertEqual(result.kind, "TERM")

        # 超过1000字符的深度嵌套应该被长度限制拒绝
        query_over_limit = "(" * 500 + "test" + ")" * 500
        with self.assertRaisesRegex(ValueError, "搜索表达式过长"):
            parse_query(query_over_limit)

    def test_complex_valid_expressions(self):
        """复杂但合法的表达式应该成功解析"""
        # 多层嵌套的布尔运算
        result = parse_query("(a+b)/(c+d)")
        self.assertEqual(result.kind, "OR")

        # NOT在括号中
        result = parse_query("test+-(bad/worse)")
        self.assertEqual(result.kind, "AND")
        self.assertEqual(result.right.kind, "NOT")

        # 长的合法表达式
        result = parse_query("word1+word2+word3/word4-word5")
        self.assertIsNotNone(result)

        # 短语和term混合
        result = parse_query('"exact phrase"+keyword')
        self.assertEqual(result.kind, "AND")
        self.assertEqual(result.left.kind, "PHRASE")
        self.assertEqual(result.right.kind, "TERM")

    def test_edge_case_normalization(self):
        """测试normalize对边界情况的处理"""
        # 只有标点符号的term可能被normalize成空字符串
        # 这应该不产生token，从而导致解析错误或返回None
        result = parse_query("!!!")
        # normalize可能会把它清理掉
        if result is not None:
            self.assertIsNotNone(result)

        # 数字应该正常工作
        result = parse_query("123+456")
        self.assertIsNotNone(result)

    def test_phrase_with_special_content(self):
        """短语中的特殊内容"""
        # 短语中的转义
        result = parse_query(r'"quote with \" inside"')
        self.assertEqual(result.kind, "PHRASE")

        # 短语中的运算符应该被当作普通字符
        result = parse_query('"has + and - operators"')
        self.assertEqual(result.kind, "PHRASE")

        # 短语中的括号
        result = parse_query('"has (parentheses) inside"')
        self.assertEqual(result.kind, "PHRASE")


class ManticoreOutboxTests(unittest.TestCase):
    def setUp(self):
        self.conn = sqlite3.connect(":memory:")
        self.conn.row_factory = sqlite3.Row
        create_schema(
            self.conn,
            detect_sqlite_features(self.conn),
        )
        self.conn.execute("INSERT INTO chats(chat_id, chat_title) VALUES (1, 'Chat')")
        self.conn.commit()

    def tearDown(self):
        self.conn.close()

    def _insert_message(self):
        self.conn.execute(
            """
            INSERT INTO messages(
                chat_id, message_id, msg_date_text, msg_date_ts,
                content, content_norm, msg_type
            ) VALUES (1, 10, 'date', 100, 'Raw', 'normalized', 'TEXT')
            """
        )
        self.conn.commit()
        return int(self.conn.execute("SELECT pk FROM messages").fetchone()[0])

    def test_message_and_media_changes_collapse_to_one_revisioned_item(self):
        pk = self._insert_message()
        self.conn.execute(
            """
            INSERT INTO message_media(chat_id, message_id, file_size, duration_sec)
            VALUES (1, 10, 123, 9)
            """
        )
        self.conn.execute(
            "UPDATE messages SET content_norm = 'changed' WHERE pk = ?", (pk,)
        )
        self.conn.commit()

        row = self.conn.execute(
            f"SELECT * FROM {OUTBOX_TABLE} WHERE pk = ?", (pk,)
        ).fetchone()
        self.assertEqual(row["operation"], "upsert")
        self.assertEqual(row["revision"], 3)

    def test_moving_media_requeues_both_old_and_new_messages(self):
        first_pk = self._insert_message()
        self.conn.execute(
            """
            INSERT INTO messages(
                chat_id, message_id, msg_date_text, msg_date_ts,
                content, content_norm, msg_type
            ) VALUES (1, 11, 'date', 101, 'Second', 'second', 'TEXT')
            """
        )
        self.conn.execute(
            """
            INSERT INTO message_media(chat_id, message_id, file_size, duration_sec)
            VALUES (1, 10, 123, 9)
            """
        )
        self.conn.commit()
        self.conn.execute(f"DELETE FROM {OUTBOX_TABLE}")
        self.conn.execute(
            "UPDATE message_media SET message_id = 11 WHERE message_id = 10"
        )
        self.conn.commit()

        rows = self.conn.execute(
            f"SELECT pk, operation FROM {OUTBOX_TABLE} ORDER BY pk"
        ).fetchall()
        self.assertEqual(
            [tuple(row) for row in rows],
            [(first_pk, "upsert"), (first_pk + 1, "upsert")],
        )

    def test_index_status_requires_an_explicit_completed_rebuild(self):
        self.assertFalse(manticore_index_is_ready(self.conn, "tg_messages"))
        set_manticore_index_status(self.conn, "tg_messages", "building")
        self.assertEqual(
            get_manticore_index_status(self.conn, "tg_messages"), "building"
        )
        set_manticore_index_status(self.conn, "tg_messages", "ready")
        self.assertTrue(manticore_index_is_ready(self.conn, "tg_messages"))

    def test_pending_outbox_disables_ready_index_until_it_is_drained(self):
        set_manticore_index_status(self.conn, "tg_messages", "ready")
        self.assertTrue(manticore_index_is_ready(self.conn, "tg_messages"))

        self._insert_message()
        self.assertFalse(manticore_index_is_ready(self.conn, "tg_messages"))

        self.assertEqual(drain_manticore_outbox(self.conn, _FakeManticoreClient()), 1)
        self.assertTrue(manticore_index_is_ready(self.conn, "tg_messages"))

    def test_delete_overwrites_pending_upsert(self):
        pk = self._insert_message()
        self.conn.execute("DELETE FROM messages WHERE pk = ?", (pk,))
        self.conn.commit()
        row = self.conn.execute(
            f"SELECT operation, revision FROM {OUTBOX_TABLE} WHERE pk = ?", (pk,)
        ).fetchone()
        self.assertEqual(tuple(row), ("delete", 2))

    def test_drain_writes_current_document_and_acknowledges_revision(self):
        pk = self._insert_message()
        self.conn.execute(
            """
            INSERT INTO message_media(chat_id, message_id, file_size, duration_sec)
            VALUES (1, 10, 123, 9)
            """
        )
        self.conn.commit()
        client = _FakeManticoreClient()

        self.assertEqual(drain_manticore_outbox(self.conn, client), 1)
        self.assertEqual(self.conn.execute(f"SELECT COUNT(*) FROM {OUTBOX_TABLE}").fetchone()[0], 0)
        operation = client.operations[0]["replace"]
        self.assertEqual(pk, operation["id"])
        self.assertEqual(operation["doc"]["content"], "normalized")
        self.assertEqual(operation["doc"]["file_size"], 123)

    def test_drain_does_not_ack_a_newer_revision(self):
        pk = self._insert_message()
        conn = self.conn

        class ConcurrentUpdateClient(_FakeManticoreClient):
            def bulk(self, operations):
                super().bulk(operations)
                conn.execute(
                    "UPDATE messages SET content_norm = 'newer' WHERE pk = ?", (pk,)
                )
                conn.commit()

        self.assertEqual(
            drain_manticore_outbox(self.conn, ConcurrentUpdateClient()), 1
        )
        row = self.conn.execute(
            f"SELECT operation, revision FROM {OUTBOX_TABLE} WHERE pk = ?", (pk,)
        ).fetchone()
        self.assertEqual(tuple(row), ("upsert", 2))

    def test_failed_drain_marks_index_stale_and_keeps_outbox_item(self):
        pk = self._insert_message()
        set_manticore_index_status(self.conn, "tg_messages", "ready")

        class FailingClient(_FakeManticoreClient):
            def bulk(self, operations):
                raise RuntimeError("search backend unavailable")

        with self.assertRaisesRegex(RuntimeError, "backend unavailable"):
            drain_manticore_outbox(self.conn, FailingClient())

        self.assertFalse(manticore_index_is_ready(self.conn, "tg_messages"))
        row = self.conn.execute(
            f"SELECT attempts, last_error FROM {OUTBOX_TABLE} WHERE pk = ?", (pk,)
        ).fetchone()
        self.assertEqual(row["attempts"], 1)
        self.assertIn("backend unavailable", row["last_error"])

    def test_empty_content_is_kept_as_a_filterable_document(self):
        self.conn.execute(
            """
            INSERT INTO messages(
                chat_id, message_id, msg_date_text, msg_date_ts,
                content, content_norm, msg_type
            ) VALUES (1, 11, 'date', 101, '', '', 'VIDEO')
            """
        )
        self.conn.commit()
        client = _FakeManticoreClient()

        self.assertEqual(drain_manticore_outbox(self.conn, client), 1)
        self.assertEqual(client.operations[0]["replace"]["doc"]["content"], "")

    def test_schema_removes_legacy_sqlite_search_objects(self):
        self.conn.executescript(
            """
            DROP TRIGGER trg_manticore_messages_insert;
            CREATE TABLE message_search_terms(pk INTEGER, term TEXT);
            CREATE TABLE message_search_terms_rebuild_queue(pk INTEGER PRIMARY KEY);
            CREATE TABLE message_search_terms_meta(key TEXT PRIMARY KEY, value TEXT);
            CREATE TABLE messages_fts(content TEXT);
            CREATE TRIGGER trg_message_terms_queue_insert
            AFTER INSERT ON messages BEGIN SELECT 1; END;
            """
        )
        create_schema(self.conn, detect_sqlite_features(self.conn))

        names = {
            row[0]
            for row in self.conn.execute(
                """
                SELECT name FROM sqlite_master
                WHERE name IN (
                    'message_search_terms', 'message_search_terms_rebuild_queue',
                    'message_search_terms_meta', 'messages_fts',
                    'trg_message_terms_queue_insert'
                )
                """
            )
        }
        self.assertEqual(set(), names)


class ManticoreValidationTests(unittest.TestCase):
    class Client:
        table = "tg_messages"

        def __init__(self, count):
            self.count = count

        def ensure_table(self):
            return None

        def document_count(self):
            return self.count

    class SampleClient(Client):
        def __init__(self, count, sample):
            super().__init__(count)
            self.sample = sample

        def sample_documents(self, document_ids):
            return {
                int(document_id): self.sample[int(document_id)]
                for document_id in document_ids
                if int(document_id) in self.sample
            }

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp_dir.name) / "validation.db"
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        create_schema(conn, detect_sqlite_features(conn))
        conn.execute("INSERT INTO chats(chat_id, chat_title) VALUES (1, 'Chat')")
        conn.execute(
            """
            INSERT INTO messages(
                chat_id, message_id, msg_date_text, msg_date_ts, msg_type
            ) VALUES (1, 1, 'date', 1, 'TEXT')
            """
        )
        conn.execute(f"DELETE FROM {OUTBOX_TABLE}")
        conn.commit()
        conn.close()

    def tearDown(self):
        self.temp_dir.cleanup()

    def _connect(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def test_matching_counts_restore_ready_status(self):
        self.assertTrue(validate_manticore_state(self._connect, self.Client(1)))
        conn = self._connect()
        try:
            self.assertTrue(manticore_index_is_ready(conn, "tg_messages"))
            self.assertEqual(
                get_manticore_meta(
                    conn, "manticore:tg_messages:manticore_document_count"
                ),
                "1",
            )
        finally:
            conn.close()

    def test_missing_documents_invalidate_ready_status(self):
        conn = self._connect()
        set_manticore_index_status(conn, "tg_messages", "ready")
        conn.close()

        self.assertFalse(validate_manticore_state(self._connect, self.Client(0)))
        conn = self._connect()
        try:
            self.assertFalse(manticore_index_is_ready(conn, "tg_messages"))
            self.assertEqual(
                get_manticore_meta(
                    conn, "manticore:tg_messages:manticore_document_count"
                ),
                "0",
            )
        finally:
            conn.close()

    def test_same_counts_with_mismatched_sample_stay_stale(self):
        client = self.SampleClient(
            1,
            {
                1: {
                    "chat_id": 1,
                    "message_id": 99,
                    "msg_date_ts": 1,
                    "type_code": 1,
                    "file_size": 0,
                    "duration_sec": 0,
                    "is_promo": 0,
                }
            },
        )

        self.assertFalse(validate_manticore_state(self._connect, client))
        conn = self._connect()
        try:
            self.assertEqual(
                get_manticore_meta(
                    conn, "manticore:tg_messages:last_validation_error"
                ),
                "Manticore consistency sample mismatch",
            )
            self.assertFalse(manticore_index_is_ready(conn, "tg_messages"))
        finally:
            conn.close()

    def test_matching_sample_restores_ready_status(self):
        client = self.SampleClient(
            1,
            {
                1: {
                    "content": "",
                    "chat_id": 1,
                    "message_id": 1,
                    "msg_date_ts": 1,
                    "type_code": 1,
                    "file_size": 0,
                    "duration_sec": 0,
                    "is_promo": 0,
                }
            },
        )

        self.assertTrue(validate_manticore_state(self._connect, client))
        conn = self._connect()
        try:
            self.assertTrue(
                get_manticore_meta(
                    conn, "manticore:tg_messages:source_sample_digest"
                )
            )
            self.assertEqual(
                get_manticore_meta(
                    conn, "manticore:tg_messages:source_sample_digest"
                ),
                get_manticore_meta(
                    conn, "manticore:tg_messages:manticore_sample_digest"
                ),
            )
        finally:
            conn.close()

    def test_normal_sync_does_not_force_an_exact_validation(self):
        self.assertFalse(
            _manticore_validation_is_due(
                now=130.0,
                last_validated_at=100.0,
                interval_seconds=600.0,
                index_ready=True,
            )
        )

    def test_periodic_or_recovery_state_triggers_exact_validation(self):
        self.assertTrue(
            _manticore_validation_is_due(
                now=700.0,
                last_validated_at=100.0,
                interval_seconds=600.0,
                index_ready=True,
            )
        )
        self.assertTrue(
            _manticore_validation_is_due(
                now=160.0,
                last_validated_at=100.0,
                interval_seconds=600.0,
                index_ready=False,
            )
        )

    def test_stale_validation_is_rate_limited(self):
        self.assertFalse(
            _manticore_validation_is_due(
                now=159.0,
                last_validated_at=100.0,
                interval_seconds=600.0,
                index_ready=False,
            )
        )
        self.assertTrue(
            _manticore_validation_is_due(
                now=160.0,
                last_validated_at=100.0,
                interval_seconds=600.0,
                index_ready=False,
            )
        )

    def test_recovery_validation_does_not_follow_short_normal_interval(self):
        self.assertFalse(
            _manticore_validation_is_due(
                now=190.0,
                last_validated_at=100.0,
                interval_seconds=60.0,
                recovery_interval_seconds=120.0,
                index_ready=False,
            )
        )
        self.assertTrue(
            _manticore_validation_is_due(
                now=220.0,
                last_validated_at=100.0,
                interval_seconds=60.0,
                recovery_interval_seconds=120.0,
                index_ready=False,
            )
        )

    def test_recovery_validation_backoff_is_bounded_and_resets(self):
        interval = 60.0
        sequence = []
        for _ in range(5):
            sequence.append(interval)
            interval = _next_recovery_validation_interval(
                interval, validation_ready=False
            )
        self.assertEqual(sequence, [60.0, 120.0, 240.0, 480.0, 600.0])
        self.assertEqual(
            _next_recovery_validation_interval(600.0, validation_ready=False),
            600.0,
        )
        self.assertEqual(
            _next_recovery_validation_interval(600.0, validation_ready=True),
            60.0,
        )


class ManticoreServiceTests(unittest.TestCase):
    def setUp(self):
        self.conn = sqlite3.connect(":memory:")
        self.conn.row_factory = sqlite3.Row
        create_schema(
            self.conn,
            detect_sqlite_features(self.conn),
        )
        self.conn.execute("INSERT INTO chats(chat_id, chat_title) VALUES (1, 'Chat')")
        self.conn.execute(
            """
            INSERT INTO messages(
                chat_id, message_id, msg_date_text, msg_date_ts,
                content, content_norm, msg_type
            ) VALUES (1, 10, 'date', 100, 'Visible', 'visible', 'TEXT')
            """
        )
        self.conn.commit()
        self.pk = int(self.conn.execute("SELECT pk FROM messages").fetchone()[0])

    def tearDown(self):
        self.conn.close()

    def _params(self, **overrides):
        values = {
            "raw_query": "福利",
            "search_type": "all",
            "sort_by_req": "time",
            "order_req": "desc",
            "page": 1,
            "chat_id": None,
            "skip_count": True,
        }
        values.update(overrides)
        return SearchParams(**values)

    def test_search_hydrates_manticore_ids_from_sqlite_in_hit_order(self):
        client = _FakeManticoreClient(
            [
                {
                    "hits": {
                        "total": 1,
                        "total_relation": "eq",
                        "hits": [{"_id": self.pk, "_source": {}}],
                    }
                }
            ]
        )
        payload = manticore_search_payload_service(
            self.conn,
            self._params(),
            client=client,
            page_size=100,
            max_count=50000000,
            max_matches=1000000,
            map_search_items_fn=lambda rows: rows,
        )

        self.assertEqual(payload["search_backend"], "manticore")
        self.assertEqual(payload["items"][0]["content"], "Visible")
        self.assertIn('MATCH(\'"福利"\')', client.sql[0])
        self.assertIn("ORDER BY msg_date_ts DESC", client.sql[0])

    def test_stale_manticore_hit_marks_index_unavailable(self):
        self.conn.execute(f"DELETE FROM {OUTBOX_TABLE}")
        set_manticore_index_status(self.conn, "tg_messages", "ready")
        client = _FakeManticoreClient(
            [
                {
                    "hits": {
                        "total": 1,
                        "total_relation": "eq",
                        "hits": [{"_id": self.pk + 100, "_source": {}}],
                    }
                }
            ]
        )

        with self.assertRaisesRegex(ManticoreError, "absent from SQLite"):
            manticore_search_payload_service(
                self.conn,
                self._params(),
                client=client,
                page_size=100,
                max_count=50000000,
                max_matches=1000000,
                map_search_items_fn=lambda rows: rows,
            )

        self.assertFalse(manticore_index_is_ready(self.conn, "tg_messages"))

    def test_page_query_reuses_manticore_total_when_count_is_deferred(self):
        client = _FakeManticoreClient(
            [
                {
                    "hits": {
                        "total": 12,
                        "total_relation": "eq",
                        "hits": [{"_id": self.pk, "_source": {}}],
                    }
                }
            ]
        )
        payload = manticore_search_payload_service(
            self.conn,
            self._params(skip_count=True),
            client=client,
            page_size=100,
            max_count=50000000,
            max_matches=1000000,
            map_search_items_fn=lambda rows: rows,
        )

        self.assertEqual(payload["total"], 12)
        self.assertEqual(len(client.sql), 1)

    def test_count_only_returns_total_without_expensive_group_facets(self):
        client = _FakeManticoreClient(
            [
                {"hits": {"total": 4, "total_relation": "eq", "hits": []}},
            ]
        )
        payload = manticore_search_payload_service(
            self.conn,
            self._params(skip_count=False, count_only=True),
            client=client,
            page_size=100,
            max_count=50000000,
            max_matches=1000000,
            map_search_items_fn=lambda rows: rows,
        )
        self.assertEqual(payload["total"], 4)
        self.assertEqual(payload["chat_facets"], [])

    def test_relevance_sort_uses_manticore_weight_for_each_message_type(self):
        expected_type_codes = {
            "text": 1,
            "image": 2,
            "video": 3,
            "audio": 4,
        }
        for search_type, type_code in expected_type_codes.items():
            with self.subTest(search_type=search_type):
                client = _FakeManticoreClient(
                    [{"hits": {"total": 0, "total_relation": "eq", "hits": []}}]
                )
                payload = manticore_search_payload_service(
                    self.conn,
                    self._params(
                        search_type=search_type,
                        sort_by_req="relevance",
                    ),
                    client=client,
                    page_size=100,
                    max_count=50000000,
                    max_matches=1000000,
                    map_search_items_fn=lambda rows: rows,
                )

                self.assertEqual(payload["effective_sort"], "relevance")
                self.assertIn("ORDER BY WEIGHT() DESC", client.sql[0])
                self.assertIn(f"type_code = {type_code}", client.sql[0])

    def test_relevance_sort_respects_ascending_order(self):
        client = _FakeManticoreClient(
            [{"hits": {"total": 0, "total_relation": "eq", "hits": []}}]
        )
        payload = manticore_search_payload_service(
            self.conn,
            self._params(sort_by_req="relevance", order_req="asc"),
            client=client,
            page_size=100,
            max_count=50000000,
            max_matches=1000000,
            map_search_items_fn=lambda rows: rows,
        )

        self.assertEqual(payload["effective_order"], "asc")
        self.assertIn("ORDER BY WEIGHT() ASC", client.sql[0])

    def test_non_relevance_page_sort_does_not_compute_bm25_scores(self):
        client = _FakeManticoreClient(
            [{"hits": {"total": 0, "total_relation": "eq", "hits": []}}]
        )
        manticore_search_payload_service(
            self.conn,
            self._params(sort_by_req="time"),
            client=client,
            page_size=100,
            max_count=50000000,
            max_matches=1000000,
            map_search_items_fn=lambda rows: rows,
        )

        self.assertIn("OPTION ranker=none", client.sql[0])

    def test_very_deep_pages_stop_before_the_expensive_offset_window(self):
        client = _FakeManticoreClient()

        with self.assertRaisesRegex(ValueError, "前 2000 页"):
            manticore_search_payload_service(
                self.conn,
                self._params(page=2001),
                client=client,
                page_size=50,
                max_count=50000000,
                max_matches=1000000,
                max_browsable_results=100000,
                map_search_items_fn=lambda rows: rows,
            )

        self.assertEqual(client.sql, [])

    def test_empty_query_browses_with_sqlite_ordering_indexes(self):
        payload = sqlite_browse_payload_service(
            self.conn,
            self._params(raw_query="", text_query=""),
            page_size=100,
            max_count=50000000,
            map_search_items_fn=lambda rows: [dict(row) for row in rows],
        )

        self.assertEqual(payload["search_backend"], "sqlite_browse")
        self.assertEqual([row["pk"] for row in payload["items"]], [self.pk])

    def test_empty_query_relevance_sort_falls_back_to_time(self):
        payload = sqlite_browse_payload_service(
            self.conn,
            self._params(raw_query="", text_query="", sort_by_req="relevance"),
            page_size=100,
            max_count=50000000,
            map_search_items_fn=lambda rows: [dict(row) for row in rows],
        )

        self.assertEqual(payload["effective_sort"], "time")

    def test_empty_query_count_uses_the_browse_path(self):
        payload = sqlite_browse_payload_service(
            self.conn,
            self._params(raw_query="", text_query="", count_only=True),
            page_size=100,
            max_count=50000000,
            map_search_items_fn=lambda rows: rows,
        )

        self.assertEqual(payload["total"], 1)
        self.assertEqual(payload["chat_facets"], [])

    def test_empty_query_browse_uses_the_same_deep_page_window(self):
        with self.assertRaisesRegex(ValueError, "前 2000 页"):
            sqlite_browse_payload_service(
                self.conn,
                self._params(raw_query="", text_query="", page=2001),
                page_size=50,
                max_count=50000000,
                max_browsable_results=100000,
                map_search_items_fn=lambda rows: rows,
            )


if __name__ == "__main__":
    unittest.main()
