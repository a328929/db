"""Unit tests for SQL builder utilities."""

try:
    import pytest
except ImportError:
    pytest = None

from tg_harvest.storage.sql_builders import build_batch_case_update


class TestBuildBatchCaseUpdate:
    """Tests for build_batch_case_update()."""

    def test_basic_case_update(self):
        """Test basic CASE update with single field."""
        sql, params = build_batch_case_update(
            table='test_table',
            match_field='id',
            case_updates={
                1: {'status': 'sent'},
                2: {'status': 'sent'},
            }
        )

        assert 'UPDATE test_table SET' in sql
        assert 'status = CASE id' in sql
        assert 'WHEN ? THEN ?' in sql
        assert 'WHERE id IN (?, ?)' in sql
        assert params == (1, 'sent', 2, 'sent', 1, 2)

    def test_multiple_fields_case_update(self):
        """Test CASE update with multiple fields per row."""
        sql, params = build_batch_case_update(
            table='test_table',
            match_field='message_id',
            case_updates={
                123: {'status': 'sent', 'hop_count': 2},
                456: {'status': 'sent', 'hop_count': 3},
            }
        )

        assert 'UPDATE test_table SET' in sql
        assert 'hop_count = CASE message_id' in sql
        assert 'status = CASE message_id' in sql
        assert 'WHERE message_id IN (?, ?)' in sql
        # Verify all params are present (order may vary due to sorting)
        assert 123 in params
        assert 456 in params
        assert 'sent' in params
        assert 2 in params
        assert 3 in params

    def test_with_set_fields(self):
        """Test CASE update with additional constant SET fields."""
        sql, params = build_batch_case_update(
            table='test_table',
            match_field='id',
            case_updates={
                1: {'status': 'sent'},
                2: {'status': 'sent'},
            },
            set_fields={'updated_at': 1234567890, 'processed': True}
        )

        assert 'updated_at = ?' in sql
        assert 'processed = ?' in sql
        assert 1234567890 in params
        assert True in params

    def test_with_where_conditions(self):
        """Test CASE update with WHERE conditions."""
        sql, params = build_batch_case_update(
            table='test_table',
            match_field='id',
            case_updates={
                1: {'status': 'sent'},
                2: {'status': 'sent'},
            },
            where_conditions={'clone_run_id': 42, 'active': True}
        )

        assert 'WHERE id IN (?, ?)' in sql
        assert 'clone_run_id = ?' in sql
        assert 'active = ?' in sql
        assert 42 in params
        assert True in params

    def test_with_where_clauses(self):
        """Test CASE update with raw WHERE clauses."""
        sql, params = build_batch_case_update(
            table='test_table',
            match_field='id',
            case_updates={
                1: {'status': 'sent'},
            },
            where_clauses=['hop < 3', 'error_count = 0']
        )

        assert 'hop < 3' in sql
        assert 'error_count = 0' in sql

    def test_empty_case_updates_raises_error(self):
        """Test that empty case_updates raises ValueError."""
        if pytest:
            with pytest.raises(ValueError, match="case_updates cannot be empty"):
                build_batch_case_update(
                    table='test_table',
                    match_field='id',
                    case_updates={}
                )
        else:
            try:
                build_batch_case_update(
                    table='test_table',
                    match_field='id',
                    case_updates={}
                )
                raise AssertionError("Should have raised ValueError")
            except ValueError as e:
                assert "case_updates cannot be empty" in str(e)

    def test_no_fields_in_case_updates_raises_error(self):
        """Test that case_updates with no fields raises ValueError."""
        if pytest:
            with pytest.raises(ValueError, match="No fields to update"):
                build_batch_case_update(
                    table='test_table',
                    match_field='id',
                    case_updates={1: {}, 2: {}}
                )
        else:
            try:
                build_batch_case_update(
                    table='test_table',
                    match_field='id',
                    case_updates={1: {}, 2: {}}
                )
                raise AssertionError("Should have raised ValueError")
            except ValueError as e:
                assert "No fields to update" in str(e)

    def test_partial_field_coverage(self):
        """Test CASE update where not all rows have all fields."""
        sql, params = build_batch_case_update(
            table='test_table',
            match_field='id',
            case_updates={
                1: {'status': 'sent', 'hop_count': 2},
                2: {'status': 'sent'},  # Missing hop_count
                3: {'hop_count': 3},     # Missing status
            }
        )

        # Both fields should have CASE statements
        assert 'hop_count = CASE id' in sql
        assert 'status = CASE id' in sql
        # Should handle partial coverage (some WHEN clauses per field)
        assert sql.count('WHEN ? THEN ?') >= 3


class TestSQLInjectionSafety:
    """Tests to verify SQL injection protection."""

    def test_batch_case_update_injection_attempt(self):
        """Test that malicious values are properly parameterized."""
        sql, params = build_batch_case_update(
            table='test_table',
            match_field='id',
            case_updates={
                1: {'status': "'; DROP TABLE users; --"},
            }
        )

        # Should use parameters, not string interpolation
        assert "DROP TABLE" not in sql
        assert "'; DROP TABLE users; --" in params

class TestParameterOrdering:
    """Tests to verify parameter ordering matches placeholders."""

    def test_batch_case_params_match_placeholders(self):
        """Test that parameter count matches placeholder count."""
        sql, params = build_batch_case_update(
            table='test_table',
            match_field='id',
            case_updates={
                1: {'status': 'sent', 'hop': 2},
                2: {'status': 'sent', 'hop': 3},
            },
            set_fields={'updated_at': 9999},
            where_conditions={'run_id': 42}
        )

        placeholder_count = sql.count('?')
        assert len(params) == placeholder_count
