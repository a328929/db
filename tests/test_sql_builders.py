"""Unit tests for SQL builder utilities."""

try:
    import pytest
except ImportError:
    pytest = None

from tg_harvest.storage.sql_builders import (
    build_batch_case_update,
    build_status_transition_update,
    build_conditional_set_clauses,
    build_in_flight_state_update,
)


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
                assert False, "Should have raised ValueError"
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
                assert False, "Should have raised ValueError"
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


class TestBuildStatusTransitionUpdate:
    """Tests for build_status_transition_update()."""

    def test_basic_status_update(self):
        """Test basic status transition."""
        sql, params = build_status_transition_update(
            table='sync_jobs',
            status_field='status',
            new_status='running'
        )

        assert sql == 'UPDATE sync_jobs SET status = ?'
        assert params == ('running',)

    def test_with_set_fields(self):
        """Test status transition with additional fields."""
        sql, params = build_status_transition_update(
            table='sync_jobs',
            status_field='status',
            new_status='running',
            set_fields={'started_at': 1234567890, 'owner_pid': 12345}
        )

        assert 'UPDATE sync_jobs SET status = ?' in sql
        assert 'started_at = ?' in sql
        assert 'owner_pid = ?' in sql
        assert params == ('running', 1234567890, 12345)

    def test_with_increment_fields(self):
        """Test status transition with field increments."""
        sql, params = build_status_transition_update(
            table='sync_jobs',
            status_field='status',
            new_status='running',
            increment_fields=['attempt_count', 'retry_count']
        )

        assert 'attempt_count = attempt_count + 1' in sql
        assert 'retry_count = retry_count + 1' in sql
        assert params == ('running',)

    def test_with_where_conditions(self):
        """Test status transition with WHERE clause."""
        sql, params = build_status_transition_update(
            table='sync_jobs',
            status_field='status',
            new_status='running',
            where_conditions={'id': 42, 'status': 'pending'}
        )

        assert 'WHERE id = ? AND status = ?' in sql
        assert params == ('running', 42, 'pending')

    def test_complete_status_transition(self):
        """Test status transition with all options."""
        sql, params = build_status_transition_update(
            table='sync_jobs',
            status_field='state',
            new_status='running',
            set_fields={'started_at': 1234567890, 'owner_pid': 12345},
            increment_fields=['attempt_count'],
            where_conditions={'id': 42, 'state': 'pending'}
        )

        assert 'UPDATE sync_jobs SET state = ?' in sql
        assert 'started_at = ?' in sql
        assert 'owner_pid = ?' in sql
        assert 'attempt_count = attempt_count + 1' in sql
        assert 'WHERE id = ? AND state = ?' in sql
        assert params == ('running', 1234567890, 12345, 42, 'pending')


class TestBuildConditionalSetClauses:
    """Tests for build_conditional_set_clauses()."""

    def test_basic_conditional_clauses(self):
        """Test basic conditional SET clauses."""
        clauses = build_conditional_set_clauses(
            conditional_fields={
                'status': ('new_status IS NOT NULL', 'new.status', 'old.status'),
                'updated_at': ('new_status IS NOT NULL', '?', 'old.updated_at')
            }
        )

        assert len(clauses) == 2
        assert 'status = CASE WHEN new_status IS NOT NULL THEN new.status ELSE old.status END' in clauses
        assert 'updated_at = CASE WHEN new_status IS NOT NULL THEN ? ELSE old.updated_at END' in clauses

    def test_with_table_prefix(self):
        """Test conditional SET clauses with table prefix."""
        clauses = build_conditional_set_clauses(
            conditional_fields={
                'status': ('new_status IS NOT NULL', 'new.status', 'old.status')
            },
            table_prefix='channels.'
        )

        assert len(clauses) == 1
        assert clauses[0].startswith('channels.status = CASE WHEN')

    def test_empty_conditional_fields(self):
        """Test with empty conditional_fields."""
        clauses = build_conditional_set_clauses(
            conditional_fields={}
        )

        assert clauses == []

    def test_multiple_conditions(self):
        """Test with multiple different conditions."""
        clauses = build_conditional_set_clauses(
            conditional_fields={
                'field1': ('condition1', 'value1', 'default1'),
                'field2': ('condition2', 'value2', 'default2'),
                'field3': ('condition3', 'value3', 'default3'),
            }
        )

        assert len(clauses) == 3
        assert all('CASE WHEN' in clause for clause in clauses)
        assert all('THEN' in clause for clause in clauses)
        assert all('ELSE' in clause for clause in clauses)
        assert all('END' in clause for clause in clauses)


class TestBuildInFlightStateUpdate:
    """Tests for build_in_flight_state_update()."""

    def test_claim_work_basic(self):
        """Test claiming work (setting in_flight=True)."""
        sql, params = build_in_flight_state_update(
            table='sync_jobs',
            in_flight=True
        )

        assert 'UPDATE sync_jobs SET in_flight = ?' in sql
        assert params == (1,)

    def test_claim_work_with_ownership(self):
        """Test claiming work with full ownership information."""
        sql, params = build_in_flight_state_update(
            table='sync_jobs',
            in_flight=True,
            generation=5,
            owner_instance_id='worker-01',
            owner_pid=12345,
            owner_host='server1.example.com'
        )

        assert 'in_flight = ?' in sql
        assert 'generation = ?' in sql
        assert 'owner_instance_id = ?' in sql
        assert 'owner_pid = ?' in sql
        assert 'owner_host = ?' in sql
        assert params == (1, 5, 'worker-01', 12345, 'server1.example.com')

    def test_release_work(self):
        """Test releasing work (setting in_flight=False)."""
        sql, params = build_in_flight_state_update(
            table='sync_jobs',
            in_flight=False
        )

        assert 'in_flight = ?' in sql
        assert 'owner_instance_id = NULL' in sql
        assert 'owner_pid = NULL' in sql
        assert 'owner_host = NULL' in sql
        assert params == (0,)

    def test_claim_with_where_conditions(self):
        """Test claiming work with WHERE conditions."""
        sql, params = build_in_flight_state_update(
            table='sync_jobs',
            in_flight=True,
            generation=5,
            owner_instance_id='worker-01',
            where_conditions={'id': 42, 'in_flight': False}
        )

        assert 'WHERE id = ? AND in_flight = ?' in sql
        assert 42 in params
        assert False in params

    def test_release_with_where_conditions(self):
        """Test releasing work with WHERE conditions."""
        sql, params = build_in_flight_state_update(
            table='sync_jobs',
            in_flight=False,
            where_conditions={'id': 42, 'owner_pid': 12345}
        )

        assert 'WHERE id = ? AND owner_pid = ?' in sql
        assert params == (0, 42, 12345)

    def test_claim_partial_ownership(self):
        """Test claiming with only some ownership fields."""
        sql, params = build_in_flight_state_update(
            table='sync_jobs',
            in_flight=True,
            owner_pid=12345,
            owner_host='server1.example.com'
        )

        assert 'in_flight = ?' in sql
        assert 'owner_pid = ?' in sql
        assert 'owner_host = ?' in sql
        assert 'generation = ?' not in sql
        assert 'owner_instance_id = ?' not in sql
        assert params == (1, 12345, 'server1.example.com')


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

    def test_status_transition_injection_attempt(self):
        """Test that malicious status values are parameterized."""
        sql, params = build_status_transition_update(
            table='sync_jobs',
            status_field='status',
            new_status="running'; DELETE FROM sync_jobs; --"
        )

        assert "DELETE FROM" not in sql
        assert "running'; DELETE FROM sync_jobs; --" in params


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

    def test_status_transition_params_match_placeholders(self):
        """Test that parameter count matches placeholder count."""
        sql, params = build_status_transition_update(
            table='sync_jobs',
            status_field='status',
            new_status='running',
            set_fields={'started_at': 1111, 'owner': 'test'},
            where_conditions={'id': 42, 'status': 'pending'}
        )

        placeholder_count = sql.count('?')
        assert len(params) == placeholder_count

    def test_in_flight_params_match_placeholders(self):
        """Test that parameter count matches placeholder count."""
        sql, params = build_in_flight_state_update(
            table='sync_jobs',
            in_flight=True,
            generation=5,
            owner_instance_id='worker-01',
            owner_pid=12345,
            where_conditions={'id': 42}
        )

        placeholder_count = sql.count('?')
        assert len(params) == placeholder_count
