"""Integration tests for sql_builders with clone_state_media_transfers."""

import sqlite3
import tempfile
from pathlib import Path

from tg_harvest.storage.clone_state_media_transfers import (
    ensure_clone_media_transfers,
    mark_clone_media_transfer_source_hop_sent,
    mark_clone_media_transfer_target_hop_sent,
    mark_clone_media_transfer_target_hop_observed,
)


def _create_test_db():
    """Create a temporary database with required schema."""
    tmpfile = tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.db')
    tmpfile.close()

    conn = sqlite3.connect(tmpfile.name)
    conn.row_factory = sqlite3.Row  # Enable column name access
    cur = conn.cursor()

    # Create the media transfers table
    cur.execute("""
        CREATE TABLE admin_clone_media_transfers (
            migration_id TEXT NOT NULL,
            run_id TEXT NOT NULL,
            plan_id TEXT NOT NULL,
            source_chat_id INTEGER NOT NULL,
            source_message_id INTEGER NOT NULL,
            target_chat_id INTEGER NOT NULL,
            transfer_strategy TEXT NOT NULL,
            relay_chat_id INTEGER,
            source_account TEXT,
            target_account TEXT,
            source_random_id INTEGER,
            target_random_id INTEGER,
            relay_message_id INTEGER,
            target_message_id INTEGER,
            source_hop_status TEXT NOT NULL,
            target_hop_status TEXT NOT NULL,
            cleanup_status TEXT NOT NULL,
            error_message TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
    """)

    conn.commit()
    cur.close()

    return conn, tmpfile.name


def test_mark_source_hop_sent_batch_update():
    """Test mark_clone_media_transfer_source_hop_sent with batch CASE update."""
    conn, db_path = _create_test_db()

    try:
        # Register relay transfers
        ensure_clone_media_transfers(
            conn,
            migration_id='mig1',
            run_id='run1',
            plan_id='plan1',
            source_chat_id=100,
            source_message_ids=[1, 2, 3],
            target_chat_id=200,
            transfer_strategy='relay',
            relay_chat_id=150,
            source_account='acc1',
            target_account='acc2',
        )

        # Mark source hop as sent with relay message IDs
        mark_clone_media_transfer_source_hop_sent(
            conn,
            run_id='run1',
            source_chat_id=100,
            relay_message_ids_by_source={1: 1001, 2: 1002, 3: 1003}
        )

        # Verify updates
        cur = conn.cursor()
        cur.execute("""
            SELECT source_message_id, relay_message_id, source_hop_status, cleanup_status
            FROM admin_clone_media_transfers
            WHERE run_id = 'run1'
            ORDER BY source_message_id
        """)
        rows = cur.fetchall()

        assert len(rows) == 3
        assert tuple(rows[0]) == (1, 1001, 'sent', 'pending')
        assert tuple(rows[1]) == (2, 1002, 'sent', 'pending')
        assert tuple(rows[2]) == (3, 1003, 'sent', 'pending')

        print('✓ test_mark_source_hop_sent_batch_update passed')

    finally:
        conn.close()
        Path(db_path).unlink()


def test_mark_target_hop_sent_batch_update():
    """Test mark_clone_media_transfer_target_hop_sent with batch CASE update."""
    conn, db_path = _create_test_db()

    try:
        # Register direct transfers
        ensure_clone_media_transfers(
            conn,
            migration_id='mig1',
            run_id='run1',
            plan_id='plan1',
            source_chat_id=100,
            source_message_ids=[10, 20, 30],
            target_chat_id=200,
            transfer_strategy='direct',
            relay_chat_id=None,
            source_account='acc1',
            target_account='acc2',
        )

        # Mark target hop as sent with target message IDs
        mark_clone_media_transfer_target_hop_sent(
            conn,
            run_id='run1',
            source_chat_id=100,
            target_message_ids_by_source={10: 2010, 20: 2020, 30: 2030}
        )

        # Verify updates
        cur = conn.cursor()
        cur.execute("""
            SELECT source_message_id, target_message_id, target_hop_status
            FROM admin_clone_media_transfers
            WHERE run_id = 'run1'
            ORDER BY source_message_id
        """)
        rows = cur.fetchall()

        assert len(rows) == 3
        assert tuple(rows[0]) == (10, 2010, 'sent')
        assert tuple(rows[1]) == (20, 2020, 'sent')
        assert tuple(rows[2]) == (30, 2030, 'sent')

        print('✓ test_mark_target_hop_sent_batch_update passed')

    finally:
        conn.close()
        Path(db_path).unlink()


def test_mark_target_hop_observed_batch_update():
    """Test mark_clone_media_transfer_target_hop_observed with batch CASE update."""
    conn, db_path = _create_test_db()

    try:
        # Register direct transfers
        ensure_clone_media_transfers(
            conn,
            migration_id='mig1',
            run_id='run1',
            plan_id='plan1',
            source_chat_id=100,
            source_message_ids=[40, 50, 60],
            target_chat_id=200,
            transfer_strategy='direct',
            relay_chat_id=None,
            source_account='acc1',
            target_account='acc2',
        )

        # Mark target hop as observed (unconfirmed) with target message IDs
        mark_clone_media_transfer_target_hop_observed(
            conn,
            run_id='run1',
            source_chat_id=100,
            target_message_ids_by_source={40: 2040, 50: 2050, 60: 2060}
        )

        # Verify updates
        cur = conn.cursor()
        cur.execute("""
            SELECT source_message_id, target_message_id, target_hop_status
            FROM admin_clone_media_transfers
            WHERE run_id = 'run1'
            ORDER BY source_message_id
        """)
        rows = cur.fetchall()

        assert len(rows) == 3
        assert tuple(rows[0]) == (40, 2040, 'unconfirmed')
        assert tuple(rows[1]) == (50, 2050, 'unconfirmed')
        assert tuple(rows[2]) == (60, 2060, 'unconfirmed')

        print('✓ test_mark_target_hop_observed_batch_update passed')

    finally:
        conn.close()
        Path(db_path).unlink()


def test_mark_target_hop_observed_respects_sent_status():
    """Test that mark_target_hop_observed doesn't overwrite 'sent' status."""
    conn, db_path = _create_test_db()

    try:
        # Register direct transfers
        ensure_clone_media_transfers(
            conn,
            migration_id='mig1',
            run_id='run1',
            plan_id='plan1',
            source_chat_id=100,
            source_message_ids=[70, 80],
            target_chat_id=200,
            transfer_strategy='direct',
            relay_chat_id=None,
            source_account='acc1',
            target_account='acc2',
        )

        # Mark one as sent first
        mark_clone_media_transfer_target_hop_sent(
            conn,
            run_id='run1',
            source_chat_id=100,
            target_message_ids_by_source={70: 2070}
        )

        # Try to mark only the pending one as observed (should succeed)
        mark_clone_media_transfer_target_hop_observed(
            conn,
            run_id='run1',
            source_chat_id=100,
            target_message_ids_by_source={80: 2080}  # Only the one not yet sent
        )

        # Verify the statuses
        cur = conn.cursor()
        cur.execute("""
            SELECT source_message_id, target_message_id, target_hop_status
            FROM admin_clone_media_transfers
            WHERE run_id = 'run1'
            ORDER BY source_message_id
        """)
        rows = cur.fetchall()

        assert len(rows) == 2
        assert tuple(rows[0]) == (70, 2070, 'sent')  # Should remain 'sent'
        assert tuple(rows[1]) == (80, 2080, 'unconfirmed')

        print('✓ test_mark_target_hop_observed_respects_sent_status passed')

    finally:
        conn.close()
        Path(db_path).unlink()


if __name__ == '__main__':
    test_mark_source_hop_sent_batch_update()
    test_mark_target_hop_sent_batch_update()
    test_mark_target_hop_observed_batch_update()
    test_mark_target_hop_observed_respects_sent_status()
    print('\n=== All integration tests passed ===')
