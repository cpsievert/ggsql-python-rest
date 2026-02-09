"""Tests for session management."""

from datetime import timezone

from ggsql_rest._sessions import Session, SessionManager


def test_session_creation():
    session = Session("test123", timeout_mins=30)
    assert session.id == "test123"
    assert session.tables == []
    assert not session.is_expired()


def test_session_touch():
    session = Session("test123", timeout_mins=30)
    first_access = session.last_accessed
    session.touch()
    assert session.last_accessed >= first_access


def test_session_expiry():
    session = Session("test123", timeout_mins=0)
    # With 0 timeout, session expires immediately
    assert session.is_expired()


def test_session_manager_create():
    mgr = SessionManager(timeout_mins=30)
    session = mgr.create()
    assert session.id is not None
    assert len(session.id) == 32  # uuid hex


def test_session_manager_get():
    mgr = SessionManager(timeout_mins=30)
    session = mgr.create()
    retrieved = mgr.get(session.id)
    assert retrieved is not None
    assert retrieved.id == session.id


def test_session_manager_get_nonexistent():
    mgr = SessionManager(timeout_mins=30)
    assert mgr.get("nonexistent") is None


def test_session_manager_delete():
    mgr = SessionManager(timeout_mins=30)
    session = mgr.create()
    assert mgr.delete(session.id) is True
    assert mgr.get(session.id) is None


def test_session_manager_delete_nonexistent():
    mgr = SessionManager(timeout_mins=30)
    assert mgr.delete("nonexistent") is False


def test_session_manager_cleanup_expired():
    mgr = SessionManager(timeout_mins=0)  # Immediate expiry
    session = mgr.create()
    session_id = session.id
    mgr.cleanup_expired()
    assert mgr.get(session_id) is None


def test_session_uses_utc():
    session = Session("test", timeout_mins=30)
    assert session.created_at.tzinfo == timezone.utc
    assert session.last_accessed.tzinfo == timezone.utc
    session.touch()
    assert session.last_accessed.tzinfo == timezone.utc


def test_create_triggers_cleanup():
    """Creating a session cleans up expired ones."""
    mgr = SessionManager(timeout_mins=0)  # Immediate expiry
    s1 = mgr.create()
    s1_id = s1.id

    # Verify s1 is still in the internal dict (not yet cleaned up)
    assert s1_id in mgr._sessions

    # s1 is now expired. Creating s2 should clean it up.
    s2 = mgr.create()
    assert s2.id != s1_id

    # Verify s1 is actually gone from internal dict (not just lazily expired on get)
    assert s1_id not in mgr._sessions
