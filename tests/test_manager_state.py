"""SetState transitions and their events."""

from __future__ import annotations

from dbguard.events import EventLog
from dbguard.manager.model import State
from dbguard.manager.state import SetState


def make(tmp_path=None):
    log = EventLog(tmp_path / "e.jsonl" if tmp_path else None)
    return SetState("rs1", "dbguard", log), log


def test_starts_discovering_and_never_healthy_without_primary():
    st, log = make()
    assert st.state == State.DISCOVERING
    assert not st.to(State.HEALTHY)
    assert st.state == State.DISCOVERING
    st.primary = "mysql-a1"
    assert st.to(State.HEALTHY)
    assert [e.type for e in log.query()] == ["healthy"]


def test_resume_without_primary_goes_back_to_discovering():
    st, _ = make()
    st.halt("x")
    assert st.resume()
    assert st.state == State.DISCOVERING


def test_transitions_emit_events(tmp_path):
    st, log = make(tmp_path)
    st.primary = "mysql-a1"
    assert st.to(State.SUSPECT, note="probe failed")
    assert not st.to(State.SUSPECT)                     # no duplicate event
    assert st.to(State.HEALTHY)
    assert st.to(State.FAILING_OVER, emit=False)
    assert st.to(State.DEGRADED)
    types = [e.type for e in log.query()]
    assert types == ["suspect", "healthy", "degraded"]
    assert all(e.old_primary == "mysql-a1" for e in log.query())
    # persisted
    assert [e.type for e in EventLog(tmp_path / "e.jsonl").query()] == types


def test_halted_is_sticky_until_resume():
    st, log = make()
    st.primary = "mysql-a1"
    st.halt("replicas diverged: x")
    assert st.state == State.HALTED and st.halt_reason == "replicas diverged: x"
    assert not st.to(State.HEALTHY)
    assert st.state == State.HALTED
    assert st.resume()
    assert st.state == State.HEALTHY and st.halt_reason is None
    assert [e.type for e in log.query()] == ["halt", "resume"]
    assert "replicas diverged" in log.query()[-1].note
    assert not st.resume()


def test_cooldown():
    now = [100.0]
    st = SetState("rs1", "dbguard", EventLog(None), clock=lambda: now[0])
    assert not st.in_cooldown()
    st.cooldown_until = 120.0
    assert st.in_cooldown()
    now[0] = 121.0
    assert not st.in_cooldown()


def test_on_change_callback():
    seen = []
    st = SetState("rs1", "dbguard", EventLog(None),
                  on_change=lambda rs, a, b: seen.append((a, b)))
    st.primary = "mysql-a1"
    st.to(State.DEGRADED)
    st.halt("x")
    st.resume()
    assert seen == [(State.DISCOVERING, State.DEGRADED), (State.DEGRADED, State.HALTED),
                    (State.HALTED, State.HEALTHY)]
