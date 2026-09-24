"""Per-set state machine. Every change of state is an event."""

from __future__ import annotations

import time
from collections.abc import Callable

import structlog

from dbguard.events import Event, EventLog
from dbguard.manager.model import State

log = structlog.get_logger("dbguard.manager.state")

# state entered -> event type recorded. FAILING_OVER and REBUILDING are recorded by the
# failover and rebuild events themselves, which carry the detail.
_EVENT_FOR = {
    State.SUSPECT: "suspect",
    State.DEGRADED: "degraded",
    State.HEALTHY: "healthy",
    State.HALTED: "halt",
}


class SetState:
    def __init__(self, rs: str, mode: str, events: EventLog,
                 on_change: Callable[[str, State, State], None] | None = None,
                 clock: Callable[[], float] = time.time):
        self.rs = rs
        self.mode = mode
        self.events = events
        self.clock = clock
        self.on_change = on_change
        # Every set starts DISCOVERING and leaves it only through discovery, bootstrap or
        # cold start. HEALTHY with no primary once fooled `make wait-healthy` on a fleet
        # whose mysqld were still initialising.
        self.state = State.DISCOVERING
        self.since = clock()
        self.halt_reason: str | None = None
        self.primary: str | None = None
        self.cooldown_until: float | None = None
        self.failovers_total = 0
        self.note: str | None = None

    @property
    def halted(self) -> bool:
        return self.state == State.HALTED

    def to(self, new: State, note: str | None = None, emit: bool = True, **event_kw) -> bool:
        """Move to ``new``. Returns True if the state changed.

        HALTED is sticky: only resume() leaves it. A human has to look first.
        """
        if self.state == new:
            return False
        if self.state == State.HALTED:
            log.warning("ignored transition out of HALTED", rs=self.rs, to=new.value)
            return False
        if new == State.HEALTHY and self.primary is None:
            log.warning("refused HEALTHY without a primary", rs=self.rs, frm=self.state.value)
            return False
        old = self.state
        self.state = new
        self.since = self.clock()
        self.note = note
        if new == State.HALTED:
            self.halt_reason = note or "halted"
        log.info("state", rs=self.rs, frm=old.value, to=new.value, note=note)
        if emit and new in _EVENT_FOR:
            self.events.append(Event(rs=self.rs, type=_EVENT_FOR[new], mode=self.mode,
                                     old_primary=self.primary, new_primary=self.primary,
                                     note=note, **event_kw))
        if self.on_change:
            self.on_change(self.rs, old, new)
        return True

    def halt(self, reason: str, **event_kw) -> None:
        self.to(State.HALTED, note=reason, **event_kw)

    def resume(self, note: str | None = None) -> bool:
        if self.state != State.HALTED:
            return False
        old = self.state
        new = State.HEALTHY if self.primary is not None else State.DISCOVERING
        self.state = new
        self.since = self.clock()
        reason = self.halt_reason
        self.halt_reason = None
        self.note = note
        self.events.append(Event(rs=self.rs, type="resume", mode=self.mode,
                                 old_primary=self.primary, new_primary=self.primary,
                                 note=note or f"resumed by operator (was: {reason})"))
        if self.on_change:
            self.on_change(self.rs, old, new)
        return True

    def in_cooldown(self, now: float | None = None) -> bool:
        now = self.clock() if now is None else now
        return self.cooldown_until is not None and now < self.cooldown_until
