"""Approval workflow as an explicit state machine (pure domain).

State lives in Delta (runtime), but the *rules* of valid transitions are domain
logic and belong here — testable without any infrastructure.
"""
from __future__ import annotations

from ..model import CaseState, StewardshipCase

INVALID_TRANSITION = "invalid_transition"

_TRANSITIONS: dict[CaseState, set[CaseState]] = {
    CaseState.OPEN: {CaseState.IN_REVIEW, CaseState.REJECTED},
    CaseState.IN_REVIEW: {CaseState.APPROVED, CaseState.REJECTED, CaseState.OPEN},
    CaseState.APPROVED: set(),
    CaseState.REJECTED: set(),
}


class ApprovalStateMachine:
    @staticmethod
    def can_transition(frm: CaseState, to: CaseState) -> bool:
        return to in _TRANSITIONS.get(frm, set())

    @staticmethod
    def transition(case: StewardshipCase, to: CaseState, actor: str,
                   note: str = "") -> StewardshipCase:
        if not ApprovalStateMachine.can_transition(case.state, to):
            raise ValueError(f"{INVALID_TRANSITION}: {case.state} -> {to}")
        case.decisions.append({
            "from": case.state.value, "to": to.value,
            "actor": actor, "note": note,
        })
        case.state = to
        return case
