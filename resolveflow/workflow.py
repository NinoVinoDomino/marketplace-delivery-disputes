import asyncio
from collections.abc import Mapping
from typing import Any, Literal, TypedDict
from uuid import uuid4

from resolveflow.agents import Agents, ProviderError
from resolveflow.evidence import Evidence, FactName, Turn
from resolveflow.orders import Order, RefundLedger
from resolveflow.policy_diff import Snapshot


class CaseState(TypedDict, total=False):
    case_id: str
    order_id: str
    policy_version: str
    policy_hash: str
    messages: list[str]
    message_ids: list[str]
    turns: list[dict]
    evidence: dict
    clarification: dict
    stage: str
    reply: str
    citations: list[str]
    review_reason: str
    review_reply: str
    approval: bool
    refund_id: str


MAX_MESSAGES = 3
QUESTIONS: dict[FactName, str] = {
    "parcel_missing": "Is the parcel still missing?",
    "carrier_checked": "Have you contacted the carrier about the missing parcel?",
}


def case_turns(state: CaseState) -> list[Turn]:
    if "turns" in state:
        return [Turn.model_validate(turn) for turn in state["turns"]]
    ids = [state["case_id"], *state["message_ids"]]
    return [Turn(message_id=key, text=text) for key, text in zip(ids, state["messages"], strict=True)]


def evidence_values(evidence: Evidence) -> dict[str, bool | None]:
    return {fact: statement["value"] if statement else None for fact, statement in evidence.model_dump().items()}


def clarification_fact(
    snapshot: Snapshot, order: Order, facts: dict[str, bool | None], turn_count: int,
) -> FactName | None:
    if (
        turn_count >= MAX_MESSAGES or facts["parcel_missing"] is False
        or not snapshot.refund_criteria.order_matches(order)
    ):
        return None
    return next((fact for fact in snapshot.refund_criteria.required_facts if facts[fact] is not True), None)


def assessment_stage(snapshot: Snapshot, order: Order, facts: dict[str, bool | None]) -> str:
    if facts["parcel_missing"] is False:
        return "resolved"
    return "awaiting_operator" if snapshot.refund_criteria.matches(order, facts) else "manual_review"


def build_graph(
    agents: Agents,
    orders: Mapping[str, Order],
    policies: Mapping[str, Snapshot],
    ledger: RefundLedger,
    checkpointer: Any,
):
    from langgraph.graph import END, START, StateGraph
    from langgraph.types import interrupt

    def policy(state: CaseState) -> Snapshot:
        snapshot = policies[state["policy_version"]]
        if state.get("policy_hash", snapshot.digest) != snapshot.digest:
            raise ValueError("Pinned policy snapshot has changed")
        return snapshot

    def eligible(state: CaseState, evidence: Evidence) -> bool:
        return policy(state).refund_criteria.matches(orders[state["order_id"]], evidence_values(evidence))

    async def collect(state: CaseState) -> dict:
        snapshot = policy(state)
        order = orders[state["order_id"]]
        if order.status == "in_transit":
            return {
                "stage": "resolved",
                "reply": "Your parcel is still in transit.",
                "citations": ["delivery.in_transit"],
                "policy_hash": snapshot.digest,
            }
        turns = case_turns(state)
        try:
            evidence = await agents.evidence(turns)
        except ProviderError as error:
            if error.code not in {"INVALID_MODEL_OUTPUT", "UNGROUNDED_MODEL_OUTPUT"}:
                raise
            return {
                **manual_review("evidence_extraction_failed", snapshot.digest),
                "evidence": {}, "citations": ["delivery.evidence"],
            }
        fact = clarification_fact(snapshot, order, evidence_values(evidence), len(turns))
        question = {"id": str(uuid4()), "target_fact": fact, "question": QUESTIONS[fact]} if fact else {}
        return {
            "evidence": evidence.model_dump(),
            "stage": "assessing",
            "clarification": question,
            "policy_hash": snapshot.digest,
        }

    def route_evidence(state: CaseState) -> Literal["clarify", "assess", "__end__"]:
        if state["stage"] in {"resolved", "manual_review"}:
            return END
        return "clarify" if state.get("clarification") else "assess"

    def clarify(state: CaseState) -> dict:
        question = state.get("clarification")
        if not question:
            # Previously suspended questions keep their original meaning after upgrade.
            if state["evidence"]["parcel_missing"] is None:
                question = {"question": "Is the parcel still missing, and have you contacted the carrier?"}
            else:
                question = {"question": QUESTIONS["carrier_checked"], "target_fact": "carrier_checked"}
        message = interrupt({"type": "clarification", **question})
        turn = Turn(
            message_id=message["id"], text=message["text"],
            clarification_id=message.get("clarification_id"), question=question["question"],
            target_fact=question.get("target_fact"),
        )
        return {
            "messages": [*state["messages"], turn.text],
            "message_ids": [*state["message_ids"], turn.message_id],
            "turns": [previous.model_dump() for previous in case_turns(state)] + [turn.model_dump()],
        }

    def manual_review(reason: str, policy_hash: str) -> dict:
        return {
            "stage": "manual_review", "reply": "Your case is queued for manual review.",
            "review_reason": reason, "citations": ["refund.approval"], "policy_hash": policy_hash,
        }

    async def assess(state: CaseState) -> dict:
        snapshot = policy(state)
        try:
            evidence = Evidence.model_validate(state["evidence"])
        except ValueError:
            evidence = await agents.evidence(case_turns(state))
        saved = {"evidence": evidence.model_dump(), "policy_hash": snapshot.digest}
        stage = assessment_stage(snapshot, orders[state["order_id"]], evidence_values(evidence))
        if stage == "resolved":
            return {**saved, "stage": "resolved", "reply": "Thanks for confirming your parcel arrived.", "citations": []}
        if stage == "manual_review":
            return {**saved, **manual_review("refund_criteria_not_met", snapshot.digest)}
        return {
            **saved, "stage": "awaiting_operator",
            "reply": "Your case is queued for a refund decision.",
            "review_reason": "refund_criteria_met",
            "citations": ["refund.approval"],
        }

    def route_assessment(state: CaseState) -> Literal["approve", "__end__"]:
        return "approve" if state["stage"] == "awaiting_operator" else END

    def approve(state: CaseState) -> dict:
        approved = interrupt({
            "type": "approval",
            "order_id": state["order_id"],
            "policy_version": state["policy_version"],
        })
        return {"approval": approved is True, "policy_hash": policy(state).digest}

    async def finish(state: CaseState) -> dict:
        if not state["approval"]:
            return {"stage": "refund_declined", "reply": "The refund was not approved."}
        try:
            evidence = Evidence.model_validate(state["evidence"])
        except ValueError:
            evidence = await agents.evidence(case_turns(state))
        if not eligible(state, evidence):
            return {"stage": "refund_declined", "reply": "The refund was not approved."}
        refund_id = await asyncio.to_thread(ledger.execute, state["case_id"], state["order_id"])
        if refund_id is None:
            return {"stage": "refund_already_issued", "reply": "A refund already exists for this order."}
        return {"stage": "refund_issued", "refund_id": refund_id, "reply": "Your demo refund was issued."}

    builder = StateGraph(CaseState)
    builder.add_node("collect", collect)
    builder.add_node("clarify", clarify)
    builder.add_node("assess", assess)
    builder.add_node("approve", approve)
    builder.add_node("finish", finish)
    builder.add_edge(START, "collect")
    builder.add_conditional_edges("collect", route_evidence)
    builder.add_edge("clarify", "collect")
    builder.add_conditional_edges("assess", route_assessment)
    builder.add_edge("approve", "finish")
    builder.add_edge("finish", END)
    return builder.compile(checkpointer=checkpointer)
