import json
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from enum import Enum
from time import perf_counter

import grpc

from resolveflow.proto import guardrails_pb2 as pb
from resolveflow.proto.guardrails_pb2_grpc import GuardrailServiceStub


MAX_TEXT_LENGTH = 4_000
logger = logging.getLogger(__name__)


class Decision(str, Enum):
    ALLOW = "allow"
    BLOCK = "block"
    UNAVAILABLE = "unavailable"


REASONS = {
    pb.PROMPT_INJECTION: "prompt_injection",
    pb.BANNED_WORD: "banned_word",
}


@dataclass(frozen=True)
class Verdict:
    decision: Decision
    policy_version: str = ""
    reasons: tuple[str, ...] = ()
    error_code: str = ""


def valid_message(text: str) -> bool:
    return isinstance(text, str) and bool(text.strip()) and len(text) <= MAX_TEXT_LENGTH


class GuardrailClient:
    def __init__(self, channel: grpc.aio.Channel, timeout_seconds: float = 0.5):
        self.stub = GuardrailServiceStub(channel)
        self.timeout_seconds = timeout_seconds

    async def inspect(self, text: str) -> Verdict:
        try:
            response = await self.stub.Inspect(
                pb.InspectRequest(text=text), timeout=self.timeout_seconds
            )
        except grpc.aio.AioRpcError as error:
            return Verdict(Decision.UNAVAILABLE, error_code=error.code().name)

        if (
            response.policy_version
            and all(reason in REASONS for reason in response.reasons)
        ):
            if response.decision == pb.ALLOW and not response.reasons:
                return Verdict(Decision.ALLOW, response.policy_version)
            if response.decision == pb.BLOCK and response.reasons:
                reasons = tuple(REASONS[reason] for reason in response.reasons)
                return Verdict(Decision.BLOCK, response.policy_version, reasons)
        return Verdict(Decision.UNAVAILABLE, error_code="INVALID_VERDICT")


async def submit_message(
    text: str,
    guardrail: GuardrailClient,
    dispatch: Callable[[str], Awaitable[None]],
    *,
    request_id: str,
) -> Verdict:
    """Gate every new user message, including a clarification."""
    if not valid_message(text):
        raise ValueError("Message must contain 1–4000 characters")

    started = perf_counter()
    verdict = await guardrail.inspect(text)
    event = {
        "event": "guardrail_verdict",
        "request_id": request_id,
        "decision": verdict.decision.value,
        "policy_version": verdict.policy_version,
        "reasons": verdict.reasons,
        "error_code": verdict.error_code,
        "duration_ms": round((perf_counter() - started) * 1_000, 2),
    }
    level = logging.WARNING if verdict.decision is Decision.UNAVAILABLE else logging.INFO
    logger.log(level, "%s", json.dumps(event))

    if verdict.decision is Decision.ALLOW:
        await dispatch(text)
    return verdict
