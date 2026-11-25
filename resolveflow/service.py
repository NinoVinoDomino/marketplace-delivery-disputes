"""gRPC boundary for the dispute workflow."""

import asyncio
import hmac
import json
import logging
from collections.abc import Awaitable, Callable
from time import perf_counter
from uuid import UUID, uuid4

import grpc
from langgraph.types import Command

from resolveflow.agents import ProviderError
from resolveflow.guardrails import Decision, GuardrailClient, submit_message, valid_message
from resolveflow.orders import ORDERS
from resolveflow.proto import disputes_pb2 as pb
from resolveflow.proto.disputes_pb2_grpc import DisputeServiceServicer
from resolveflow.settings import Settings
from resolveflow.workflow import case_turns


logger = logging.getLogger(__name__)
STAGES = {
    "awaiting_operator": pb.AWAITING_OPERATOR,
    "manual_review": pb.MANUAL_REVIEW,
    "resolved": pb.RESOLVED,
    "refund_declined": pb.REFUND_DECLINED,
    "refund_issued": pb.REFUND_ISSUED,
    "refund_already_issued": pb.REFUND_ALREADY_ISSUED,
}


class ServiceError(Exception):
    def __init__(self, code: grpc.StatusCode, detail: str):
        self.code = code
        self.detail = detail
        super().__init__(detail)


def request_uuid(value: str, field: str) -> str:
    try:
        return str(UUID(value))
    except ValueError:
        raise ServiceError(grpc.StatusCode.INVALID_ARGUMENT, f"{field} must be a UUID") from None


def pending_interrupts(snapshot):
    return [
        pending for task in snapshot.tasks if task.result is None
        for pending in task.interrupts
    ]


def response_for(snapshot) -> pb.Case:
    state = snapshot.values
    stage = STAGES.get(state.get("stage"), pb.PROCESSING)
    reply = state.get("reply", "")
    citations = state.get("citations", [])
    clarification_id = ""
    pending_input = pending_interrupts(snapshot)
    for pending in pending_input:
        if pending.value["type"] == "clarification":
            stage = pb.NEEDS_CLARIFICATION
            reply = pending.value["question"]
            clarification_id = pending.value.get("id") or pending.id
            citations = ["delivery.evidence"]
    if snapshot.tasks and not pending_input:
        stage = pb.PROCESSING
        reply = "Your case is being processed."
    elif "review_reason" not in state:
        if stage == pb.MANUAL_REVIEW:
            reply = "Your case is queued for manual review."
        elif stage == pb.AWAITING_OPERATOR:
            reply = "Your case is queued for a refund decision."
    return pb.Case(
        case_id=state["case_id"], order_id=state["order_id"],
        stage=stage, policy_version=state["policy_version"], reply=reply,
        citations=citations, refund_id=state.get("refund_id", ""),
        clarification_id=clarification_id, policy_hash=state.get("policy_hash", ""),
    )


class DisputeService(DisputeServiceServicer):
    def __init__(self, graph, guardrail: GuardrailClient, settings: Settings, policies, saver):
        self.graph = graph
        self.guardrail = guardrail
        self.settings = settings
        self.policies = policies
        self.saver = saver
        self.locks = [asyncio.Lock() for _ in range(256)]

    def authorize(self, metadata, roles: tuple[str, ...]) -> None:
        values = [value for key, value in metadata if key == "authorization"]
        if len(values) != 1 or not values[0].startswith("Bearer "):
            raise ServiceError(grpc.StatusCode.UNAUTHENTICATED, "Bearer token required")
        token = values[0][7:].encode()
        if hmac.compare_digest(token, self.settings.customer_token.encode()):
            role = "customer"
        elif hmac.compare_digest(token, self.settings.operator_token.encode()):
            role = "operator"
        else:
            raise ServiceError(grpc.StatusCode.UNAUTHENTICATED, "Invalid bearer token")
        if role not in roles:
            raise ServiceError(grpc.StatusCode.PERMISSION_DENIED, "Role cannot use this method")

    async def run(self, method, context, raw_case_id, roles, operation) -> pb.Case:
        request_id = str(uuid4())
        await context.send_initial_metadata((("x-request-id", request_id),))
        started = perf_counter()
        status = grpc.StatusCode.OK
        stage = ""
        error_code = ""
        try:
            self.authorize(context.invocation_metadata(), roles)
            case_id = request_uuid(raw_case_id, "case_id")
            config = {"configurable": {"thread_id": case_id}}
            async with self.case_lock(case_id):
                response = await operation(config, case_id, request_id)
            stage = pb.Stage.Name(response.stage)
            return response
        except ServiceError as error:
            status = error.code
            await context.abort(status, error.detail)
        except ProviderError as error:
            status = grpc.StatusCode.UNAVAILABLE if error.retryable else grpc.StatusCode.FAILED_PRECONDITION
            error_code = error.code
            await context.abort(status, "Model provider could not complete the current step")
        except asyncio.CancelledError:
            status = grpc.StatusCode.CANCELLED
            raise
        except Exception as error:
            status = grpc.StatusCode.INTERNAL
            error_code = type(error).__name__
            await context.abort(status, "Workflow processing failed")
        finally:
            logger.log(
                logging.INFO if status == grpc.StatusCode.OK else logging.WARNING,
                "%s",
                json.dumps({
                    "event": "dispute_rpc", "request_id": request_id, "method": method,
                    "status": status.name, "error_code": error_code, "stage": stage,
                    "duration_ms": round((perf_counter() - started) * 1_000, 2),
                }),
            )

    def case_lock(self, case_id):
        return self.locks[UUID(case_id).int % len(self.locks)]

    async def case_ids(self):
        return sorted({
            item.config["configurable"]["thread_id"]
            async for item in self.saver.alist(None)
        })

    async def recover(self, case_ids):
        for case_id in case_ids:
            async with self.case_lock(case_id):
                config = {"configurable": {"thread_id": case_id}}
                try:
                    snapshot = await self.graph.aget_state(config)
                    await self.resume_pending(config, snapshot)
                except Exception as error:
                    logger.warning("Recovery failed for case %s (%s)", case_id, type(error).__name__)

    async def resume_pending(self, config, snapshot):
        if snapshot.tasks and not pending_interrupts(snapshot):
            await self.graph.ainvoke(None, config, durability="sync")
            return await self.existing(config)
        return snapshot

    async def accept(
        self, text: str, dispatch: Callable[[str], Awaitable[None]], request_id: str,
    ) -> None:
        if not valid_message(text):
            raise ServiceError(grpc.StatusCode.INVALID_ARGUMENT, "Message must contain 1–4000 characters")
        verdict = await submit_message(text, self.guardrail, dispatch, request_id=request_id)
        if verdict.decision is Decision.BLOCK:
            raise ServiceError(grpc.StatusCode.PERMISSION_DENIED, "Message rejected by content policy")
        if verdict.decision is Decision.UNAVAILABLE:
            raise ServiceError(grpc.StatusCode.UNAVAILABLE, "Message screening unavailable")

    async def existing(self, config):
        snapshot = await self.graph.aget_state(config)
        if "case_id" not in snapshot.values:
            raise ServiceError(grpc.StatusCode.NOT_FOUND, "Case not found")
        return snapshot

    async def OpenCase(self, request, context):
        async def operation(config, case_id, request_id):
            snapshot = await self.graph.aget_state(config)
            if "case_id" in snapshot.values:
                if (snapshot.values["order_id"], snapshot.values["messages"][0]) != (request.order_id, request.text):
                    raise ServiceError(grpc.StatusCode.ALREADY_EXISTS, "Case ID already used")
                return response_for(await self.resume_pending(config, snapshot))
            if request.order_id not in ORDERS:
                raise ServiceError(grpc.StatusCode.NOT_FOUND, "Order not found")

            async def dispatch(text):
                await self.graph.ainvoke({
                    "case_id": case_id, "order_id": request.order_id, "messages": [text],
                    "message_ids": [], "policy_version": self.settings.active_policy_version,
                    "policy_hash": self.policies[self.settings.active_policy_version].digest,
                }, config, durability="sync")

            await self.accept(request.text, dispatch, request_id)
            return response_for(await self.existing(config))

        return await self.run("OpenCase", context, request.case_id, ("customer",), operation)

    async def AddMessage(self, request, context):
        async def operation(config, case_id, request_id):
            message_id = request_uuid(request.message_id, "message_id")
            if not request.clarification_id:
                raise ServiceError(grpc.StatusCode.INVALID_ARGUMENT, "clarification_id is required")
            snapshot = await self.existing(config)
            state = snapshot.values
            if message_id == case_id:
                raise ServiceError(grpc.StatusCode.ALREADY_EXISTS, "Message ID already used")
            if message_id in state["message_ids"]:
                original = next(turn for turn in case_turns(state) if turn.message_id == message_id)
                if original.text != request.text or (
                    original.clarification_id is not None and original.clarification_id != request.clarification_id
                ):
                    raise ServiceError(grpc.StatusCode.ALREADY_EXISTS, "Message ID already used")
                return response_for(await self.resume_pending(config, snapshot))
            current = response_for(snapshot)
            if current.stage != pb.NEEDS_CLARIFICATION or current.clarification_id != request.clarification_id:
                raise ServiceError(grpc.StatusCode.FAILED_PRECONDITION, "Clarification is no longer active")

            async def dispatch(text):
                await self.graph.ainvoke(
                    Command(resume={"id": message_id, "text": text, "clarification_id": request.clarification_id}),
                    config, durability="sync",
                )

            await self.accept(request.text, dispatch, request_id)
            return response_for(await self.existing(config))

        return await self.run("AddMessage", context, request.case_id, ("customer",), operation)

    async def DecideRefund(self, request, context):
        async def operation(config, case_id, request_id):
            if not request.HasField("approved"):
                raise ServiceError(grpc.StatusCode.INVALID_ARGUMENT, "An approval decision is required")
            snapshot = await self.existing(config)
            if "approval" in snapshot.values:
                if snapshot.values["approval"] != request.approved:
                    raise ServiceError(grpc.StatusCode.ALREADY_EXISTS, "A different decision was already recorded")
                return response_for(await self.resume_pending(config, snapshot))
            if response_for(snapshot).stage != pb.AWAITING_OPERATOR or not pending_interrupts(snapshot):
                raise ServiceError(grpc.StatusCode.FAILED_PRECONDITION, "Case is not waiting for approval")
            await self.graph.ainvoke(Command(resume=request.approved), config, durability="sync")
            return response_for(await self.existing(config))

        return await self.run("DecideRefund", context, request.case_id, ("operator",), operation)

    async def GetCase(self, request, context):
        async def operation(config, case_id, request_id):
            return response_for(await self.existing(config))

        return await self.run("GetCase", context, request.case_id, ("customer", "operator"), operation)

    async def RetryCase(self, request, context):
        async def operation(config, case_id, request_id):
            snapshot = await self.existing(config)
            return response_for(await self.resume_pending(config, snapshot))

        return await self.run("RetryCase", context, request.case_id, ("operator",), operation)

    async def ListReviews(self, request, context):
        try:
            self.authorize(context.invocation_metadata(), ("operator",))
        except ServiceError as error:
            await context.abort(error.code, error.detail)
        reviews = []
        for case_id in await self.case_ids():
            snapshot = await self.graph.aget_state({"configurable": {"thread_id": case_id}})
            if "case_id" not in snapshot.values:
                continue
            dispute = response_for(snapshot)
            if dispute.stage not in {pb.MANUAL_REVIEW, pb.AWAITING_OPERATOR}:
                continue
            evidence = [
                pb.EvidenceItem(fact=fact, **statement)
                for fact, statement in snapshot.values.get("evidence", {}).items()
                if isinstance(statement, dict)
            ]
            order = ORDERS[dispute.order_id]
            criteria = self.policies[dispute.policy_version].refund_criteria
            reviews.append(pb.Review(
                dispute=dispute, reason=snapshot.values.get("review_reason", "legacy_review"),
                evidence=evidence, order_status=order.status, order_refundable=order.refundable,
                required_facts=criteria.required_facts,
            ))
        return pb.ReviewQueue(reviews=reviews)

    async def CompleteReview(self, request, context):
        async def operation(config, case_id, request_id):
            reply = request.reply.strip()
            if not valid_message(reply):
                raise ServiceError(grpc.StatusCode.INVALID_ARGUMENT, "Reply must contain 1–4000 characters")
            snapshot = await self.existing(config)
            if "review_reply" in snapshot.values:
                if snapshot.values["review_reply"] != reply:
                    raise ServiceError(grpc.StatusCode.ALREADY_EXISTS, "Review already completed")
                return response_for(snapshot)
            if response_for(snapshot).stage != pb.MANUAL_REVIEW:
                raise ServiceError(grpc.StatusCode.FAILED_PRECONDITION, "Case is not in manual review")
            await self.graph.aupdate_state(
                config, {
                    "stage": "resolved", "reply": reply, "review_reply": reply,
                    "policy_hash": self.policies[snapshot.values["policy_version"]].digest,
                }, as_node="finish",
            )
            return response_for(await self.existing(config))

        return await self.run("CompleteReview", context, request.case_id, ("operator",), operation)
