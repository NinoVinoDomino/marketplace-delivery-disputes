import asyncio
import json
import sqlite3
import unittest
from dataclasses import replace
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch
from uuid import uuid4

import grpc

from resolveflow.agents import MockAgents, ProviderError
from resolveflow.orders import RefundLedger
from resolveflow.mock_guardrail import MockGuardrail
from resolveflow.proto import disputes_pb2 as pb
from resolveflow.proto.disputes_pb2_grpc import DisputeServiceStub
from resolveflow.proto.guardrails_pb2_grpc import add_GuardrailServiceServicer_to_server
from resolveflow.rpc_server import running_server
from resolveflow.server import run_api
from resolveflow.settings import ROOT, Settings


CUSTOMER = "test-customer-token-for-grpc"
OPERATOR = "test-operator-token-for-grpc"


class DisputeTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        directory = self.enterContext(TemporaryDirectory())
        guardrail = grpc.aio.server()
        add_GuardrailServiceServicer_to_server(MockGuardrail(), guardrail)
        guard_port = await self.enterAsyncContext(
            running_server(guardrail, "127.0.0.1:0", "resolveflow.guardrails.v1.GuardrailService")
        )
        self.settings = Settings(
            CUSTOMER, OPERATOR, "127.0.0.1:0", f"127.0.0.1:{guard_port}", 1.0, 32,
            Path(directory), "v1", "mock", "us-east-1", "",
        )

    async def call(self, stub, method, request, token=CUSTOMER):
        return await getattr(stub, method)(
            request, metadata=(("authorization", f"Bearer {token}"),), timeout=5,
        )

    async def test_golden_cases_on_both_policy_versions(self):
        fixtures = json.loads((ROOT / "evals/golden_cases.json").read_text())
        for version in ("v1", "v2"):
            async with run_api(replace(self.settings, active_policy_version=version)) as port:
                async with grpc.aio.insecure_channel(f"127.0.0.1:{port}") as channel:
                    stub = DisputeServiceStub(channel)
                    for fixture in fixtures:
                        with self.subTest(version=version, name=fixture["name"]):
                            case_id = str(uuid4())
                            initial, *clarifications = fixture["messages"]
                            result = await self.call(stub, "OpenCase", pb.OpenCaseRequest(
                                case_id=case_id, order_id=fixture["order_id"], text=initial,
                            ))
                            stages = [pb.Stage.Name(result.stage)]
                            for text in clarifications:
                                result = await self.call(stub, "AddMessage", pb.AddMessageRequest(
                                    case_id=case_id, message_id=str(uuid4()), text=text,
                                    clarification_id=result.clarification_id,
                                ))
                                stages.append(pb.Stage.Name(result.stage))
                            self.assertEqual(stages, fixture["stages"])
                            self.assertEqual(list(result.citations), fixture["citations"])
                            self.assertEqual(result.policy_version, version)
                            self.assertEqual(len(result.policy_hash), 64)
                            self.assertNotIn("hours", result.reply)

    async def test_guardrail_and_approval_across_restart(self):
        case_id = str(uuid4())
        request = pb.OpenCaseRequest(case_id=case_id, order_id="RF-1001", text="My parcel is missing.")
        async with run_api(self.settings) as port:
            async with grpc.aio.insecure_channel(f"127.0.0.1:{port}") as channel:
                stub = DisputeServiceStub(channel)
                opened = await self.call(stub, "OpenCase", request)
                self.assertEqual(opened.stage, pb.NEEDS_CLARIFICATION)
                with self.assertRaises(grpc.aio.AioRpcError) as rejected:
                    await self.call(stub, "AddMessage", pb.AddMessageRequest(
                        case_id=case_id, message_id=str(uuid4()), text="test_prompt_injection",
                        clarification_id=opened.clarification_id,
                    ))
                self.assertEqual(rejected.exception.code(), grpc.StatusCode.PERMISSION_DENIED)
                current = await self.call(stub, "GetCase", pb.GetCaseRequest(case_id=case_id))
                self.assertEqual(current, opened)
                clarification = pb.AddMessageRequest(
                    case_id=case_id, message_id=str(uuid4()), clarification_id=opened.clarification_id,
                    text="I contacted the carrier. My parcel is missing.",
                )
                awaiting = await self.call(stub, "AddMessage", clarification)
                self.assertEqual(awaiting.stage, pb.AWAITING_OPERATOR)
                self.assertEqual(awaiting, await self.call(stub, "AddMessage", clarification))

        async with run_api(replace(self.settings, active_policy_version="v2")) as port:
            async with grpc.aio.insecure_channel(f"127.0.0.1:{port}") as channel:
                stub = DisputeServiceStub(channel)
                self.assertEqual(await self.call(stub, "OpenCase", request), awaiting)
                approval = pb.DecideRefundRequest(case_id=case_id, approved=True)
                with self.assertRaises(grpc.aio.AioRpcError) as unauthorized:
                    await self.call(stub, "DecideRefund", approval)
                self.assertEqual(unauthorized.exception.code(), grpc.StatusCode.PERMISSION_DENIED)
                issued, duplicate = await asyncio.gather(
                    self.call(stub, "DecideRefund", approval, OPERATOR),
                    self.call(stub, "DecideRefund", approval, OPERATOR),
                )
                self.assertEqual(issued.stage, pb.REFUND_ISSUED)
                self.assertEqual(issued.policy_version, "v1")
                self.assertTrue(issued.refund_id)
                self.assertEqual(issued, duplicate)


    async def test_old_question_cannot_consume_new_question(self):
        async with run_api(self.settings) as port, grpc.aio.insecure_channel(f"127.0.0.1:{port}") as channel:
            stub = DisputeServiceStub(channel)
            case_id = str(uuid4())
            opened = await self.call(stub, "OpenCase", pb.OpenCaseRequest(
                case_id=case_id, order_id="RF-1001", text="Where is my order?",
            ))
            answer = pb.AddMessageRequest(
                case_id=case_id, message_id=str(uuid4()), text="Yes.", clarification_id=opened.clarification_id,
            )
            second = await self.call(stub, "AddMessage", answer)
            self.assertEqual(second.stage, pb.NEEDS_CLARIFICATION)
            self.assertNotEqual(second.clarification_id, opened.clarification_id)
            self.assertEqual(await self.call(stub, "AddMessage", answer), second)
            answer.message_id = str(uuid4())
            with self.assertRaises(grpc.aio.AioRpcError) as stale:
                await self.call(stub, "AddMessage", answer)
            self.assertEqual(stale.exception.code(), grpc.StatusCode.FAILED_PRECONDITION)
            answer.clarification_id = second.clarification_id
            self.assertEqual((await self.call(stub, "AddMessage", answer)).stage, pb.AWAITING_OPERATOR)

    async def test_manual_queue_and_policy_change_across_restart(self):
        manual_id, old_id = str(uuid4()), str(uuid4())
        async with run_api(self.settings) as port, grpc.aio.insecure_channel(f"127.0.0.1:{port}") as channel:
            stub = DisputeServiceStub(channel)
            manual = await self.call(stub, "OpenCase", pb.OpenCaseRequest(
                case_id=manual_id, order_id="RF-1003",
                text="I contacted the carrier. My parcel is missing.",
            ))
            self.assertEqual(manual.stage, pb.MANUAL_REVIEW)
            old = await self.call(stub, "OpenCase", pb.OpenCaseRequest(
                case_id=old_id, order_id="RF-1001", text="My parcel is missing.",
            ))
            self.assertEqual(old.stage, pb.NEEDS_CLARIFICATION)

        settings = replace(self.settings, active_policy_version="v3")
        async with run_api(settings) as port, grpc.aio.insecure_channel(f"127.0.0.1:{port}") as channel:
            stub = DisputeServiceStub(channel)
            new = await self.call(stub, "OpenCase", pb.OpenCaseRequest(
                case_id=str(uuid4()), order_id="RF-1001", text="My parcel is missing.",
            ))
            self.assertEqual(new.stage, pb.AWAITING_OPERATOR)
            self.assertNotEqual(new.policy_hash, old.policy_hash)
            previous = await self.call(stub, "GetCase", pb.GetCaseRequest(case_id=old_id))
            self.assertEqual(previous, old)
            previous = await self.call(stub, "AddMessage", pb.AddMessageRequest(
                case_id=old_id, message_id=str(uuid4()), text="No.", clarification_id=old.clarification_id,
            ))
            self.assertEqual(previous.stage, pb.NEEDS_CLARIFICATION)
            self.assertEqual(previous.policy_hash, old.policy_hash)
            with self.assertRaises(grpc.aio.AioRpcError) as unauthorized:
                await self.call(stub, "ListReviews", pb.ListReviewsRequest())
            self.assertEqual(unauthorized.exception.code(), grpc.StatusCode.PERMISSION_DENIED)
            queue = await self.call(stub, "ListReviews", pb.ListReviewsRequest(), OPERATOR)
            items = {review.dispute.case_id: review for review in queue.reviews}
            self.assertEqual(set(items), {manual_id, new.case_id})
            self.assertEqual(items[manual_id].reason, "refund_criteria_not_met")
            self.assertIn("parcel is missing", next(
                fact.quote for fact in items[manual_id].evidence if fact.fact == "parcel_missing"
            ))
            complete = pb.CompleteReviewRequest(case_id=manual_id, reply="We reviewed your case.")
            result = await self.call(stub, "CompleteReview", complete, OPERATOR)
            self.assertEqual(result.stage, pb.RESOLVED)
            self.assertEqual(result.reply, complete.reply)
            self.assertEqual(result, await self.call(stub, "CompleteReview", complete, OPERATOR))
            queue = await self.call(stub, "ListReviews", pb.ListReviewsRequest(), OPERATOR)
            self.assertEqual([item.dispute.case_id for item in queue.reviews], [new.case_id])

    async def test_matching_rpc_retries_pending_extraction(self):
        async with run_api(self.settings) as port, grpc.aio.insecure_channel(f"127.0.0.1:{port}") as channel:
            stub = DisputeServiceStub(channel)
            case_id = str(uuid4())
            request = pb.OpenCaseRequest(case_id=case_id, order_id="RF-1001", text="My parcel is missing.")
            for method in ("OpenCase", "AddMessage"):
                with patch.object(MockAgents, "evidence", side_effect=ProviderError("TEST_TIMEOUT", True)):
                    with self.assertRaises(grpc.aio.AioRpcError) as failure:
                        await self.call(stub, method, request)
                self.assertEqual(failure.exception.code(), grpc.StatusCode.UNAVAILABLE)
                result = await self.call(stub, method, request)
                if method == "OpenCase":
                    self.assertEqual(result.stage, pb.NEEDS_CLARIFICATION)
                    request = pb.AddMessageRequest(
                        case_id=case_id, message_id=str(uuid4()), text="Yes.",
                        clarification_id=result.clarification_id,
                    )
                else:
                    self.assertEqual(result.stage, pb.AWAITING_OPERATOR)

    async def test_restart_recovers_before_ledger_commit(self):
        await self.assert_refund_recovery(committed=False)

    async def test_restart_recovers_after_ledger_commit(self):
        await self.assert_refund_recovery(committed=True)

    async def assert_refund_recovery(self, *, committed):
        case_id = str(uuid4())
        stored_refund_ids = []
        original = RefundLedger.execute

        def fail(ledger, case_id, order_id):
            if committed:
                stored_refund_ids.append(original(ledger, case_id, order_id))
            raise RuntimeError("simulated interruption between checkpoints")

        approval = pb.DecideRefundRequest(case_id=case_id, approved=True)
        async with run_api(self.settings) as port, grpc.aio.insecure_channel(f"127.0.0.1:{port}") as channel:
            stub = DisputeServiceStub(channel)
            opened = await self.call(stub, "OpenCase", pb.OpenCaseRequest(
                case_id=case_id, order_id="RF-1001", text="I called the carrier. My parcel is missing.",
            ))
            self.assertEqual(opened.stage, pb.AWAITING_OPERATOR)
            with patch.object(RefundLedger, "execute", fail):
                with self.assertRaises(grpc.aio.AioRpcError) as failure:
                    await self.call(stub, "DecideRefund", approval, OPERATOR)
            self.assertEqual(failure.exception.code(), grpc.StatusCode.INTERNAL)
            result = await self.call(stub, "GetCase", pb.GetCaseRequest(case_id=case_id))
            self.assertEqual(result.stage, pb.PROCESSING)

        async with run_api(self.settings) as port, grpc.aio.insecure_channel(f"127.0.0.1:{port}") as channel:
            stub = DisputeServiceStub(channel)
            async with asyncio.timeout(5):
                while True:
                    result = await self.call(stub, "GetCase", pb.GetCaseRequest(case_id=case_id))
                    if result.stage != pb.PROCESSING:
                        break
                    await asyncio.sleep(0.02)
            self.assertEqual(result.stage, pb.REFUND_ISSUED)
            self.assertEqual(await self.call(stub, "DecideRefund", approval, OPERATOR), result)
        if committed:
            self.assertEqual(stored_refund_ids, [result.refund_id])
        database = sqlite3.connect(self.settings.data_dir / "refunds.sqlite")
        try:
            rows = database.execute("SELECT case_id, refund_id FROM refunds").fetchall()
        finally:
            database.close()
        self.assertEqual(rows, [(case_id, result.refund_id)])
