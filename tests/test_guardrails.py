import asyncio
import unittest

import grpc

from resolveflow.guardrails import Decision, GuardrailClient, submit_message
from resolveflow.mock_guardrail import MockGuardrail
from resolveflow.proto import guardrails_pb2 as pb
from resolveflow.proto import guardrails_pb2_grpc as rpc


class TestGuardrailService(MockGuardrail):
    async def Inspect(self, request, context):
        if request.text == "simulate_timeout":
            await asyncio.sleep(2)
        if request.text == "simulate_outage":
            await context.abort(grpc.StatusCode.UNAVAILABLE, "Test outage")
        if request.text == "simulate_invalid_verdict":
            return pb.InspectResponse(decision=pb.ALLOW)
        return await super().Inspect(request, context)


class GuardrailTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        server = grpc.aio.server()
        rpc.add_GuardrailServiceServicer_to_server(TestGuardrailService(), server)
        port = server.add_insecure_port("127.0.0.1:0")
        await server.start()
        self.addAsyncCleanup(server.stop, 0)
        channel = grpc.aio.insecure_channel(f"127.0.0.1:{port}")
        self.addAsyncCleanup(channel.close)
        await asyncio.wait_for(channel.channel_ready(), timeout=5)
        self.client = GuardrailClient(channel)
        self.dispatched = []

    async def submit(self, text):
        async def dispatch(message):
            self.dispatched.append(message)

        return await submit_message(text, self.client, dispatch, request_id="test-request")

    async def test_only_allowed_messages_are_dispatched(self):
        message = "My parcel was marked as delivered."
        verdict = await self.submit(message)
        self.assertEqual(verdict.decision, Decision.ALLOW)
        self.assertEqual(self.dispatched, [message])

    async def test_injection_and_banned_words_block_dispatch(self):
        for text, reason in (
            ("test_prompt_injection", "prompt_injection"),
            ("TEST_BANNED_WORD", "banned_word"),
        ):
            with self.subTest(text=text):
                verdict = await self.submit(text)
                self.assertEqual(verdict.decision, Decision.BLOCK)
                self.assertEqual(verdict.reasons, (reason,))
        self.assertEqual(self.dispatched, [])

    async def test_rpc_failures_and_invalid_verdict_block_dispatch(self):
        for text, code in (
            ("simulate_timeout", "DEADLINE_EXCEEDED"),
            ("simulate_outage", "UNAVAILABLE"),
            ("simulate_invalid_verdict", "INVALID_VERDICT"),
        ):
            with self.subTest(code=code), self.assertLogs("resolveflow.guardrails", "WARNING") as logs:
                verdict = await self.submit(text)
                self.assertEqual(verdict.decision, Decision.UNAVAILABLE)
                self.assertEqual(verdict.error_code, code)
                self.assertNotIn(text, logs.output[0])
        self.assertEqual(self.dispatched, [])

    async def test_rejects_blank_messages_at_both_boundaries(self):
        with self.assertRaises(ValueError):
            await self.submit(" ")
        verdict = await self.client.inspect(" ")
        self.assertEqual(verdict.error_code, "INVALID_ARGUMENT")
        self.assertEqual(self.dispatched, [])
