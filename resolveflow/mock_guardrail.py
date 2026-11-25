"""Local gRPC fixture for the guardrail service contract."""

import asyncio
import signal
import re
import unicodedata

import grpc
from dotenv import load_dotenv

from resolveflow.guardrails import valid_message
from resolveflow.proto import guardrails_pb2 as pb
from resolveflow.proto import guardrails_pb2_grpc as rpc
from resolveflow.rpc_server import running_server
from resolveflow.settings import ROOT, demo_bind


POLICY_VERSION = "mock-v1"
INJECTION_MARKER = "test_prompt_injection"
BANNED_WORDS = re.compile(r"\b(?:test_banned_word|test_prohibited_phrase)\b")


class MockGuardrail(rpc.GuardrailServiceServicer):
    async def Inspect(self, request, context):
        if not valid_message(request.text):
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "Invalid message length")

        normalized = unicodedata.normalize("NFKC", request.text).casefold()
        reasons = []
        if INJECTION_MARKER in normalized:
            reasons.append(pb.PROMPT_INJECTION)
        if BANNED_WORDS.search(normalized):
            reasons.append(pb.BANNED_WORD)
        return pb.InspectResponse(
            decision=pb.BLOCK if reasons else pb.ALLOW,
            reasons=reasons,
            policy_version=POLICY_VERSION,
        )


async def serve():
    load_dotenv(ROOT / ".env")
    address = demo_bind("GUARDRAIL_BIND", "127.0.0.1:50051")
    shutdown = asyncio.Event()
    asyncio.get_running_loop().add_signal_handler(signal.SIGTERM, shutdown.set)
    server = grpc.aio.server(
        maximum_concurrent_rpcs=64,
        options=(("grpc.max_receive_message_length", 32_768),),
    )
    rpc.add_GuardrailServiceServicer_to_server(MockGuardrail(), server)
    service_name = pb.DESCRIPTOR.services_by_name["GuardrailService"].full_name
    async with running_server(server, address, service_name):
        await shutdown.wait()


if __name__ == "__main__":
    asyncio.run(serve())
