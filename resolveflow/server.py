import asyncio
import logging
import signal
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager, suppress

import aiosqlite
import grpc
from langgraph.checkpoint.serde.jsonplus import JsonPlusSerializer
from langgraph.checkpoint.sqlite.aio import AsyncSqliteSaver

from resolveflow.agents import BedrockAgents, MockAgents
from resolveflow.guardrails import GuardrailClient
from resolveflow.orders import ORDERS, RefundLedger
from resolveflow.proto import disputes_pb2 as pb
from resolveflow.proto.disputes_pb2_grpc import add_DisputeServiceServicer_to_server
from resolveflow.rpc_server import running_server
from resolveflow.service import DisputeService
from resolveflow.settings import Settings, load_policies
from resolveflow.workflow import build_graph


@asynccontextmanager
async def run_api(settings: Settings) -> AsyncIterator[int]:
    policies = load_policies()
    if settings.active_policy_version not in policies:
        raise ValueError("ACTIVE_POLICY_VERSION does not exist")
    settings.data_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
    ledger = RefundLedger(settings.data_dir / "refunds.sqlite")
    agents = (
        MockAgents() if settings.model_provider == "mock"
        else BedrockAgents(settings.bedrock_model_id, settings.aws_region)
    )
    options = (("grpc.max_receive_message_length", 32_768),)
    async with (
        aiosqlite.connect(settings.data_dir / "checkpoints.sqlite") as connection,
        grpc.aio.insecure_channel(settings.guardrail_target, options=options) as channel,
    ):
        saver = AsyncSqliteSaver(connection, serde=JsonPlusSerializer(allowed_msgpack_modules=None))
        await saver.setup()
        graph = build_graph(agents, ORDERS, policies, ledger, saver)
        guardrail = GuardrailClient(channel, timeout_seconds=settings.guardrail_timeout)
        server = grpc.aio.server(maximum_concurrent_rpcs=settings.max_inflight, options=options)
        service = DisputeService(graph, guardrail, settings, policies, saver)
        case_ids = await service.case_ids()
        for case_id in case_ids:
            state = (await graph.aget_state({"configurable": {"thread_id": case_id}})).values
            if "case_id" not in state:
                continue
            snapshot = policies.get(state["policy_version"])
            if snapshot is None or state.get("policy_hash", snapshot.digest) != snapshot.digest:
                raise ValueError(f"Published policy snapshot unavailable for case {case_id}")
        add_DisputeServiceServicer_to_server(service, server)
        service_name = pb.DESCRIPTOR.services_by_name["DisputeService"].full_name
        async with running_server(server, settings.api_bind, service_name) as port:
            recovery = asyncio.create_task(service.recover(case_ids))
            try:
                yield port
            finally:
                recovery.cancel()
                with suppress(asyncio.CancelledError):
                    await recovery


async def main():
    shutdown = asyncio.Event()
    asyncio.get_running_loop().add_signal_handler(signal.SIGTERM, shutdown.set)
    async with run_api(Settings.load()):
        await shutdown.wait()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    asyncio.run(main())
