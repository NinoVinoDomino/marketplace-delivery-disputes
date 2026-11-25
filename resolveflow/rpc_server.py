from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import grpc
from grpc_health.v1 import health, health_pb2, health_pb2_grpc


@asynccontextmanager
async def running_server(
    server: grpc.aio.Server, address: str, service_name: str,
) -> AsyncIterator[int]:
    probe = health.aio.HealthServicer()
    health_pb2_grpc.add_HealthServicer_to_server(probe, server)
    await probe.set(service_name, health_pb2.HealthCheckResponse.SERVING)
    port = server.add_insecure_port(address)
    await server.start()
    try:
        yield port
    finally:
        await probe.enter_graceful_shutdown()
        await server.stop(grace=2)
