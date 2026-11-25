"""Measure the gRPC ingress path with synthetic in-transit or missing parcel disputes."""

import argparse
import asyncio
import json
import math
from collections import Counter
from time import perf_counter
from uuid import uuid4

import grpc

from resolveflow.proto import disputes_pb2 as pb
from resolveflow.proto.disputes_pb2_grpc import DisputeServiceStub
from resolveflow.settings import Settings


def percentile(sorted_values: list[float], fraction: float) -> float | None:
    if not sorted_values:
        return None
    index = math.ceil(fraction * len(sorted_values)) - 1
    return round(sorted_values[index], 2)


async def run(settings: Settings, requests: int, concurrency: int, scenario: str = "in-transit") -> dict:
    metadata = (("authorization", f"Bearer {settings.customer_token}"),)
    outcomes: Counter[str] = Counter()
    successes_ms = []
    pending = iter(range(requests))
    delivered = scenario == "delivered"
    order_id = "RF-1001" if delivered else "RF-1002"
    text = "I have called the carrier. Where is my order?" if delivered else "Where is my order?"
    expected = pb.AWAITING_OPERATOR if delivered else pb.RESOLVED

    async with grpc.aio.insecure_channel(settings.local_api_target) as channel:
        await asyncio.wait_for(channel.channel_ready(), timeout=5)
        stub = DisputeServiceStub(channel)

        async def worker():
            for _ in pending:
                started = perf_counter()
                case_id = str(uuid4())
                try:
                    response = await stub.OpenCase(
                        pb.OpenCaseRequest(
                            case_id=case_id, order_id=order_id, text=text,
                        ),
                        metadata=metadata,
                        timeout=30,
                    )
                    if delivered and response.stage == pb.NEEDS_CLARIFICATION:
                        response = await stub.AddMessage(pb.AddMessageRequest(
                            case_id=case_id, message_id=str(uuid4()), text="Yes.",
                            clarification_id=response.clarification_id,
                        ), metadata=metadata, timeout=30)
                except grpc.aio.AioRpcError as error:
                    outcomes[error.code().name] += 1
                else:
                    if response.stage == expected:
                        outcomes["OK"] += 1
                        successes_ms.append((perf_counter() - started) * 1_000)
                    else:
                        outcomes["UNEXPECTED_STAGE"] += 1

        started = perf_counter()
        await asyncio.gather(*(worker() for _ in range(concurrency)))
        elapsed = perf_counter() - started

    successes_ms.sort()
    return {
        "scenario": scenario,
        "requests": requests,
        "concurrency": concurrency,
        "elapsed_seconds": round(elapsed, 3),
        "successful_requests_per_second": round(outcomes["OK"] / elapsed, 2),
        "successful_latency_ms": {
            "p50": percentile(successes_ms, 0.50),
            "p95": percentile(successes_ms, 0.95),
            "p99": percentile(successes_ms, 0.99),
        },
        "status_counts": dict(outcomes),
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--requests", type=int, default=50)
    parser.add_argument("--concurrency", type=int, default=8)
    parser.add_argument("--scenario", choices=("in-transit", "delivered"), default="in-transit")
    args = parser.parse_args()
    if not 1 <= args.concurrency <= min(args.requests, 500) or args.requests > 10_000:
        parser.error("Use 1–10,000 requests and concurrency up to 500")
    print(json.dumps(asyncio.run(run(Settings.load(), args.requests, args.concurrency, args.scenario)), indent=2))
