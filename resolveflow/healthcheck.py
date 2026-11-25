"""Probe a local service via the standard gRPC health contract."""

import argparse

import grpc
from grpc_health.v1 import health_pb2, health_pb2_grpc


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("target")
    parser.add_argument("service")
    args = parser.parse_args()
    try:
        with grpc.insecure_channel(args.target) as channel:
            result = health_pb2_grpc.HealthStub(channel).Check(
                health_pb2.HealthCheckRequest(service=args.service), timeout=1,
            )
    except grpc.RpcError as error:
        raise SystemExit(error.code().name) from None
    if result.status != health_pb2.HealthCheckResponse.SERVING:
        raise SystemExit(health_pb2.HealthCheckResponse.ServingStatus.Name(result.status))


if __name__ == "__main__":
    main()
