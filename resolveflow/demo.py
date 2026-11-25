"""Drive the happy path and a blocked clarification via the public gRPC API."""

import argparse
import asyncio
import json
from uuid import uuid4

import grpc
from google.protobuf.json_format import MessageToDict

from resolveflow.proto import disputes_pb2 as pb
from resolveflow.proto.disputes_pb2_grpc import DisputeServiceStub
from resolveflow.settings import Settings


async def main(require_refund: bool):
    settings = Settings.load()
    customer = (("authorization", f"Bearer {settings.customer_token}"),)
    operator = (("authorization", f"Bearer {settings.operator_token}"),)
    case_id = str(uuid4())
    async with grpc.aio.insecure_channel(settings.local_api_target) as channel:
        stub = DisputeServiceStub(channel)
        opened = await stub.OpenCase(pb.OpenCaseRequest(
            case_id=case_id, order_id="RF-1001", text="My parcel is missing.",
        ), metadata=customer, timeout=30)
        print(json.dumps(MessageToDict(opened)))
        result = opened
        if opened.stage == pb.NEEDS_CLARIFICATION:
            try:
                await stub.AddMessage(pb.AddMessageRequest(
                    case_id=case_id, message_id=str(uuid4()), clarification_id=opened.clarification_id, text="test_prompt_injection",
                ), metadata=customer, timeout=10)
            except grpc.aio.AioRpcError as error:
                if error.code() != grpc.StatusCode.PERMISSION_DENIED:
                    raise
                print(json.dumps({"rejected_clarification": error.code().name}))
            else:
                raise RuntimeError("Expected the mock to reject the test marker")

            result = await stub.AddMessage(pb.AddMessageRequest(
                case_id=case_id, message_id=str(uuid4()), clarification_id=opened.clarification_id,
                text="I contacted the carrier. The parcel is still missing.",
            ), metadata=customer, timeout=60)
            print(json.dumps(MessageToDict(result)))
        if result.stage == pb.AWAITING_OPERATOR:
            reviews = await stub.ListReviews(pb.ListReviewsRequest(), metadata=operator, timeout=10)
            review = next(item for item in reviews.reviews if item.dispute.case_id == case_id)
            print(json.dumps(MessageToDict(review)))
            approval = pb.DecideRefundRequest(case_id=case_id, approved=True)
            result = await stub.DecideRefund(approval, metadata=operator, timeout=10)
            duplicate = await stub.DecideRefund(approval, metadata=operator, timeout=10)
            if result != duplicate:
                raise RuntimeError("Repeated approval changed the case")
            print(json.dumps(MessageToDict(result)))
        if require_refund and result.stage != pb.REFUND_ISSUED:
            raise RuntimeError(f"Expected REFUND_ISSUED, got {pb.Stage.Name(result.stage)}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--require-refund", action="store_true", help="Require a fresh refund")
    asyncio.run(main(parser.parse_args().require_refund))
