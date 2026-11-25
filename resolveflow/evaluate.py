"""Score evidence and routing on the labelled synthetic dispute set."""

import argparse
import asyncio
import math
import hashlib
import json
from collections import Counter
from time import perf_counter

from resolveflow.agents import EVIDENCE_PROMPT, BedrockAgents, MockAgents, ProviderError
from resolveflow.evidence import Turn
from resolveflow.orders import ORDERS
from resolveflow.settings import ROOT, Settings, load_policies
from resolveflow.workflow import assessment_stage, clarification_fact, evidence_values


def ratio(numerator: int, denominator: int) -> dict:
    return {
        "count": numerator,
        "total": denominator,
        "rate": round(numerator / denominator, 4) if denominator else None,
    }


async def evaluate(provider: str, policy_version: str) -> dict:
    cases = json.loads((ROOT / "evals" / "labelled_dialogues.json").read_text())
    policy = load_policies()[policy_version]
    model_id = "mock"
    agents = MockAgents()
    if provider == "bedrock":
        settings = Settings.load()
        if not settings.bedrock_model_id:
            raise ValueError("Set BEDROCK_MODEL_ID for Bedrock evaluation")
        model_id = settings.bedrock_model_id
        agents = BedrockAgents(model_id, settings.aws_region)
    fact_counts = {fact: Counter() for fact in ("carrier_checked", "parcel_missing")}
    counters = Counter()
    errors = Counter()
    route_errors = []
    latencies = []

    for case in cases:
        turns = [Turn.model_validate(turn) for turn in case["turns"]]
        expected_route = case["routes"][policy_version]
        started = perf_counter()
        try:
            evidence = await agents.evidence(turns)
        except ProviderError as error:
            errors[error.code] += 1
            if error.code in {"INVALID_MODEL_OUTPUT", "UNGROUNDED_MODEL_OUTPUT"}:
                counters["invalid"] += 1
                actual_evidence = {}
                actual_route = "manual_review"
            else:
                continue
        else:
            actual_evidence = evidence.model_dump()
            facts = evidence_values(evidence)
            order = ORDERS[case["order_id"]]
            next_fact = clarification_fact(policy, order, facts, len(turns))
            actual_route = (
                f"clarify:{next_fact}" if next_fact
                else assessment_stage(policy, order, facts)
            )
        finally:
            latencies.append(round((perf_counter() - started) * 1000, 2))

        counters["scored"] += 1
        for fact, counts in fact_counts.items():
            expected = case["facts"][fact]
            actual = actual_evidence.get(fact)
            expected_value = expected["value"] if expected else None
            actual_value = actual["value"] if actual else None
            counts["correct"] += expected_value == actual_value
            counts["true_positive"] += expected_value is True and actual_value is True
            counts["expected_positive"] += expected_value is True
            counts["predicted_positive"] += actual_value is True
            if actual:
                counts["quotes"] += 1
                counts["correct_quotes"] += bool(
                    expected and actual_value == expected_value
                    and actual["message_id"] == expected["message_id"]
                    and expected["quote"].casefold() in actual["quote"].casefold()
                )
        counters["non_refund"] += expected_route != "awaiting_operator"
        counters["non_clarify"] += not expected_route.startswith("clarify:")
        counters["non_manual"] += expected_route != "manual_review"
        counters["false_refund"] += actual_route == "awaiting_operator" and expected_route != actual_route
        counters["extra_clarify"] += actual_route.startswith("clarify:") and not expected_route.startswith("clarify:")
        counters["extra_manual"] += actual_route == "manual_review" and expected_route != actual_route
        if expected_route != actual_route:
            route_errors.append({"case": case["name"], "expected": expected_route, "actual": actual_route})

    latencies.sort()
    return {
        "provider": provider, "model": model_id, "policy": policy_version, "policy_hash": policy.digest,
        "prompt_hash": hashlib.sha256(EVIDENCE_PROMPT.encode()).hexdigest() if provider == "bedrock" else None,
        "dataset_size": len(cases), "scored": counters["scored"], "provider_errors": dict(errors),
        "invalid_output": ratio(counters["invalid"], len(cases)),
        "extraction_latency_ms": {
            "mean": round(sum(latencies) / len(latencies), 2),
            "p95": latencies[math.ceil(0.95 * len(latencies)) - 1],
        } if latencies else {},
        "facts": {
            fact: {
                "accuracy_including_unknown": ratio(counts["correct"], counters["scored"]),
                "positive_precision": ratio(counts["true_positive"], counts["predicted_positive"]),
                "positive_recall": ratio(counts["true_positive"], counts["expected_positive"]),
                "evidence_span_match": ratio(counts["correct_quotes"], counts["quotes"]),
            }
            for fact, counts in fact_counts.items()
        },
        "false_refund_review": ratio(counters["false_refund"], counters["non_refund"]),
        "unnecessary_clarification": ratio(counters["extra_clarify"], counters["non_clarify"]),
        "unnecessary_manual_review": ratio(counters["extra_manual"], counters["non_manual"]),
        "routing_errors": route_errors,
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--provider", choices=("mock", "bedrock"), default="mock")
    parser.add_argument("--policy", choices=tuple(load_policies()), default="v2")
    args = parser.parse_args()
    result = asyncio.run(evaluate(args.provider, args.policy))
    print(json.dumps(result, indent=2))
    if not result["scored"]:
        raise SystemExit(1)
