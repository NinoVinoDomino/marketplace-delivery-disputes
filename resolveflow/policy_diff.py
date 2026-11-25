"""Compare stable rule IDs across two policy snapshots."""

import hashlib
import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Optional

from resolveflow.orders import Order


FACT_NAMES = {"carrier_checked", "parcel_missing"}


@dataclass(frozen=True)
class Rule:
    rule_id: str
    text: str


@dataclass(frozen=True)
class RefundCriteria:
    order_statuses: tuple[str, ...]
    require_refundable: bool
    required_facts: tuple[str, ...]

    @classmethod
    def load(cls, value: dict) -> "RefundCriteria":
        statuses = value["order_statuses"]
        facts = value["required_facts"]
        if (
            not isinstance(statuses, list) or not statuses
            or any(status not in ("delivered",) for status in statuses)
            or type(value["require_refundable"]) is not bool
            or not isinstance(facts, list) or any(fact not in FACT_NAMES for fact in facts)
            or "parcel_missing" not in facts or len(set(facts)) != len(facts)
        ):
            raise ValueError("Invalid refund criteria")
        return cls(tuple(statuses), value["require_refundable"], tuple(facts))

    def order_matches(self, order: Order) -> bool:
        return order.status in self.order_statuses and (order.refundable or not self.require_refundable)

    def matches(self, order: Order, facts: dict[str, bool | None]) -> bool:
        return self.order_matches(order) and all(facts.get(fact) is True for fact in self.required_facts)


@dataclass(frozen=True)
class Snapshot:
    version: str
    rules: tuple[Rule, ...]
    follow_up_hours: int = 0
    refund_criteria: RefundCriteria | None = None
    digest: str = ""

    @classmethod
    def load(cls, path: Path) -> "Snapshot":
        payload = json.loads(path.read_text(encoding="utf-8"))
        canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        hours = payload.get("follow_up_hours", 0)
        if type(hours) is not int or hours < 0:
            raise ValueError("Invalid follow-up duration")
        return cls(
            version=payload["version"],
            rules=tuple(
                Rule(rule_id, text.replace("{hours}", str(hours)))
                for rule_id, text in payload["rules"].items()
            ),
            follow_up_hours=hours,
            refund_criteria=RefundCriteria.load(payload["refund_criteria"]),
            digest=hashlib.sha256(canonical.encode("utf-8")).hexdigest(),
        )


@dataclass(frozen=True)
class Change:
    rule_id: str
    before: Optional[str]
    after: Optional[str]


def compare(previous: Snapshot, candidate: Snapshot) -> list[Change]:
    old_rules = {rule.rule_id: rule.text for rule in previous.rules}
    new_rules = {rule.rule_id: rule.text for rule in candidate.rules}
    changes = [
        Change(rule_id, old_rules.get(rule_id), new_rules.get(rule_id))
        for rule_id in sorted(old_rules.keys() | new_rules.keys())
        if old_rules.get(rule_id) != new_rules.get(rule_id)
    ]
    if previous.refund_criteria != candidate.refund_criteria:
        changes.append(Change(
            "refund_criteria",
            json.dumps(asdict(previous.refund_criteria)) if previous.refund_criteria else None,
            json.dumps(asdict(candidate.refund_criteria)) if candidate.refund_criteria else None,
        ))
    return changes


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("previous", type=Path)
    parser.add_argument("candidate", type=Path)
    args = parser.parse_args()
    previous = Snapshot.load(args.previous)
    candidate = Snapshot.load(args.candidate)
    print(json.dumps({
        "previous_version": previous.version,
        "candidate_version": candidate.version,
        "changes": [asdict(change) for change in compare(previous, candidate)],
    }, indent=2, ensure_ascii=False))
