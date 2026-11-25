import re
from collections.abc import Iterable

from resolveflow.policy_diff import Rule


def tokens(text: str) -> set[str]:
    return set(re.findall(r"\w+", text.casefold()))


def rerank_top_k(query: str, rules: Iterable[Rule], k: int) -> list[Rule]:
    """Rank by unique matching terms, then stable rule ID."""
    if k <= 0:
        return []
    query_terms = tokens(query)
    return sorted(rules, key=lambda rule: (
        -len(query_terms & tokens(rule.text)),
        rule.rule_id,
    ))[:k]
