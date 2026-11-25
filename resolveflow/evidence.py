"""Shared evidence schema and deterministic demo extraction."""

import re
from typing import Literal

from pydantic import BaseModel, StrictBool


FactName = Literal["carrier_checked", "parcel_missing"]


class Turn(BaseModel):
    message_id: str
    text: str
    clarification_id: str | None = None
    question: str | None = None
    target_fact: FactName | None = None


class Statement(BaseModel):
    value: StrictBool
    message_id: str
    quote: str


class Evidence(BaseModel):
    carrier_checked: Statement | None
    parcel_missing: Statement | None

    def grounded_in(self, turns: list[Turn]) -> bool:
        indexed = {turn.message_id: turn for turn in turns}
        for fact in ("carrier_checked", "parcel_missing"):
            statement = getattr(self, fact)
            if statement is None:
                continue
            turn = indexed.get(statement.message_id)
            if not turn or not statement.quote.strip() or statement.quote not in turn.text:
                return False
            short = SHORT_ANSWER.fullmatch(turn.text)
            if short and (turn.target_fact != fact or statement.value != (short[1].lower() == "yes")):
                return False
        return True



CLAUSES = re.compile(r"[.!?;,]+|\b(?:but|and)\b", re.IGNORECASE)
SHORT_ANSWER = re.compile(r"\s*(yes|no)[.!?]*\s*", re.IGNORECASE)
CARRIER = re.compile(
    r"\b(?:(?P<negative>(?:have|has|had|did) not |(?:haven't|hasn't|hadn't|didn't|never|not) )"
    r"(?:yet )?(?:check(?:ed)? with|contact(?:ed)?|call(?:ed)?)|checked with|contacted|called)"
    r" (?:the )?carrier\b"
)
PARCEL_MISSING = re.compile(
    r"\b(?:parcel|package|order) (?:is |was )?(?:still )?missing\b"
    r"|\b(?:never arrived|(?:not|never|haven't|didn't) (?:yet )?receive[d]?|can't find)\b"
)
PARCEL_FOUND = re.compile(
    r"\b(?:parcel|package|order) (?:isn't|is not|wasn't|was not|was never|no longer) (?:still )?missing\b"
    r"|\b(?:parcel|package|order) (?:has )?arrived\b"
    r"|\b(?:found|received) (?:the|my) (?:parcel|package|order)\b"
)
ARRIVAL_NEGATED = re.compile(r"\b(?:never|not|haven't|didn't) (?:yet )?(?:found|received|arrived)\b")
CORRECTION = re.compile(r"\b(?:actually|correction|i was wrong|i was mistaken|i made a mistake)\b")
HEDGED = re.compile(r"\b(?:maybe|might|perhaps|not sure|if)\b")


def mock_evidence(turns: list[Turn]) -> Evidence:
    facts: dict[str, Statement | None] = {}
    seen: set[str] = set()
    for turn in turns:
        statements: dict[str, list[Statement]] = {}
        short = SHORT_ANSWER.fullmatch(turn.text)
        if short and turn.target_fact:
            statements[turn.target_fact] = [
                Statement(value=short[1].lower() == "yes", message_id=turn.message_id, quote=short[1])
            ]
        else:
            for clause in CLAUSES.split(turn.text):
                quote = clause.strip()
                text = quote.casefold().replace("’", "'")
                if HEDGED.search(text):
                    continue
                for match in CARRIER.finditer(text):
                    statements.setdefault("carrier_checked", []).append(Statement(
                        value=not bool(match["negative"]), message_id=turn.message_id, quote=quote,
                    ))
                missing = PARCEL_MISSING.search(text)
                found = PARCEL_FOUND.search(text)
                if found and not ARRIVAL_NEGATED.search(text):
                    statements.setdefault("parcel_missing", []).append(Statement(
                        value=False, message_id=turn.message_id, quote=quote,
                    ))
                elif missing:
                    statements.setdefault("parcel_missing", []).append(Statement(
                        value=True, message_id=turn.message_id, quote=quote,
                    ))

        for fact, candidates in statements.items():
            latest = candidates[-1]
            previous = facts.get(fact)
            conflicting_clause = any(item.value != latest.value for item in candidates)
            if conflicting_clause:
                facts[fact] = None
            elif (
                fact not in seen or (previous and previous.value == latest.value)
                or turn.target_fact == fact or CORRECTION.search(turn.text.casefold())
                or (fact == "parcel_missing" and not latest.value and PARCEL_FOUND.search(latest.quote.casefold()))
            ):
                facts[fact] = latest
            else:
                facts[fact] = None
            seen.add(fact)
    return Evidence(carrier_checked=facts.get("carrier_checked"), parcel_missing=facts.get("parcel_missing"))
