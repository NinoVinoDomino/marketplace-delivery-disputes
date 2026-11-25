from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase

from resolveflow.orders import RefundLedger
from resolveflow.policy_diff import Rule
from resolveflow.ranking import rerank_top_k


class RankingTests(TestCase):
    def test_top_k_tie_and_empty_input(self):
        rules = [Rule("b", "carrier"), Rule("c", "carrier refund"), Rule("a", "refund")]
        self.assertEqual(rerank_top_k("CARRIER refund refund", iter(rules), 2), [rules[1], rules[2]])
        self.assertEqual(rerank_top_k("refund", rules, 0), [])
        self.assertEqual(rerank_top_k("refund", [], 3), [])


class LedgerTests(TestCase):
    def test_only_one_refund_wins_for_an_order(self):
        with TemporaryDirectory() as directory:
            ledger = RefundLedger(Path(directory) / "refunds.sqlite")
            cases = ("case-a", "case-b")
            with ThreadPoolExecutor(max_workers=2) as executor:
                results = list(executor.map(lambda case: ledger.execute(case, "order-a"), cases))
            self.assertEqual(sum(refund is not None for refund in results), 1)
            winner = next(index for index, refund in enumerate(results) if refund)
            self.assertEqual(ledger.execute(cases[winner], "order-a"), results[winner])
