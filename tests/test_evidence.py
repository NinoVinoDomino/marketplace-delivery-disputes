import asyncio
import json
from pathlib import Path
from unittest import TestCase

from resolveflow.evidence import Evidence, Statement, Turn, mock_evidence
from resolveflow.evaluate import evaluate


class EvidenceTests(TestCase):
    def test_mock_matches_labelled_facts_and_evidence(self):
        dataset = Path(__file__).resolve().parents[1] / "evals" / "labelled_dialogues.json"
        for case in json.loads(dataset.read_text()):
            turns = [Turn.model_validate(turn) for turn in case["turns"]]
            evidence = mock_evidence(turns)
            with self.subTest(case=case["name"]):
                self.assertTrue(evidence.grounded_in(turns))
                for fact, expected in case["facts"].items():
                    actual = getattr(evidence, fact)
                    if expected is None:
                        self.assertIsNone(actual)
                    else:
                        self.assertIsNotNone(actual)
                        self.assertEqual(actual.value, expected["value"])
                        self.assertEqual(actual.message_id, expected["message_id"])
                        self.assertIn(expected["quote"], actual.quote)

    def test_rejects_ungrounded_or_misassigned_short_answer(self):
        turns = [Turn(message_id="2", text="Yes.", target_fact="carrier_checked")]
        answer = Statement(value=True, message_id="2", quote="Yes")
        self.assertFalse(Evidence(carrier_checked=None, parcel_missing=answer).grounded_in(turns))
        invented = answer.model_copy(update={"quote": "Yes, I called"})
        self.assertFalse(Evidence(carrier_checked=invented, parcel_missing=None).grounded_in(turns))


    def test_mock_eval_routes_match_both_eligibility_versions(self):
        for version in ("v2", "v3"):
            with self.subTest(version=version):
                report = asyncio.run(evaluate("mock", version))
                self.assertEqual(report["scored"], report["dataset_size"])
                self.assertEqual(report["routing_errors"], [])
