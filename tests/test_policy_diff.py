from pathlib import Path
from unittest import TestCase

from resolveflow.policy_diff import Change, Rule, Snapshot, compare


class PolicyDiffTests(TestCase):
    def test_changed_rule_keeps_old_and_new_citations(self):
        directory = Path(__file__).resolve().parents[1] / "policies"
        before = Snapshot.load(directory / "v1.json")
        after = Snapshot.load(directory / "v2.json")

        self.assertEqual(before.version, "v1")
        self.assertEqual(after.version, "v2")
        changes = compare(before, after)
        self.assertEqual([change.rule_id for change in changes], ["delivery.follow_up"])
        self.assertIn("48 hours", changes[0].before)
        self.assertIn("24 hours", changes[0].after)

    def test_diff_returns_additions_and_removals_in_stable_order(self):
        before = Snapshot("v1", (Rule("removed", "Old text"),))
        after = Snapshot("v2", (Rule("added", "New text"),))

        self.assertEqual(compare(before, after), [
            Change("added", None, "New text"),
            Change("removed", "Old text", None),
        ])

    def test_refund_criteria_change_is_visible(self):
        from resolveflow.orders import ORDERS
        from resolveflow.settings import load_policies

        old, new = (load_policies()[version] for version in ("v2", "v3"))
        facts = {"parcel_missing": True, "carrier_checked": False}
        self.assertFalse(old.refund_criteria.matches(ORDERS["RF-1001"], facts))
        self.assertTrue(new.refund_criteria.matches(ORDERS["RF-1001"], facts))
        self.assertFalse(new.refund_criteria.matches(ORDERS["RF-1003"], facts))
        self.assertEqual([item.rule_id for item in compare(old, new)], ["refund_criteria"])

    def test_archive_rejects_rewritten_or_missing_history(self):
        import shutil
        from tempfile import TemporaryDirectory

        from resolveflow.settings import ROOT, load_policies

        with TemporaryDirectory() as directory:
            archive = Path(directory) / "policies"
            shutil.copytree(ROOT / "policies", archive)
            path = archive / "v1.json"
            path.write_text(path.read_text().replace('"follow_up_hours": 48', '"follow_up_hours": 72'))
            with self.assertRaisesRegex(ValueError, "published manifest"):
                load_policies(archive)
            path.unlink()
            with self.assertRaisesRegex(ValueError, "published manifest"):
                load_policies(archive)
