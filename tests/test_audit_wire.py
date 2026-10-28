import importlib.util
import json
import tempfile
import unittest
from pathlib import Path

spec = importlib.util.spec_from_file_location(
    "audit_wire", Path(__file__).resolve().parents[1] / "scripts" / "audit-wire.py"
)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


class WireAuditTests(unittest.TestCase):
    def audit(self, quantity="1.00", delta="0.10", checkpoint="1.10", sequence=3, omit_empty_side=False):
        def snapshot(seq, value):
            return {"type": "orderbook_snapshot", "sid": 1, "seq": seq,
                    "msg": {"market_ticker": "A", "yes_dollars_fp": [["0.0760", value]],
                            "no_dollars_fp": []}}

        frames = [snapshot(1, quantity),
                  {"type": "orderbook_delta", "sid": 1, "seq": 2,
                   "msg": {"market_ticker": "A", "side": "yes",
                           "price_dollars": "0.0760", "delta_fp": delta}},
                  snapshot(sequence, checkpoint)]
        if omit_empty_side:
            for frame in frames:
                frame["msg"].pop("no_dollars_fp", None)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "capture.wire.jsonl"
            path.write_text("".join(json.dumps({"received_at": "2026-10-27T00:00:00Z",
                                               "frame": json.dumps(frame)}) + "\n" for frame in frames),
                            encoding="utf-8")
            return module.audit(path)

    def test_omitted_empty_snapshot_side_is_valid(self):
        self.assertTrue(self.audit(omit_empty_side=True)["passed"])

    def test_consistent_feed(self):
        self.assertTrue(self.audit()["passed"])

    def test_contiguous_sequence_does_not_hide_snapshot_disagreement(self):
        result = self.audit("52478.34", "-10.11", "52465.50")
        self.assertFalse(result["passed"])
        self.assertEqual(result["sequence_gaps"], [])
        self.assertEqual(result["snapshot_mismatches"], 1)
        self.assertEqual(result["cases"][0]["levels"][0]["offset"], "-2.73")

    def test_missing_sequence_fails_even_with_matching_books(self):
        result = self.audit(sequence=4)
        self.assertFalse(result["passed"])
        self.assertEqual(len(result["sequence_gaps"]), 1)

    def test_six_place_quantities_remain_exact_above_float_precision(self):
        self.assertTrue(self.audit("9007199254.740991", "0.000001", "9007199254.740992")["passed"])

    def test_captured_exchange_disagreement_is_reproducible_without_credentials(self):
        path = Path(__file__).resolve().parent / "fixtures" / "kalshi-inconsistent-snapshot.wire.jsonl.gz"
        result = module.audit(path)
        self.assertFalse(result["passed"])
        self.assertEqual(result["sequence_gaps"], [])
        self.assertEqual(result["snapshot_mismatches"], 1)
        case = result["cases"][0]
        self.assertEqual(case["seq"], 2839)
        self.assertEqual(case["levels"][0]["offset"], "-2.73")


if __name__ == "__main__":
    unittest.main()
