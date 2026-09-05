"""Navigation boundary and durable-resume tests; cleartext transport only."""

import json
from pathlib import Path
import tempfile
import unittest
from test_transparent_pir_incremental import h, S
from transparent_pir_incremental import (
    build_generation,
    new_state,
    save,
    sync,
    FileTransport,
    PAGE,
)
from transparent_pir_incremental_run import oracle, verify


class NavigationTests(unittest.TestCase):
    def test_boundaries_and_budget_resume(self):
        # Repeated heights cross page boundaries; equality must skip only events
        # at or before the checkpoint. Some suffixes have only inline events.
        blocks = [
            {
                "height": i,
                "hash": h(f"nav{i}"),
                "prev_hash": h(f"nav{i - 1}"),
                "transactions": [
                    {
                        "index": j + 1,
                        "txid": h(f"nav{i}-{j}"),
                        "vin": [],
                        "vout": [{"n": 0, "script": S, "value_zat": 1}],
                    }
                    for j in range(100 if i < 8 else 2)
                ],
            }
            for i in range(1, 9)
        ]
        accepted = {0: h("nav0"), **{b["height"]: b["hash"] for b in blocks}}
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            folder = build_generation(
                "main", blocks, root / "generations", page_bytes=3584
            )
            for checkpoint in [0, 1, 4, 6, 7]:
                for budget in [1, 1000]:
                    with self.subTest(checkpoint=checkpoint, budget=budget):
                        initial, expected, utxos = oracle(blocks, [S], checkpoint)
                        path = root / "state.json"
                        save(
                            path,
                            new_state(
                                "main", [S], checkpoint, accepted[checkpoint], initial
                            ),
                        )
                        for _ in range(40):
                            state = sync(
                                path,
                                folder,
                                accepted,
                                FileTransport(),
                                budget=budget,
                                navigation=True,
                            )
                            if state["pending"] is None:
                                break
                            self.assertEqual(state["height"], checkpoint)
                            self.assertEqual(state["utxos"], initial)
                            self.assertEqual(state, json.loads(path.read_text()))
                        verify(state, expected, utxos, 8)

            class CorruptBounds(FileTransport):
                def fetch(self, folder, manifest, table, rows):
                    decoded, cost = super().fetch(folder, manifest, table, rows)
                    if table == "pages":
                        row = rows[0]
                        raw = bytearray(decoded[row])
                        fields = list(PAGE.unpack_from(raw))
                        fields[-1] += 1
                        PAGE.pack_into(raw, 0, *fields)
                        decoded[row] = bytes(raw)
                    return decoded, cost

            initial, _, _ = oracle(blocks, [S], 4)
            save(path, new_state("main", [S], 4, accepted[4], initial))
            with self.assertRaisesRegex(ValueError, "page height bounds"):
                sync(path, folder, accepted, CorruptBounds(), navigation=True)
            self.assertEqual(json.loads(path.read_text())["height"], 4)


if __name__ == "__main__":
    unittest.main()
