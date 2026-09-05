"""State-machine tests use explicit cleartext transports, never performance evidence."""

import copy
import hashlib
import json
import os
from pathlib import Path
import tempfile
import unittest

from transparent_pir_incremental import (
    build_generation,
    new_state,
    save,
    sync,
    rewind,
    FileTransport,
    PAGE,
    ChargedFailure,
)


def h(name):
    return hashlib.sha256(name.encode()).hexdigest()


S = "76a914" + "11" * 20 + "88ac"
T = "76a914" + "22" * 20 + "88ac"


def chain():
    blocks = []
    for i in range(1, 5):
        blocks.append(
            {
                "height": i,
                "hash": h(f"b{i}"),
                "prev_hash": h(f"b{i - 1}"),
                "transactions": [],
            }
        )
    blocks[0]["transactions"] = [
        {
            "index": 1,
            "txid": h("receive"),
            "vin": [],
            "vout": [{"script": S, "value_zat": 7, "n": 0}],
        }
    ]
    blocks[1]["transactions"] = [
        {
            "index": 1,
            "txid": h("spend"),
            "vin": [{"script": S, "value_zat": 7, "txid": h("receive"), "n": 0}],
            "vout": [],
        }
    ]
    blocks[2]["transactions"] = [
        {
            "index": 0,
            "txid": h("coinbase"),
            "vin": [],
            "vout": [{"script": S, "value_zat": 10, "n": 0}],
        }
    ]
    blocks[3]["transactions"] = [
        {
            "index": 1,
            "txid": h("tail"),
            "vin": [],
            "vout": [{"script": S, "value_zat": 3, "n": 0}],
        }
    ]
    return blocks


class IncrementalTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.blocks = chain()
        self.accepted = {0: h("b0"), **{b["height"]: b["hash"] for b in self.blocks}}
        self.folder = build_generation("main", self.blocks, self.root / "generations")
        self.path = self.root / "state.json"
        save(self.path, new_state("main", [S], 0, h("b0")))

    def tearDown(self):
        self.tmp.cleanup()

    def read(self):
        return json.loads(self.path.read_text())

    def run_sync(self, **kwargs):
        return sync(
            self.path,
            self.folder,
            self.accepted,
            kwargs.pop("transport", FileTransport()),
            **kwargs,
        )

    def test_exact_offline_receive_spend_and_coinbase(self):
        state = self.run_sync()
        self.assertEqual(len(state["events"]), 4)
        self.assertNotIn(h("receive") + ":0", state["utxos"])
        self.assertTrue(state["utxos"][h("coinbase") + ":0"]["coinbase"])
        self.assertEqual(state["height"], 4)

    def test_budget_restart_does_not_advance_coverage(self):
        state = self.run_sync(budget=1)
        self.assertEqual(state["height"], 0)
        self.assertIsNotNone(state["pending"])
        self.assertEqual(state["events"], [])
        state = self.run_sync(budget=1)
        self.assertEqual(state["height"], 4)

    def test_zero_budget_and_idempotent_completion(self):
        self.assertEqual(self.run_sync(budget=0)["height"], 0)
        state = self.run_sync()
        before = copy.deepcopy(state)
        transport = FileTransport()
        self.assertEqual(self.run_sync(transport=transport), before)
        self.assertEqual(transport.calls, [])

    def test_absent_script_finishes_without_private_query(self):
        save(self.path, new_state("main", [T], 0, h("b0")))
        transport = FileTransport()
        self.assertEqual(self.run_sync(transport=transport)["height"], 4)
        self.assertEqual(transport.calls, [])

    def test_filter_false_positive_is_not_address_use(self):
        # Search a tiny fixture's intentionally high-FP filter, then check exact absence.
        folder = build_generation(
            "main", self.blocks, self.root / "fp", probability=0.5
        )
        from transparent_pir_incremental import read_targets

        manifest = json.loads((folder / "manifest.json").read_text())
        for i in range(1000):
            script = (
                "76a914" + hashlib.sha256(str(i).encode()).digest()[:20].hex() + "88ac"
            )
            if read_targets(folder, manifest, [script], 0, self.accepted)[0]:
                break
        else:
            self.fail("no deterministic false positive found")
        save(self.path, new_state("main", [script], 0, h("b0")))
        transport = FileTransport()
        state = sync(self.path, folder, self.accepted, transport)
        self.assertTrue(transport.calls)
        self.assertEqual(state["events"], [])
        self.assertEqual(state["utxos"], {})

    def test_missing_filter_fails_closed(self):
        (self.folder / "filters.bin").unlink()
        with self.assertRaises(FileNotFoundError):
            self.run_sync()
        self.assertEqual(self.read()["height"], 0)

    def test_corrupt_filter_and_stale_anchor_fail_closed(self):
        raw = (self.folder / "filters.bin").read_bytes()
        (self.folder / "filters.bin").write_bytes(raw[:-1])
        with self.assertRaises(ValueError):
            self.run_sync()
        (self.folder / "filters.bin").write_bytes(raw)
        self.accepted[4] = h("other")
        with self.assertRaises(ValueError):
            self.run_sync()

    def test_malformed_page_never_commits(self):
        class Corrupt(FileTransport):
            def fetch(inner, folder, manifest, table, rows):
                data, cost = super().fetch(folder, manifest, table, rows)
                if table == "pages":
                    for row in data:
                        raw = bytearray(data[row])
                        raw[1] ^= 1
                        data[row] = bytes(raw)
                return data, cost

        with self.assertRaises(ValueError):
            self.run_sync(transport=Corrupt())
        self.assertEqual(self.read()["height"], 0)
        self.assertEqual(self.read()["events"], [])

    def test_missing_receive_is_unresolved(self):
        blocks = self.blocks[1:]
        folder = build_generation("main", blocks, self.root / "missing")
        save(self.path, new_state("main", [S], 1, h("b1")))
        with self.assertRaisesRegex(ValueError, "unresolved"):
            sync(self.path, folder, self.accepted, FileTransport())
        self.assertEqual(self.read()["height"], 1)

    def test_unavailable_service_preserves_pending(self):
        class Unavailable(FileTransport):
            def fetch(self, *args):
                raise TimeoutError("injected timeout")

        with self.assertRaises(TimeoutError):
            self.run_sync(transport=Unavailable())
        self.assertIsNotNone(self.read()["pending"])
        self.assertEqual(self.read()["height"], 0)
        self.assertEqual(self.run_sync()["height"], 4)

    def test_lost_response_charged_and_retried(self):
        class Lost(FileTransport):
            def fetch(self, *args):
                raise ChargedFailure(
                    {"queries": 1, "upload_bytes": 100, "response_bytes": 20}
                )

        with self.assertRaises(ChargedFailure):
            self.run_sync(transport=Lost())
        self.assertEqual(self.read()["cost"]["queries"], 1)
        self.assertEqual(self.read()["height"], 0)
        self.assertEqual(self.run_sync()["height"], 4)

    def test_reorg_discards_tail_and_pending(self):
        state = self.run_sync()
        fork = copy.deepcopy(self.blocks)
        fork[2]["hash"] = h("fork3")
        fork[3]["prev_hash"] = h("fork3")
        fork[3]["hash"] = h("fork4")
        fork[2]["transactions"] = []
        fork[3]["transactions"] = []
        accepted = {0: h("b0"), **{b["height"]: b["hash"] for b in fork}}
        rewind(state, 2, h("b2"), accepted)
        save(self.path, state)
        self.assertEqual(len(state["events"]), 2)
        self.assertEqual(state["utxos"], {})
        folder = build_generation("main", fork, self.root / "fork")
        state = sync(self.path, folder, accepted, FileTransport())
        self.assertEqual(state["utxos"], {})
        self.assertEqual(state["height"], 4)

    def test_generation_switch_requires_explicit_rewind(self):
        self.run_sync(budget=1)
        extended = self.blocks + [
            {"height": 5, "hash": h("b5"), "prev_hash": h("b4"), "transactions": []}
        ]
        folder = build_generation("main", extended, self.root / "next")
        accepted = {**self.accepted, 5: h("b5")}
        with self.assertRaisesRegex(ValueError, "original retained generation"):
            sync(self.path, folder, accepted, FileTransport())
        self.assertEqual(self.run_sync()["height"], 4)
        self.assertEqual(
            sync(self.path, folder, accepted, FileTransport())["height"], 5
        )

    def test_tail_sealing_preserves_exact_history(self):
        earlier = build_generation("main", self.blocks[:3], self.root / "earlier")
        sync(self.path, earlier, self.accepted, FileTransport())
        state = self.run_sync()
        self.assertEqual(len(state["events"]), 4)
        self.assertEqual(len(state["utxos"]), 2)

    def test_invalid_common_ancestor_rejected(self):
        state = self.run_sync()
        with self.assertRaises(ValueError):
            rewind(state, 2, h("not-old-chain"), {2: h("not-old-chain")})

    def test_coinbase_flag_cannot_reuse_a_received_outpoint(self):
        from transparent_pir_incremental import apply_events, EVENT

        receive = EVENT.pack(
            1, 1, 1, bytes.fromhex(h("outpoint")), 0, 7, bytes(32)
        ).hex()
        spend = EVENT.pack(
            2, 2, 1, bytes.fromhex(h("outpoint")), 0, 7, bytes.fromhex(h("spender"))
        ).hex()
        forged = EVENT.pack(
            3, 3, 0, bytes.fromhex(h("outpoint")), 0, 7, bytes(32)
        ).hex()
        with self.assertRaisesRegex(ValueError, "duplicate event"):
            apply_events({}, [[S, receive], [S, spend], [S, forged]])

    def test_wrong_generation_table_fails_before_lookup(self):
        table = self.folder / "directory.bin"
        raw = bytearray(table.read_bytes())
        raw[0] ^= 1
        table.write_bytes(raw)
        with self.assertRaisesRegex(ValueError, "table does not belong"):
            self.run_sync()
        self.assertEqual(self.read()["height"], 0)

    def test_changed_scope_cannot_inherit_pending_coverage(self):
        self.run_sync(budget=1)
        state = self.read()
        state["scripts"].append(T)
        save(self.path, state)
        with self.assertRaisesRegex(ValueError, "scope changed"):
            self.run_sync()

    def test_reorg_undoes_spend_and_invalidates_inflight_pages(self):
        self.run_sync(budget=1)
        state = self.read()
        rewind(state, 0, h("b0"), self.accepted)
        save(self.path, state)
        self.assertIsNone(state["pending"])
        state = self.run_sync()
        rewind(state, 1, h("b1"), self.accepted)
        self.assertEqual(state["utxos"][h("receive") + ":0"]["value"], 7)

    def test_multiple_scripts_self_transfer(self):
        blocks = chain()
        blocks[1]["transactions"][0]["vout"] = [{"script": T, "value_zat": 6, "n": 0}]
        folder = build_generation("main", blocks, self.root / "self-transfer")
        save(self.path, new_state("main", [S, T], 0, h("b0")))
        state = sync(self.path, folder, self.accepted, FileTransport())
        self.assertEqual(len(state["events"]), 5)
        self.assertEqual(state["utxos"][h("spend") + ":0"]["script"], T)

    def test_page_height_bounds_checked(self):
        class CorruptBounds(FileTransport):
            def fetch(inner, folder, manifest, table, rows):
                data, cost = super().fetch(folder, manifest, table, rows)
                if table == "pages":
                    for row in data:
                        raw = data[row]
                        fields = list(PAGE.unpack_from(raw))
                        fields[-1] += 1
                        data[row] = PAGE.pack(*fields) + raw[PAGE.size :]
                return data, cost

        with self.assertRaisesRegex(ValueError, "height bounds"):
            self.run_sync(transport=CorruptBounds())
        self.assertEqual(self.read()["height"], 0)

    def test_unsupported_scope_and_cross_network(self):
        with self.assertRaises(ValueError):
            new_state("main", ["00"], 0, h("b0"))
        state = self.read()
        state["network"] = "test"
        save(self.path, state)
        with self.assertRaises(ValueError):
            self.run_sync()


@unittest.skipUnless(os.environ.get("PIR_TEST_BINARY"), "requires the real PIR harness")
class RealPirRecoveryTests(unittest.TestCase):
    def test_tail_sealing_and_reorg_with_real_private_rows(self):
        from transparent_pir_incremental import PirTransport

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            evidence = Path(os.environ.get("PIR_TEST_OUTPUT", str(root / "evidence")))
            blocks = chain()
            accepted = {0: h("b0"), **{b["height"]: b["hash"] for b in blocks}}
            statepath = root / "state.json"
            save(statepath, new_state("main", [S], 0, h("b0")))
            transport = PirTransport(os.environ["PIR_TEST_BINARY"], evidence)
            early = build_generation("main", blocks[:3], root / "generations")
            state = sync(statepath, early, accepted, transport)
            self.assertEqual(state["height"], 3)
            full = build_generation("main", blocks, root / "generations")
            state = sync(statepath, full, accepted, transport)
            self.assertEqual(len(state["events"]), 4)
            self.assertEqual(
                set(state["utxos"]), {h("coinbase") + ":0", h("tail") + ":0"}
            )
            fork = copy.deepcopy(blocks)
            fork[2]["hash"] = h("fork3")
            fork[3]["prev_hash"] = h("fork3")
            fork[3]["hash"] = h("fork4")
            fork[2]["transactions"] = [
                {
                    "index": 1,
                    "txid": h("fork-receive"),
                    "vin": [],
                    "vout": [{"script": S, "value_zat": 5, "n": 0}],
                }
            ]
            fork[3]["transactions"] = [
                {
                    "index": 1,
                    "txid": h("fork-spend"),
                    "vin": [
                        {"script": S, "value_zat": 5, "txid": h("fork-receive"), "n": 0}
                    ],
                    "vout": [],
                }
            ]
            accepted = {0: h("b0"), **{b["height"]: b["hash"] for b in fork}}
            rewind(state, 2, h("b2"), accepted)
            save(statepath, state)
            generation = build_generation("main", fork, root / "generations")
            state = sync(statepath, generation, accepted, transport)
            self.assertEqual(state["height"], 4)
            self.assertEqual(state["hash"], h("fork4"))
            self.assertEqual(len(state["events"]), 4)
            self.assertEqual(state["utxos"], {})
            (evidence / "recovery-result.json").write_text(
                json.dumps(
                    {
                        "classification": "synthetic fork with real PIR retrieval",
                        "tail_sealing": "pass",
                        "reorg_rollback_and_fork_receive_spend": "pass",
                        "cost": state["cost"],
                        "verified_events": len(state["events"]),
                        "final_utxos": len(state["utxos"]),
                    },
                    indent=2,
                )
                + "\n"
            )


if __name__ == "__main__":
    unittest.main()
