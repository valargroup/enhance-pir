"""Filter-format dispatch: BIP 158 generations alongside the original Bloom ones.

Cleartext transports throughout; these are state-machine tests, never evidence
about private query behaviour or performance.
"""

import copy
import hashlib
import json
import os
from pathlib import Path
import shutil
import tempfile
import unittest

from transparent_pir_incremental import (
    BIP158_V1,
    BLOOM_V1,
    FileTransport,
    build_generation,
    complete_scripts,
    new_state,
    read_targets,
    save,
    sync,
)
from test_transparent_pir_incremental import chain, h, S, T


def find_cli():
    """The release transparent-filter-cli, if it has been built."""
    override = os.environ.get("TRANSPARENT_FILTER_CLI")
    if override:
        return Path(override)
    root = Path(__file__).resolve().parent.parent
    for profile in ("release", "debug"):
        candidate = root / "target" / profile / "transparent-filter-cli"
        if candidate.exists():
            return candidate
    return None


CLI = find_cli()
requires_cli = unittest.skipIf(
    CLI is None,
    "build it with: cargo build --release -p transparent-filter --features cli",
)


class ElementSetTests(unittest.TestCase):
    """Extraction rules, independent of any encoder."""

    def test_complete_scripts_keeps_scripts_the_private_backend_cannot_serve(self):
        block = {
            "height": 1,
            "hash": h("b1"),
            "prev_hash": h("b0"),
            "transactions": [
                {
                    "index": 1,
                    "txid": h("tx"),
                    "vin": [],
                    # A bare multisig-ish script: not a supported address type,
                    # but it must still appear in a shared public filter.
                    "vout": [
                        {"script": "5152ae", "value_zat": 1, "n": 0},
                        {"script": S, "value_zat": 2, "n": 1},
                    ],
                }
            ],
        }
        self.assertEqual(complete_scripts(block), sorted(["5152ae", S]))

    def test_leading_op_return_excluded_but_embedded_0x6a_kept(self):
        embedded = "76a914" + "6a" * 20 + "88ac"
        block = {
            "height": 1,
            "hash": h("b1"),
            "prev_hash": h("b0"),
            "transactions": [
                {
                    "index": 1,
                    "txid": h("tx"),
                    "vin": [],
                    "vout": [
                        {"script": "6a04deadbeef", "value_zat": 0, "n": 0},
                        {"script": embedded, "value_zat": 1, "n": 1},
                        {"script": "", "value_zat": 0, "n": 2},
                    ],
                }
            ],
        }
        self.assertEqual(complete_scripts(block), [embedded])

    def test_spent_scripts_are_included_and_deduplicated(self):
        block = {
            "height": 1,
            "hash": h("b1"),
            "prev_hash": h("b0"),
            "transactions": [
                {
                    "index": 1,
                    "txid": h("tx"),
                    "vin": [{"script": S, "value_zat": 7, "txid": h("p"), "n": 0}],
                    "vout": [{"script": S, "value_zat": 6, "n": 0}],
                }
            ],
        }
        self.assertEqual(complete_scripts(block), [S])


@requires_cli
class Bip158GenerationTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.blocks = chain()
        self.accepted = {0: h("b0"), **{b["height"]: b["hash"] for b in self.blocks}}
        self.path = self.root / "state.json"
        save(self.path, new_state("main", [S], 0, h("b0")))

    def tearDown(self):
        self.tmp.cleanup()

    def build(self, name, fmt):
        return build_generation(
            "main", self.blocks, self.root / name,
            filter_format=fmt, filter_cli=CLI if fmt == BIP158_V1 else None,
        )

    def manifest(self, folder):
        return json.loads((folder / "manifest.json").read_bytes())

    def test_the_format_is_recorded_and_changes_the_generation_id(self):
        bloom = self.build("bloom", BLOOM_V1)
        bip = self.build("bip158", BIP158_V1)
        self.assertEqual(self.manifest(bloom)["filter_format"], BLOOM_V1)
        self.assertEqual(self.manifest(bip)["filter_format"], BIP158_V1)
        # Different filter bytes must not share a generation id, or a cache
        # entry from one format could be served as the other.
        self.assertNotEqual(bloom.name, bip.name)

    def test_the_two_formats_produce_different_bytes(self):
        bloom = (self.build("bloom", BLOOM_V1) / "filters.bin").read_bytes()
        bip = (self.build("bip158", BIP158_V1) / "filters.bin").read_bytes()
        self.assertNotEqual(bloom, bip)
        # No size assertion here: four synthetic blocks holding one script each
        # are too small to say anything about encoding efficiency, and sizes
        # legitimately tie. That comparison is made over the mainnet day.

    def test_sync_recovers_the_same_ledger_under_both_formats(self):
        results = {}
        for name, fmt in (("bloom", BLOOM_V1), ("bip158", BIP158_V1)):
            path = self.root / f"state-{name}.json"
            save(path, new_state("main", [S], 0, h("b0")))
            folder = self.build(name, fmt)
            state = sync(
                path, folder, self.accepted, FileTransport(),
                filter_cli=CLI if fmt == BIP158_V1 else None,
            )
            results[name] = (state["events"], state["utxos"], state["height"])
        self.assertEqual(results["bloom"], results["bip158"])
        self.assertEqual(results["bip158"][2], self.blocks[-1]["height"])

    def test_a_bip158_generation_without_a_cli_is_refused(self):
        folder = self.build("bip158", BIP158_V1)
        path = self.root / "state-nocli.json"
        save(path, new_state("main", [S], 0, h("b0")))
        with self.assertRaises(ValueError):
            sync(path, folder, self.accepted, FileTransport())

    def test_an_absent_script_is_covered_with_no_targets(self):
        folder = self.build("bip158", BIP158_V1)
        manifest = self.manifest(folder)
        targets, size = read_targets(
            folder, manifest, [T], 0, self.accepted, filter_cli=CLI
        )
        self.assertEqual(targets, [])
        self.assertGreater(size, 0)

    def test_every_present_script_matches_its_block(self):
        folder = self.build("bip158", BIP158_V1)
        manifest = self.manifest(folder)
        targets, _ = read_targets(
            folder, manifest, [S], 0, self.accepted, filter_cli=CLI
        )
        self.assertEqual(targets, [S])

    def test_an_unknown_filter_format_is_rejected(self):
        with self.assertRaises(ValueError):
            build_generation("main", self.blocks, self.root / "bad",
                             filter_format="bip158-v2-imaginary")
        folder = self.build("bip158", BIP158_V1)
        manifest = self.manifest(folder)
        manifest["filter_format"] = "something-else"
        with self.assertRaises(ValueError):
            read_targets(folder, manifest, [S], 0, self.accepted, filter_cli=CLI)

    def test_bloom_bytes_are_not_reinterpreted_as_bip158(self):
        bloom = self.build("bloom", BLOOM_V1)
        manifest = self.manifest(bloom)
        # Claiming the new format over old bytes must fail on the header, not
        # quietly produce matches from a Bloom bitmap read as a GCS stream.
        manifest["filter_format"] = BIP158_V1
        with self.assertRaises(ValueError):
            read_targets(bloom, manifest, [S], 0, self.accepted, filter_cli=CLI)

    def test_bip158_bytes_are_not_reinterpreted_as_bloom(self):
        folder = self.build("bip158", BIP158_V1)
        manifest = self.manifest(folder)
        manifest["filter_format"] = BLOOM_V1
        with self.assertRaises(ValueError):
            read_targets(folder, manifest, [S], 0, self.accepted, filter_cli=CLI)

    def test_a_generation_with_no_format_field_is_read_as_bloom(self):
        """Old generations predate the field and must still reproduce."""
        folder = self.build("bloom", BLOOM_V1)
        manifest = self.manifest(folder)
        del manifest["filter_format"]
        targets, _ = read_targets(folder, manifest, [S], 0, self.accepted)
        self.assertEqual(targets, [S])

    def test_resume_across_a_format_change_is_refused(self):
        folder = self.build("bip158", BIP158_V1)
        path = self.root / "state-resume.json"
        save(path, new_state("main", [S], 0, h("b0")))
        # Stop part way, leaving durable pending work bound to this format.
        try:
            sync(path, folder, self.accepted, FileTransport(), budget=0,
                 filter_cli=CLI)
        except Exception:
            pass
        state = json.loads(path.read_text())
        if state.get("pending") is None:
            self.skipTest("this generation completed without pausing")
        state["pending"]["filter_format"] = BLOOM_V1
        save(path, state)
        with self.assertRaises(ValueError) as raised:
            sync(path, folder, self.accepted, FileTransport(), filter_cli=CLI)
        self.assertIn("filter format", str(raised.exception))


if __name__ == "__main__":
    unittest.main()
