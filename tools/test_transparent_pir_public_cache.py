"""Public-cache accounting tests; the subprocess is a declared test double."""

import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from test_transparent_pir_incremental import chain
from transparent_pir_incremental import build_generation, PirTransport


class PublicCacheTests(unittest.TestCase):
    def test_slot_table_generation_and_eviction_accounting(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            folder = build_generation("main", chain(), root / "generations")
            manifest = json.loads((folder / "manifest.json").read_text())
            cache = set()
            transport = PirTransport(root / "fake-binary", root / "queries", cache)
            slots = [1]

            def fake_run(command, stdout, **kwargs):
                width, rows = int(command[2]), list(map(int, command[4].split(",")))
                raw = Path(command[1]).read_bytes()
                json.dump(
                    {
                        "public_sets": slots[0],
                        "published_setup_bytes": slots[0] * width * 4,
                        "samples": [
                            {"upload_bytes": 10, "response_bytes": 20, "total_ms": 1}
                            for _ in rows
                        ],
                        "decoded_rows": [
                            {"row": r, "bytes": list(raw[r * width : (r + 1) * width])}
                            for r in rows
                        ],
                    },
                    stdout,
                )

            with patch("transparent_pir_incremental.subprocess.run", fake_run):

                def charged(table="directory", source=folder, meta=manifest):
                    return transport.fetch(source, meta, table, [0])[1][
                        "setup_download_bytes"
                    ]

                self.assertEqual(charged(), 14336)
                slots[0] = 4
                self.assertEqual(charged(), 3 * 14336)
                self.assertEqual(charged(), 0)
                self.assertEqual(charged("pages"), 4 * 71680)
                changed = chain()
                changed[-1]["hash"] = "11" * 32
                newer = build_generation("main", changed, root / "generations")
                self.assertEqual(
                    charged(
                        source=newer,
                        meta=json.loads((newer / "manifest.json").read_text()),
                    ),
                    4 * 14336,
                )
                cache.clear()
                self.assertEqual(charged(), 4 * 14336)


if __name__ == "__main__":
    unittest.main()
