from __future__ import annotations

import copy
import hashlib
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from gateway.record_only import census_binding
from gateway.record_only.census_binding import (
    CensusBindingError,
    verify_target_outbound_census,
)


EVIDENCE_ROOT = (
    Path(__file__).resolve().parents[4]
    / "evidence"
    / "target-outbound-census"
)
OLD_SUPERSEDED_V2_SHA256 = (
    "5954ed1976c85b27c473e294514b3ea6dd021dbcc5ae9d1a8ae0f35d23bb5c94"
)


class CensusBindingTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.index_raw = (EVIDENCE_ROOT / "INDEX.json").read_bytes()
        cls.census_raw = (EVIDENCE_ROOT / "census-v4.json").read_bytes()
        cls.index = json.loads(cls.index_raw)
        cls.census = json.loads(cls.census_raw)

    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name).resolve() / "evidence"
        self.root.mkdir(mode=0o700)

    def tearDown(self) -> None:
        self.temp.cleanup()

    def _write_regular(self, name: str, raw: bytes) -> Path:
        path = self.root / name
        path.write_bytes(raw)
        path.chmod(0o600)
        return path

    def _write_valid_evidence(self) -> None:
        self._write_regular("INDEX.json", self.index_raw)
        self._write_regular("census-v4.json", self.census_raw)

    @staticmethod
    def _encoded(value: object) -> bytes:
        return json.dumps(value, indent=2, sort_keys=True).encode("utf-8") + b"\n"

    def _verify_mutated(
        self,
        *,
        index: dict | None = None,
        census: dict | None = None,
    ):
        index_value = copy.deepcopy(index if index is not None else self.index)
        census_value = copy.deepcopy(census if census is not None else self.census)
        census_raw = self._encoded(census_value)
        census_sha = hashlib.sha256(census_raw).hexdigest()
        index_value["canonical"]["sha256"] = census_sha
        index_raw = self._encoded(index_value)
        index_sha = hashlib.sha256(index_raw).hexdigest()
        self._write_regular("INDEX.json", index_raw)
        self._write_regular(index_value["canonical"]["artifact"], census_raw)
        with patch.object(census_binding, "EXPECTED_INDEX_SHA256", index_sha), patch.object(
            census_binding, "EXPECTED_ARTIFACT_SHA256", census_sha
        ):
            return verify_target_outbound_census(self.root)

    def test_authoritative_index_and_artifact_verify_as_no_go_work_queue(self) -> None:
        self._write_valid_evidence()
        binding = verify_target_outbound_census(self.root)
        status = binding.as_status()
        self.assertEqual(binding.index_sha256, census_binding.EXPECTED_INDEX_SHA256)
        self.assertEqual(binding.artifact_name, "census-v4.json")
        self.assertEqual(binding.total_rows, 6338)
        self.assertEqual(binding.runtime_rows, 3612)
        self.assertEqual(binding.pending_rows, 6338)
        self.assertEqual(binding.unverified_rows, 6338)
        self.assertEqual(status["unclassified_executable_mode_count"], 5)
        for field in (
            "production_ready",
            "promotion_authorized",
            "candidate_execution_authorized",
            "cutover_authorized",
            "external_delivery_attempted",
            "external_delivery_verified",
            "record_only_coverage_complete",
            "runtime_egress_trace_complete",
            "dynamic_import_trace_complete",
            "skill_trace_complete",
            "subprocess_descendant_trace_complete",
        ):
            self.assertFalse(status[field], field)

    def test_old_v2_digest_cannot_pass_as_canonical(self) -> None:
        index = copy.deepcopy(self.index)
        index["canonical"]["sha256"] = OLD_SUPERSEDED_V2_SHA256
        index_raw = self._encoded(index)
        self._write_regular("INDEX.json", index_raw)
        self._write_regular("census-v4.json", self.census_raw)
        with patch.object(
            census_binding,
            "EXPECTED_INDEX_SHA256",
            hashlib.sha256(index_raw).hexdigest(),
        ):
            with self.assertRaisesRegex(CensusBindingError, "canonical artifact binding"):
                verify_target_outbound_census(self.root)

    def test_all_superseded_v1_v2_v3_artifact_names_are_refused(self) -> None:
        for artifact in ("census-v1.json", "census-v2.json", "census-v3.json"):
            with self.subTest(artifact=artifact):
                self.temp.cleanup()
                self.temp = tempfile.TemporaryDirectory()
                self.root = Path(self.temp.name).resolve() / "evidence"
                self.root.mkdir(mode=0o700)
                index = copy.deepcopy(self.index)
                index["canonical"]["artifact"] = artifact
                index_raw = self._encoded(index)
                self._write_regular("INDEX.json", index_raw)
                self._write_regular(artifact, self.census_raw)
                with patch.object(
                    census_binding,
                    "EXPECTED_INDEX_SHA256",
                    hashlib.sha256(index_raw).hexdigest(),
                ):
                    with self.assertRaisesRegex(
                        CensusBindingError, "canonical artifact binding"
                    ):
                        verify_target_outbound_census(self.root)

    def test_canonical_artifact_path_traversal_is_refused(self) -> None:
        index = copy.deepcopy(self.index)
        index["canonical"]["artifact"] = "../census-v4.json"
        index_raw = self._encoded(index)
        self._write_regular("INDEX.json", index_raw)
        with patch.object(
            census_binding,
            "EXPECTED_INDEX_SHA256",
            hashlib.sha256(index_raw).hexdigest(),
        ):
            with self.assertRaisesRegex(CensusBindingError, "canonical artifact"):
                verify_target_outbound_census(self.root)

    def test_index_gate_field_mismatch_is_refused_after_digest_check(self) -> None:
        index = copy.deepcopy(self.index)
        index["production_ready"] = True
        index_raw = self._encoded(index)
        self._write_regular("INDEX.json", index_raw)
        with patch.object(
            census_binding,
            "EXPECTED_INDEX_SHA256",
            hashlib.sha256(index_raw).hexdigest(),
        ):
            with self.assertRaisesRegex(CensusBindingError, "must remain exactly false"):
                verify_target_outbound_census(self.root)

    def test_duplicate_index_field_is_refused_after_digest_check(self) -> None:
        index_raw = self.index_raw.replace(
            b'  "status": "PROVISIONAL_STATIC_OUTBOUND_CENSUS_NO_GO",',
            b'  "status": "NO_GO_DUPLICATE",\n'
            b'  "status": "PROVISIONAL_STATIC_OUTBOUND_CENSUS_NO_GO",',
            1,
        )
        self.assertNotEqual(index_raw, self.index_raw)
        self._write_regular("INDEX.json", index_raw)
        with patch.object(
            census_binding,
            "EXPECTED_INDEX_SHA256",
            hashlib.sha256(index_raw).hexdigest(),
        ):
            with self.assertRaisesRegex(CensusBindingError, "duplicate JSON key"):
                verify_target_outbound_census(self.root)

    def test_artifact_hash_mismatch_is_refused(self) -> None:
        self._write_regular("INDEX.json", self.index_raw)
        self._write_regular("census-v4.json", self.census_raw + b" ")
        with self.assertRaisesRegex(CensusBindingError, "artifact SHA-256 mismatch"):
            verify_target_outbound_census(self.root)

    def test_source_provenance_field_mismatch_is_refused(self) -> None:
        census = copy.deepcopy(self.census)
        census["source_commit"] = "0" * 40
        with self.assertRaisesRegex(CensusBindingError, "provenance or safety"):
            self._verify_mutated(census=census)

    def test_manifest_reference_traversal_is_refused(self) -> None:
        census = copy.deepcopy(self.census)
        census["source_tree_manifest"] = "/tmp/../tree-inventory.tsv"
        with self.assertRaisesRegex(CensusBindingError, "expected manifest reference"):
            self._verify_mutated(census=census)

    def test_scanner_count_and_mode_anomaly_mismatches_are_refused(self) -> None:
        for mutation in ("counts", "mode"):
            with self.subTest(mutation=mutation):
                self.temp.cleanup()
                self.temp = tempfile.TemporaryDirectory()
                self.root = Path(self.temp.name).resolve() / "evidence"
                self.root.mkdir(mode=0o700)
                census = copy.deepcopy(self.census)
                if mutation == "counts":
                    census["counts"]["runtime_rows"] = 3611
                    expected = "scanner counts"
                else:
                    census["scanner"]["unclassified_executables"] = []
                    expected = "scanner status"
                with self.assertRaisesRegex(CensusBindingError, expected):
                    self._verify_mutated(census=census)

    def test_non_pending_row_is_refused_even_with_matching_hashes(self) -> None:
        census = copy.deepcopy(self.census)
        census["rows"][0]["review_status"] = "verified"
        with self.assertRaisesRegex(CensusBindingError, "not pending/unverified"):
            self._verify_mutated(census=census)

    def test_symlink_and_hardlink_index_are_refused(self) -> None:
        source = self.root / "index-source"
        source.write_bytes(self.index_raw)
        source.chmod(0o600)
        (self.root / "INDEX.json").symlink_to(source)
        with self.assertRaises(CensusBindingError):
            verify_target_outbound_census(self.root)
        (self.root / "INDEX.json").unlink()
        os.link(source, self.root / "INDEX.json")
        with self.assertRaisesRegex(CensusBindingError, "link"):
            verify_target_outbound_census(self.root)

    def test_symlink_and_hardlink_artifact_are_refused(self) -> None:
        self._write_regular("INDEX.json", self.index_raw)
        source = self.root / "census-source"
        source.write_bytes(self.census_raw)
        source.chmod(0o600)
        (self.root / "census-v4.json").symlink_to(source)
        with self.assertRaises(CensusBindingError):
            verify_target_outbound_census(self.root)
        (self.root / "census-v4.json").unlink()
        os.link(source, self.root / "census-v4.json")
        with self.assertRaisesRegex(CensusBindingError, "link"):
            verify_target_outbound_census(self.root)

    def test_path_replacement_after_open_is_detected_as_toctou(self) -> None:
        self._write_valid_evidence()
        replaced = False

        def replace_path(name: str) -> None:
            nonlocal replaced
            if name != "INDEX.json" or replaced:
                return
            replaced = True
            original = self.root / name
            original.rename(self.root / "INDEX.old")
            self._write_regular(name, self.index_raw)

        with self.assertRaisesRegex(CensusBindingError, "modified|path identity changed"):
            verify_target_outbound_census(
                self.root, _after_open_hook=replace_path
            )

    def test_noncanonical_or_symlink_evidence_root_is_refused(self) -> None:
        self._write_valid_evidence()
        link = self.root.parent / "evidence-link"
        link.symlink_to(self.root, target_is_directory=True)
        with self.assertRaisesRegex(CensusBindingError, "real directory"):
            verify_target_outbound_census(link)
        traversed = self.root / ".." / self.root.name
        with self.assertRaisesRegex(CensusBindingError, "canonical"):
            verify_target_outbound_census(traversed)


if __name__ == "__main__":
    unittest.main()
