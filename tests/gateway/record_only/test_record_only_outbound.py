from __future__ import annotations

import asyncio
import json
import os
import tempfile
import threading
import time
import unittest
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from gateway.record_only.transport import (
    GatewayRecordAdapter,
    RecordOnlyError,
    RecordOnlyOutboundTransport,
    RecordOnlyRelaySender,
)


KEY = b"record-only-test-key-32-bytes-minimum-value"


class RecordOnlyTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name).resolve()
        self.records = self.root / "records"
        self.transport = RecordOnlyOutboundTransport(
            self.records,
            id_hash_key=KEY,
            source_component="test.gateway",
            clock=lambda: datetime(2026, 7, 10, 12, 0, tzinfo=timezone.utc),
        )

    def tearDown(self) -> None:
        self.transport.close()
        self.temp.cleanup()

    def test_text_record_hashes_ids_and_preserves_chinese_cifs(self) -> None:
        result = self.transport.record(
            operation="text_reply",
            platform="feishu",
            destination_kind="thread",
            destination_id="oc_private_chat",
            thread_id="om_private_thread",
            payload_type="text",
            payload="完成：中文路径 //hfs1.minieye.tech/department-pnc_team/tmp/a b/",
            mention_ids=["ou_private_user"],
            link_values=["//hfs1.minieye.tech/department-pnc_team/tmp/a b/"],
            task_id="task-001",
            terminal_state="completed",
            reply_mode="thread",
        )
        self.assertTrue(result.success)
        raw = self.transport.ledger.read_text()
        for secret in ("oc_private_chat", "om_private_thread", "ou_private_user"):
            self.assertNotIn(secret, raw)
        row = self.transport.read_all()[0]
        self.assertIn("中文路径", row["payload"])
        self.assertTrue(row["destination"]["id_hash"].startswith("hmac-sha256:"))
        self.assertEqual(row["terminal_state"], "completed")
        self.assertEqual(len(row["mentions"]), 1)
        self.assertIn(
            "//hfs1.minieye.tech/department-pnc_team/tmp/a b/", row["links"]
        )

    def test_dedupe_replay_writes_one_line(self) -> None:
        kwargs = dict(
            operation="text_send",
            platform="feishu",
            destination_kind="chat",
            destination_id="oc_a",
            payload_type="text",
            payload="same",
            caller_dedupe_key="caller-1",
        )
        first = self.transport.record(**kwargs)
        second = self.transport.record(**kwargs)
        self.assertFalse(first.duplicate)
        self.assertTrue(second.duplicate)
        self.assertEqual(first.record_id, second.record_id)
        rows = self.transport.read_all()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["attempt_count"], 2)
        self.assertEqual(second.attempt_count, 2)

    def test_card_payload_identifier_fields_and_inline_ids_are_hashed(self) -> None:
        self.transport.record(
            operation="card_send",
            platform="feishu",
            destination_kind="chat",
            destination_id="oc_card_chat",
            payload_type="interactive_card",
            payload={
                "open_id": "ou_card_user",
                "text": "hello ou_inline_user",
                "nested": {"message_id": "om_card_message"},
            },
            update_mode="create",
        )
        raw = self.transport.ledger.read_text()
        for value in ("oc_card_chat", "ou_card_user", "ou_inline_user", "om_card_message"):
            self.assertNotIn(value, raw)
        row = self.transport.read_all()[0]
        self.assertEqual(len(row["mentions"]), 2)

    def test_secret_payloads_fail_closed_without_ledger(self) -> None:
        for payload in (
            {"access_token": "secret-value"},
            "Authorization: Bearer abc",
            "https://user:password@example.invalid/path",
            "api_key=abc123",
            {"Access-Token": "secret-value"},
            {"CLIENT SECRET": "secret-value"},
            ("Authorization: Bearer tuple-secret",),
            '{"access_token":"quoted-secret"}',
            {"ＡＣＣＥＳＳ＿ＴＯＫＥＮ": "unicode-secret"},
            {"Cookie": "session=secret"},
            {"feishu_access_token": "suffix-secret"},
            {"x_api_key": "suffix-secret"},
            '{"access\\u005ftoken":"escaped-secret"}',
            "Bearer abcdefghijklmnop",
            "-----BEGIN PRIVATE KEY-----\nsecret",
            "https://example.invalid/path?token=query-secret",
        ):
            with self.subTest(payload=payload):
                with self.assertRaises(RecordOnlyError):
                    self.transport.record(
                        operation="text_send",
                        platform="feishu",
                        destination_kind="chat",
                        destination_id="oc_a",
                        payload_type="text",
                        payload=payload,
                    )
        self.assertFalse(self.transport.ledger.exists())

    def test_tuple_and_object_key_identifiers_are_redacted(self) -> None:
        self.transport.record(
            operation="card_send",
            platform="feishu",
            destination_kind="chat",
            destination_id="oc_tuple_chat",
            payload_type="interactive_card",
            payload={"ou_identifier_as_key": ("om_identifier_in_tuple",)},
            update_mode="create",
        )
        raw = self.transport.ledger.read_text()
        self.assertNotIn("ou_identifier_as_key", raw)
        self.assertNotIn("om_identifier_in_tuple", raw)

    def test_partial_existing_ledger_fails_without_overwrite(self) -> None:
        self.transport.ledger.write_bytes(b'{"schema_version":1')
        self.transport.ledger.chmod(0o600)
        before = self.transport.ledger.read_bytes()
        with self.assertRaisesRegex(RecordOnlyError, "partial"):
            self.transport.record(
                operation="text_send",
                platform="feishu",
                destination_kind="chat",
                destination_id="oc_a",
                payload_type="text",
                payload="hello",
            )
        self.assertEqual(self.transport.ledger.read_bytes(), before)

    def test_concurrent_writers_keep_unique_complete_jsonl(self) -> None:
        errors = []

        def writer(index: int) -> None:
            try:
                self.transport.record(
                    operation="text_send",
                    platform="feishu",
                    destination_kind="chat",
                    destination_id=f"oc_{index}",
                    payload_type="text",
                    payload=f"message {index}",
                )
            except Exception as exc:  # pragma: no cover - captured for assertion
                errors.append(exc)

        threads = [threading.Thread(target=writer, args=(index,)) for index in range(20)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(errors, [])
        self.assertEqual(len(self.transport.read_all()), 20)
        self.assertTrue(self.transport.ledger.read_bytes().endswith(b"\n"))

    def test_lock_name_replacement_cannot_split_transaction_lock(self) -> None:
        entered_replace = threading.Event()
        allow_replace = threading.Event()
        errors: list[tuple[str, Exception]] = []

        def hook(stage: str) -> None:
            if stage == "before_replace":
                entered_replace.set()
                if not allow_replace.wait(timeout=5):
                    raise RuntimeError("test barrier timed out")

        first = RecordOnlyOutboundTransport(
            self.records,
            id_hash_key=KEY,
            source_component="test.gateway",
            crash_hook=hook,
        )
        second = RecordOnlyOutboundTransport(
            self.records,
            id_hash_key=KEY,
            source_component="test.gateway",
        )

        def write(transport: RecordOnlyOutboundTransport, label: str) -> None:
            try:
                transport.record(
                    operation="text_send",
                    platform="feishu",
                    destination_kind="chat",
                    destination_id=f"oc_{label}",
                    payload_type="text",
                    payload=label,
                )
            except Exception as exc:  # pragma: no cover - captured for assertion
                errors.append((label, exc))

        first_thread = threading.Thread(target=write, args=(first, "first"))
        first_thread.start()
        self.assertTrue(entered_replace.wait(timeout=5))
        old_lock = self.records / ".outbound-records.lock.old"
        first.lock.rename(old_lock)
        first.lock.write_bytes(b"")
        first.lock.chmod(0o600)

        second_thread = threading.Thread(target=write, args=(second, "second"))
        second_thread.start()
        time.sleep(0.05)
        self.assertTrue(second_thread.is_alive(), "second writer bypassed directory inode lock")
        allow_replace.set()
        first_thread.join(timeout=5)
        second_thread.join(timeout=5)
        first.close()
        second.close()

        self.assertFalse(first_thread.is_alive())
        self.assertFalse(second_thread.is_alive())
        self.assertEqual([label for label, _ in errors], ["first"])
        self.assertIsInstance(errors[0][1], RecordOnlyError)
        self.assertEqual(len(self.transport.read_all()), 2)

    def test_authenticated_ledger_rejects_row_tamper(self) -> None:
        self.transport.record(
            operation="text_send",
            platform="feishu",
            destination_kind="chat",
            destination_id="oc_integrity",
            payload_type="text",
            payload="original",
            terminal_state="completed",
        )
        lines = self.transport.ledger.read_bytes().splitlines()
        row = json.loads(lines[1])
        row["terminal_state"] = "failed"
        lines[1] = json.dumps(
            row, ensure_ascii=False, sort_keys=True, separators=(",", ":")
        ).encode()
        self.transport.ledger.write_bytes(b"\n".join(lines) + b"\n")
        self.transport.ledger.chmod(0o600)
        with self.assertRaisesRegex(RecordOnlyError, "integrity"):
            self.transport.read_all()

    def test_authenticated_header_rejects_tail_truncation(self) -> None:
        for label in ("one", "two"):
            self.transport.record(
                operation="text_send",
                platform="feishu",
                destination_kind="chat",
                destination_id=f"oc_{label}",
                payload_type="text",
                payload=label,
            )
        lines = self.transport.ledger.read_bytes().splitlines()
        self.transport.ledger.write_bytes(b"\n".join(lines[:-1]) + b"\n")
        self.transport.ledger.chmod(0o600)
        with self.assertRaisesRegex(RecordOnlyError, "record count"):
            self.transport.read_all()

    def test_authenticated_header_rejects_resealed_old_census_binding(self) -> None:
        self.transport.record(
            operation="text_send",
            platform="feishu",
            destination_kind="chat",
            destination_id="oc_binding",
            payload_type="text",
            payload="bound",
        )
        lines = self.transport.ledger.read_bytes().splitlines()
        header = json.loads(lines[0])
        header["target_outbound_census_binding"]["canonical_artifact_sha256"] = (
            "5954ed1976c85b27c473e294514b3ea6dd021dbcc5ae9d1a8ae0f35d23bb5c94"
        )
        header_without_hmac = dict(header)
        header_without_hmac.pop("integrity_hmac_sha256")
        header["integrity_hmac_sha256"] = self.transport._integrity_hmac(
            header_without_hmac
        )
        lines[0] = json.dumps(
            header, ensure_ascii=False, sort_keys=True, separators=(",", ":")
        ).encode()
        self.transport.ledger.write_bytes(b"\n".join(lines) + b"\n")
        self.transport.ledger.chmod(0o600)
        with self.assertRaisesRegex(RecordOnlyError, "header invariants"):
            self.transport.read_all()

    def test_authenticated_header_rejects_resealed_authorization_flip(self) -> None:
        self.transport.record(
            operation="text_send",
            platform="feishu",
            destination_kind="chat",
            destination_id="oc_gate",
            payload_type="text",
            payload="bound",
        )
        lines = self.transport.ledger.read_bytes().splitlines()
        header = json.loads(lines[0])
        header["candidate_execution_authorized"] = True
        header_without_hmac = dict(header)
        header_without_hmac.pop("integrity_hmac_sha256")
        header["integrity_hmac_sha256"] = self.transport._integrity_hmac(
            header_without_hmac
        )
        lines[0] = json.dumps(
            header, ensure_ascii=False, sort_keys=True, separators=(",", ":")
        ).encode()
        self.transport.ledger.write_bytes(b"\n".join(lines) + b"\n")
        self.transport.ledger.chmod(0o600)
        with self.assertRaisesRegex(RecordOnlyError, "header invariants"):
            self.transport.read_all()

    def test_authenticated_ledger_rejects_duplicate_json_keys(self) -> None:
        self.transport.record(
            operation="text_send",
            platform="feishu",
            destination_kind="chat",
            destination_id="oc_duplicate_json",
            payload_type="text",
            payload="one",
        )
        ledger = self.transport.ledger.read_bytes()
        ledger = ledger.replace(
            b'"kind":"ledger_header"',
            b'"kind":"ledger_header","kind":"ledger_header"',
            1,
        )
        self.transport.ledger.write_bytes(ledger)
        self.transport.ledger.chmod(0o600)
        with self.assertRaisesRegex(RecordOnlyError, "duplicate JSON key"):
            self.transport.read_all()

    def test_observed_ledger_deletion_and_rollback_fail_closed(self) -> None:
        self.transport.record(
            operation="text_send",
            platform="feishu",
            destination_kind="chat",
            destination_id="oc_generation_one",
            payload_type="text",
            payload="one",
        )
        generation_one = self.transport.ledger.read_bytes()
        self.transport.record(
            operation="text_send",
            platform="feishu",
            destination_kind="chat",
            destination_id="oc_generation_two",
            payload_type="text",
            payload="two",
        )
        self.transport.ledger.write_bytes(generation_one)
        self.transport.ledger.chmod(0o600)
        with self.assertRaisesRegex(RecordOnlyError, "rolled back"):
            self.transport.read_all()

        self.transport.ledger.unlink()
        with self.assertRaisesRegex(RecordOnlyError, "disappeared"):
            self.transport.read_all()

    def test_crash_before_replace_keeps_old_ledger(self) -> None:
        self.transport.record(
            operation="text_send",
            platform="feishu",
            destination_kind="chat",
            destination_id="oc_old",
            payload_type="text",
            payload="old",
        )
        before = self.transport.ledger.read_bytes()
        crashing = RecordOnlyOutboundTransport(
            self.records,
            id_hash_key=KEY,
            source_component="test.gateway",
            crash_hook=lambda stage: (_ for _ in ()).throw(RuntimeError("before"))
            if stage == "before_replace"
            else None,
        )
        with self.assertRaisesRegex(RuntimeError, "before"):
            crashing.record(
                operation="text_send",
                platform="feishu",
                destination_kind="chat",
                destination_id="oc_new",
                payload_type="text",
                payload="new",
            )
        self.assertEqual(self.transport.ledger.read_bytes(), before)

    def test_crash_after_replace_is_recovered_by_dedupe(self) -> None:
        crashing = RecordOnlyOutboundTransport(
            self.records,
            id_hash_key=KEY,
            source_component="test.gateway",
            crash_hook=lambda stage: (_ for _ in ()).throw(RuntimeError("after"))
            if stage == "after_replace"
            else None,
        )
        kwargs = dict(
            operation="text_send",
            platform="feishu",
            destination_kind="chat",
            destination_id="oc_new",
            payload_type="text",
            payload="new",
        )
        with self.assertRaisesRegex(RuntimeError, "after"):
            crashing.record(**kwargs)
        recovered = self.transport.record(**kwargs)
        self.assertTrue(recovered.duplicate)
        rows = self.transport.read_all()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["attempt_count"], 2)

    def test_metadata_and_routing_modes_are_part_of_dedupe_envelope(self) -> None:
        common = dict(
            operation="text_send",
            platform="feishu",
            destination_kind="chat",
            destination_id="oc_metadata",
            payload_type="text",
            payload="same",
        )
        first = self.transport.record(**common, metadata={"route": "a"})
        second = self.transport.record(**common, metadata={"route": "b"})
        self.assertNotEqual(first.dedupe_key, second.dedupe_key)
        self.assertEqual(len(self.transport.read_all()), 2)

    def test_caller_dedupe_key_reuse_with_changed_envelope_fails_closed(self) -> None:
        common = dict(
            operation="text_send",
            platform="feishu",
            destination_kind="chat",
            destination_id="oc_caller_key",
            payload_type="text",
            caller_dedupe_key="stable-call-1",
        )
        self.transport.record(**common, payload="first")
        with self.assertRaisesRegex(RecordOnlyError, "reused"):
            self.transport.record(**common, payload="changed")
        self.assertEqual(len(self.transport.read_all()), 1)

    def test_hardlinked_ledger_and_lock_are_refused(self) -> None:
        self.transport.record(
            operation="text_send",
            platform="feishu",
            destination_kind="chat",
            destination_id="oc_link",
            payload_type="text",
            payload="first",
        )
        ledger_alias = self.root / "ledger-alias"
        os.link(self.transport.ledger, ledger_alias)
        with self.assertRaisesRegex(RecordOnlyError, "link check"):
            self.transport.read_all()
        ledger_alias.unlink()

        lock_alias = self.root / "lock-alias"
        os.link(self.transport.lock, lock_alias)
        with self.assertRaisesRegex(RecordOnlyError, "lock ownership"):
            self.transport.record(
                operation="text_send",
                platform="feishu",
                destination_kind="chat",
                destination_id="oc_link_2",
                payload_type="text",
                payload="second",
            )

    def test_replaced_record_root_fails_closed(self) -> None:
        moved = self.root / "records-moved"
        self.records.rename(moved)
        self.records.mkdir(mode=0o700)
        with self.assertRaisesRegex(RecordOnlyError, "replaced"):
            self.transport.record(
                operation="text_send",
                platform="feishu",
                destination_kind="chat",
                destination_id="oc_replaced",
                payload_type="text",
                payload="blocked",
            )

    def test_relay_sender_text_and_card_shapes(self) -> None:
        sender = RecordOnlyRelaySender(self.transport)
        text = json.loads(
            sender.send(
                {
                    "action": "send",
                    "target": "feishu:oc_relay:om_topic",
                    "message": "done",
                    "task_id": "task-002",
                }
            )
        )
        self.assertTrue(text["success"])
        created = sender.send_task_card("feishu:oc_relay:om_topic", {"title": "card"})
        updated = sender.send_task_card(
            "feishu:oc_relay:om_topic", {"title": "card2"}, message_id="om_existing"
        )
        self.assertFalse(created["updated"])
        self.assertFalse(updated["updated"])
        self.assertTrue(updated["simulated_update_recorded"])
        self.assertNotEqual(updated["message_id"], "om_existing")
        self.assertTrue(updated["provisional_target_only"])
        self.assertFalse(updated["production_ready"])
        self.assertFalse(updated["external_delivery_verified"])
        self.assertEqual(len(self.transport.read_all()), 3)

    def test_gateway_adapter_send_reply_and_update(self) -> None:
        adapter = GatewayRecordAdapter(self.transport)
        sent = asyncio.run(adapter.send("oc_gateway", "hello"))
        replied = asyncio.run(
            adapter.send(
                "oc_gateway",
                "reply",
                metadata={"thread_id": "om_thread", "reply_to_message_id": "om_source"},
            )
        )
        updated = asyncio.run(adapter.edit_message("oc_gateway", "om_sent", "edited"))
        self.assertTrue(sent.success and replied.success and updated.success)
        self.assertTrue(sent.provisional_target_only)
        self.assertFalse(sent.production_ready)
        self.assertFalse(sent.external_delivery_verified)
        self.assertNotEqual(updated.message_id, "om_sent")
        operations = [row["operation"] for row in self.transport.read_all()]
        self.assertEqual(operations, ["text_send", "text_reply", "text_update"])

    def test_unsupported_operation_and_malformed_target_fail(self) -> None:
        with self.assertRaises(RecordOnlyError):
            self.transport.record(
                operation="network_send",
                platform="feishu",
                destination_kind="chat",
                destination_id="oc_a",
                payload_type="text",
                payload="hello",
            )
        sender = RecordOnlyRelaySender(self.transport)
        with self.assertRaises(RecordOnlyError):
            sender.send({"action": "send", "target": "feishu", "message": "hello"})

    def test_contradictory_routing_and_empty_dedupe_claims_fail(self) -> None:
        with self.assertRaisesRegex(RecordOnlyError, "routing claims"):
            self.transport.record(
                operation="text_reply",
                platform="feishu",
                destination_kind="chat",
                destination_id="oc_bad_route",
                payload_type="text",
                payload="hello",
            )
        with self.assertRaisesRegex(RecordOnlyError, "must not be empty"):
            self.transport.record(
                operation="text_send",
                platform="feishu",
                destination_kind="chat",
                destination_id="oc_bad_dedupe",
                payload_type="text",
                payload="hello",
                caller_dedupe_key="",
            )
        self.assertFalse(self.transport.ledger.exists())

    def test_symlink_and_forbidden_roots_are_refused(self) -> None:
        real = self.root / "real"
        real.mkdir(mode=0o700)
        link = self.root / "link"
        link.symlink_to(real, target_is_directory=True)
        with self.assertRaises(RecordOnlyError):
            RecordOnlyOutboundTransport(
                link, id_hash_key=KEY, source_component="test.gateway"
            )
        forbidden = self.root / "forbidden"
        forbidden.mkdir(mode=0o700)
        child = forbidden / "child"
        with self.assertRaises(RecordOnlyError):
            RecordOnlyOutboundTransport(
                child,
                id_hash_key=KEY,
                source_component="test.gateway",
                forbidden_roots=(forbidden,),
            )

    def test_account_home_forbidden_roots_survive_fake_home_and_custom_roots(self) -> None:
        account_home = self.root / "account-home"
        live_like = account_home / ".hermes"
        live_like.mkdir(parents=True, mode=0o700)
        fake_home = self.root / "candidate-home"
        fake_home.mkdir(mode=0o700)
        custom = self.root / "custom-forbidden"
        custom.mkdir(mode=0o700)
        with patch.dict(os.environ, {"HOME": str(fake_home)}), patch(
            "gateway.record_only.transport.pwd.getpwuid",
            return_value=SimpleNamespace(pw_dir=str(account_home)),
        ):
            with self.assertRaisesRegex(RecordOnlyError, "forbidden root"):
                RecordOnlyOutboundTransport(
                    live_like / "records",
                    id_hash_key=KEY,
                    source_component="test.gateway",
                    forbidden_roots=(custom,),
                )

    def test_safety_status_is_explicit_machine_readable_no_go(self) -> None:
        status = self.transport.safety_status()
        status_file = json.loads(
            (
                Path(__file__).resolve().parents[3]
                / "gateway"
                / "record_only"
                / "PROTOTYPE_STATUS.json"
            ).read_text()
        )
        self.assertEqual(status, status_file)
        self.assertTrue(status["provisional_target_only"])
        self.assertFalse(status["production_ready"])
        self.assertFalse(status["promotion_authorized"])
        self.assertFalse(status["candidate_execution_authorized"])
        self.assertFalse(status["cutover_authorized"])
        self.assertFalse(status["external_delivery_attempted"])
        self.assertFalse(status["external_delivery_verified"])
        self.assertFalse(status["caller_claims_verified"])
        self.assertEqual(status["success_scope"], "record_persisted_not_delivered")
        self.assertFalse(status["record_only_coverage_complete"])
        self.assertEqual(
            status["external_outbound_census"]["index"]["sha256"],
            "b6bcfb3a597da616bec2acc8e57eea18695b0bb20e29446926cf2eb2e3f81914",
        )
        self.assertEqual(
            status["external_outbound_census"]["canonical_artifact"]["artifact"],
            "census-v4.json",
        )
        self.assertEqual(
            status["external_outbound_census"]["canonical_artifact"]["sha256"],
            "d2c17c7b03642074d301259437f17cc879e8adfbd91d07029c2dda775a563e63",
        )
        self.assertEqual(
            status["external_outbound_census"]["status"],
            "PROVISIONAL_STATIC_OUTBOUND_CENSUS_NO_GO",
        )
        self.assertFalse(
            status["external_outbound_census"]["record_only_coverage_complete"]
        )
        self.assertEqual(status["external_outbound_census"]["total_rows"], 6338)
        self.assertEqual(status["external_outbound_census"]["runtime_rows"], 3612)
        self.assertEqual(status["external_outbound_census"]["pending_rows"], 6338)
        self.assertEqual(
            status["external_outbound_census"]["unclassified_executable_mode_count"],
            5,
        )
        self.assertFalse(
            status["external_outbound_census"]["runtime_egress_trace_complete"]
        )
        self.assertFalse(
            status["external_outbound_census"]["dynamic_import_trace_complete"]
        )
        self.assertFalse(status["external_outbound_census"]["skill_trace_complete"])
        self.assertFalse(
            status["external_outbound_census"]["subprocess_descendant_trace_complete"]
        )
        self.assertEqual(
            set(status["blockers"]),
            {
                "external_outbound_census_not_verified",
                "unclassified_executable_modes_not_resolved",
                "runtime_egress_trace_not_complete",
                "dynamic_import_trace_not_complete",
                "skill_trace_not_complete",
                "subprocess_descendant_trace_not_complete",
                "candidate_integration_not_verified",
                "deny_network_containment_not_verified",
                "credential_stripping_not_verified",
                "trusted_record_key_provisioning_not_verified",
                "durable_external_ledger_anchor_not_implemented",
                "record_root_filesystem_semantics_not_attested",
                "trusted_clock_not_integrated",
            },
        )


if __name__ == "__main__":
    unittest.main()
