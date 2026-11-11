import hashlib
import json
from pathlib import Path
import io
import sqlite3
import tempfile
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor

from order_middleware import (
    ContractError, DeliveryFailure, MAX_BODY, OrderStore, canonical_bytes, decode_order, total_minor, publish_jsonl,
)


def event(order=10248, version=1, *, label=None):
    identity=(label or f"test-{order}-{version}").encode()
    return {
        "schema_version":"order.v1","event_id":hashlib.sha256(identity).hexdigest(),
        "order_id":order,"version":version,"status":"submitted" if version==1 else "amended",
        "order_date":"1997-10-14","source_sha256":"a"*64,
        "lines":[{"product_id":1,"quantity":1,"unit_price_minor":5,"discount_bps":1000}],
    }


def fail(_):
    raise DeliveryFailure("synthetic outage")


class ContractTests(unittest.TestCase):
    def test_half_up_per_line_then_sum(self):
        p=event()
        p["lines"].append({**p["lines"][0],"product_id":2})
        # 4.5 minor units rounds to 5 on each line.
        self.assertEqual(total_minor(decode_order(canonical_bytes(p))),10)

    def test_strict_json_and_types(self):
        p=event()
        raw=canonical_bytes(p)
        with self.assertRaisesRegex(ContractError,"duplicate_json_key"):
            decode_order(raw.replace(b'{"event_id"',b'{"order_id":100,"event_id"'))
        for key,value in [("version",True),("order_date","2026-02-30"),("event_id","nonhex"),("schema_version","order.v2")]:
            with self.subTest(key=key),self.assertRaises(ContractError):
                decode_order(canonical_bytes({**p,key:value}))
        with self.assertRaises(ContractError):
            decode_order(canonical_bytes({**p,"lines":p["lines"]*2}))

    def test_price_and_discount_domain(self):
        p=event()
        for key,value in (("unit_price_minor",1.5),("discount_bps",10001),("quantity",0)):
            with self.subTest(key=key),self.assertRaises(ContractError):
                decode_order(canonical_bytes({**p,"lines":[{**p["lines"][0],key:value}]}))


class StoreTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path=Path(self.temp.name)/"orders.sqlite"
        self.store=OrderStore(self.path)
        self.addCleanup(lambda:self.store.db.close())

    def publish(self, *payloads):
        for p in payloads:
            self.assertEqual(self.store.ingest(canonical_bytes(p))["status"],"accepted")
        self.assertEqual(self.store.relay(),len(payloads))

    def test_equivalent_encodings_and_conflicting_identity(self):
        p=event()
        self.assertEqual(self.store.ingest(canonical_bytes(p))["status"],"accepted")
        self.assertEqual(self.store.ingest(json.dumps(p,indent=2).encode())["status"],"duplicate")
        conflicting={**p,"order_date":"2003-03-04"}
        q=self.store.ingest(canonical_bytes(conflicting))
        self.assertEqual(q["reason"],"conflicting_event_id")
        self.assertEqual(q,self.store.ingest(canonical_bytes(conflicting)))
        q2=self.store.ingest(canonical_bytes(event(label="other-id")))
        self.assertEqual(q2["reason"],"conflicting_order_version")
        summary=self.store.summary()
        self.assertEqual((summary["events"],summary["outbox_pending"],summary["quarantined"]),(1,1,2))
        self.assertEqual(self.store.db.execute("SELECT count(*) FROM raw_messages").fetchone()[0],4)

    def test_ten_redeliveries_per_subscription_one_effect(self):
        p=event()
        self.publish(p)
        for sub in ("audit","fulfillment"):
            self.store.redeliver(sub,p["event_id"],10)
            results=self.store.consume(sub)
            self.assertEqual((results["applied"],results["duplicates"]),(1,10))
        summary=self.store.summary()
        self.assertEqual(summary["audit_effects"],1)
        self.assertEqual(summary["fulfillment_orders"],1)
        self.assertEqual(summary["receipts"],{"audit":1,"fulfillment":1})
        self.assertEqual(self.store.relay(),0)

    def test_late_version_audited_but_cannot_regress_fulfillment(self):
        self.publish(event(version=2),event())
        self.assertEqual(self.store.consume("audit")["applied"],2)
        results=self.store.consume("fulfillment")
        self.assertEqual((results["applied"],results["stale"]),(1,1))
        self.assertEqual(self.store.db.execute("SELECT version FROM fulfillment_state").fetchone()[0],2)

    def test_ingress_and_partial_fanout_failures_roll_back(self):
        self.store.db.execute("CREATE TRIGGER stop_outbox BEFORE INSERT ON outbox BEGIN SELECT RAISE(ABORT,'stop'); END")
        with self.assertRaises(sqlite3.IntegrityError):
            self.store.ingest(canonical_bytes(event()))
        self.assertEqual(self.store.summary()["events"],0)
        self.assertEqual(self.store.db.execute("SELECT count(*) FROM raw_messages").fetchone()[0],0)
        self.store.db.execute("DROP TRIGGER stop_outbox")
        self.store.ingest(canonical_bytes(event()))
        self.store.db.execute("""CREATE TRIGGER stop_second_subscription BEFORE INSERT ON deliveries
            WHEN NEW.subscription='audit' BEGIN SELECT RAISE(ABORT,'stop'); END""")
        with self.assertRaises(sqlite3.IntegrityError):
            self.store.relay()
        self.assertEqual(self.store.summary()["ready"],{"audit":0,"fulfillment":0})
        self.assertEqual(self.store.summary()["outbox_pending"],1)
        self.store.db.execute("DROP TRIGGER stop_second_subscription")
        self.assertEqual(self.store.relay(),1)

    def test_effect_and_receipt_fail_or_commit_together_after_reopen(self):
        self.publish(event())
        self.store.db.execute("CREATE TRIGGER fail_receipt BEFORE INSERT ON receipts BEGIN SELECT RAISE(ABORT,'stop'); END")
        with self.assertRaises(sqlite3.IntegrityError):
            self.store.consume("fulfillment")
        self.assertEqual(self.store.summary()["fulfillment_orders"],0)
        self.store.db.execute("DROP TRIGGER fail_receipt")
        self.store.db.close()
        self.store=OrderStore(self.path)
        self.assertEqual(self.store.consume("fulfillment")["applied"],1)
        self.assertEqual(self.store.summary()["ready"]["fulfillment"],0)

    def test_independent_outage_dlq_and_idempotent_redrive(self):
        p=event()
        self.publish(p)
        for _ in range(2):
            self.assertEqual(self.store.consume("fulfillment",handler=fail)["retried"],1)
        self.assertEqual(self.store.consume("audit")["applied"],1)
        self.assertEqual(self.store.consume("fulfillment",handler=fail)["dead_lettered"],1)
        self.assertEqual(self.store.summary()["dead_open"]["fulfillment"],1)
        for expected in ("redriven","already_resolved"):
            self.assertEqual(self.store.redrive("fulfillment",p["event_id"],"service restored")["status"],expected)
        self.assertEqual(self.store.consume("fulfillment")["applied"],1)
        self.assertEqual(self.store.summary()["receipts"],{"audit":1,"fulfillment":1})
        self.assertEqual(self.store.db.execute("SELECT reason FROM dead_letters").fetchone()[0],"service restored")

    def test_malformed_repair_and_replay_preserve_original(self):
        q=self.store.ingest(b"broken")
        bad=canonical_bytes({**event(),"schema_version":"bad"})
        with self.assertRaises(ContractError):
            self.store.repair(q["quarantine_id"],bad,"operator correction")
        good=canonical_bytes(event())
        self.assertEqual(self.store.repair(q["quarantine_id"],good,"repair source")["status"],"repaired")
        self.assertEqual(self.store.repair(q["quarantine_id"],good,"retry operator action")["status"],"already_repaired")
        self.assertEqual(self.store.summary()["ingress_repairs"],1)
        row=self.store.db.execute("SELECT raw_bytes FROM raw_messages JOIN quarantine USING(raw_sha)").fetchone()
        self.assertEqual(row[0],b"broken")

    def test_fail_and_repair_with_higher_version(self):
        old=event()
        new=event(version=2)
        self.publish(old)
        self.store.consume("fulfillment",handler=fail,max_attempts=1)
        with self.assertRaisesRegex(ContractError,"replacement_requires"):
            self.store.repair_dead_letter("fulfillment",old["event_id"],canonical_bytes(event(order=90)),"wrong order")
        reason="replace failed submission"
        for expected in ("repaired","already_repaired"):
            actual=self.store.repair_dead_letter("fulfillment",old["event_id"],canonical_bytes(new),reason)
            self.assertEqual(actual["status"],expected)
        self.store.relay()
        self.store.redeliver("fulfillment",old["event_id"])
        result=self.store.consume("fulfillment")
        self.assertEqual((result["applied"],result["duplicates"]),(1,1))
        self.assertEqual(self.store.consume("audit")["applied"],2)
        self.assertEqual(self.store.db.execute("SELECT version FROM fulfillment_state").fetchone()[0],2)

    def test_concurrent_senders_and_overlapping_consumers(self):
        p=event()
        gate=threading.Barrier(2)
        def send(_):
            with OrderStore(self.path) as worker:
                gate.wait(timeout=5)
                return worker.ingest(canonical_bytes(p))["status"]
        with ThreadPoolExecutor(max_workers=2) as pool:
            statuses=list(pool.map(send,(1,2)))
        self.assertCountEqual(statuses,["accepted","duplicate"])
        self.store.relay()
        self.store.redeliver("audit",p["event_id"],12)
        def process(_):
            with OrderStore(self.path) as worker:
                gate.wait(timeout=5)
                return worker.consume("audit")
        with ThreadPoolExecutor(max_workers=2) as pool:
            counts=list(pool.map(process,(1,2)))
        self.assertEqual(sum(n["applied"] for n in counts),1)
        self.assertEqual(sum(n["duplicates"] for n in counts),12)
        self.assertEqual(self.store.summary()["audit_effects"],1)

    def test_decoder_resource_limit_is_quarantined(self):
        result=self.store.ingest(b'{"order_id":'+b'9'*5000+b'}')
        self.assertEqual(result["status"],"quarantined")

    def test_jsonl_drains_oversized_line_and_continues(self):
        oversized=b"x"*(MAX_BODY*4+5)+b"\r"
        result=publish_jsonl(self.store,io.BytesIO(oversized+b"\n"+canonical_bytes(event())+b"\r\n"))
        self.assertEqual(result,dict(accepted=1,duplicate=0,quarantined=1))
        record=self.store.db.execute("SELECT raw_sha,length(raw_bytes),original_bytes FROM raw_messages JOIN quarantine USING(raw_sha)").fetchone()
        self.assertEqual(tuple(record),(hashlib.sha256(oversized).hexdigest(),MAX_BODY,len(oversized)))

    def test_oversize_evidence_bounded(self):
        self.assertEqual(self.store.ingest(b"x"*(MAX_BODY+1))["reason"],"invalid_body_size")
        row=self.store.db.execute("SELECT length(raw_bytes),original_bytes FROM raw_messages").fetchone()
        self.assertEqual(tuple(row),(MAX_BODY,MAX_BODY+1))
