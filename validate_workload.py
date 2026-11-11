"""Drill: run the pinned 1,000-event workload through two independent consumers."""
import argparse
from decimal import Decimal, ROUND_HALF_UP
import hashlib
import json
from pathlib import Path
import tempfile

from order_middleware import OrderStore, DeliveryFailure
from order_export import WORKLOAD_SHA

def require(condition, message):
    if not condition:
        raise RuntimeError(message)

def oracle_total(order):
    # Use Decimal rounding independently of the integer arithmetic in the message handler.
    return sum(int((Decimal(line["unit_price_minor"]) * line["quantity"] *
               (Decimal(1) - Decimal(line["discount_bps"])/10000)).quantize(Decimal(1),rounding=ROUND_HALF_UP))
               for line in order["lines"])

def drill(path, db):
    with path.open('rb') as f:
        source=f.read(2*1024*1024+1)
    require(hashlib.sha256(source).hexdigest()==WORKLOAD_SHA, "Expected the reviewed Northwind workload")
    raw=source.splitlines()
    orders=list(map(json.loads,raw))
    expected_audit={(o["event_id"],o["order_id"],o["version"],oracle_total(o)) for o in orders}
    current={}
    for o in orders:
        if o["version"]>current.get(o["order_id"],(0,))[0]:
            current[o["order_id"]]=(o["version"],o["event_id"],oracle_total(o))
    # Deliver synthetic amendments ahead of their original versions to exercise the monotonic guard.
    delivery_order=list(range(830,1000))+list(range(830))
    repair_index=22
    bad=json.loads(raw[repair_index]);bad['lines'][0]['quantity']=-1
    with OrderStore(db) as store:
        rejected=store.ingest(json.dumps(bad).encode())
        require(rejected["status"]=="quarantined", "Malformed order entered topic")
        for i in delivery_order:
            if i!=repair_index:
                require(store.ingest(raw[i])["status"]=="accepted","First submission was not accepted")
        require(store.repair(rejected["quarantine_id"],raw[repair_index],"Restore reviewed source quantity")["status"]=="repaired",
                "Controlled ingress repair failed")
        for item in raw[:10]:
            require(store.ingest(item)["status"]=="duplicate","Duplicate ingress race")
        require(store.relay(limit=1100)==1000 and store.relay()==0,"Transactional fanout mismatch")
        for o in orders[:10]:
            for sub in ("audit","fulfillment"):
                store.redeliver(sub,o["event_id"])
        def unavailable(_):
            raise DeliveryFailure("simulated_outage")
        require(store.consume("fulfillment",limit=1,handler=unavailable,max_attempts=2)["retried"]==1,"Retry was lost")
        audited=store.consume("audit",limit=2000)
        during=store.summary()
        require(audited["applied"]==1000 and audited["duplicates"]==10 and during["fulfillment_orders"]==0,
                "Consumer isolation failed")
    with OrderStore(db) as store:
        require(store.consume("fulfillment",limit=1,handler=unavailable,max_attempts=2)["dead_lettered"]==1,
                "Expected controlled dead letter")
        failed=orders[delivery_order[0]]
        require(store.redrive("fulfillment",failed["event_id"],"Downstream restored after test outage")["status"]=="redriven",
                "Expected single approved redrive")
        delivered=store.consume("fulfillment",limit=2000)
        summary=store.summary()
        audit=set(map(tuple,store.db.execute("SELECT event_id,order_id,version,total_minor FROM audit_log")))
        actual={row[0]:tuple(row[1:]) for row in store.db.execute(
            "SELECT order_id,version,event_id,total_minor FROM fulfillment_state")}
        require(actual==current and audit==expected_audit,"Reconciliation disagrees with independent decimal oracle")
        require(delivered["duplicates"]==10 and summary["receipts"]=={"audit":1000,"fulfillment":1000},
                "Duplicate subscription messages changed business effects")
        for event in raw:
            require(store.ingest(event)["status"]=="duplicate","Replay changed the accepted stream")
        require(store.relay()==0 and store.summary()==summary,"Replay changed committed state")
        require(not store.db.execute("PRAGMA foreign_key_check").fetchall() and
                store.db.execute("PRAGMA integrity_check").fetchone()[0]=="ok","Database consistency failure")
    return {"input":"Northwind sample with 170 synthetic amendments, out-of-order delivery, injected defects and outage",
        "workload_sha256":WORKLOAD_SHA,"logical_submissions":len(raw),"extra_ingress_retries":10,
        "extra_deliveries_per_subscription":10,"audit_events_while_fulfillment_unavailable":during["audit_effects"],
        "fulfillment_orders_during_outage":during["fulfillment_orders"],"reconciled_audit_events":len(audit),
        "reconciled_current_orders":len(current),"current_order_total_minor":sum(v[2] for v in current.values()),
        "out_of_order_stale_fulfillment_events":delivered["stale"],"dlq_redrives":1,
        "malformed_ingress_repairs":summary["ingress_repairs"],"replayed_after_reopen":1000,
        "unchanged_after_exact_replay":True,"foreign_key_violations":0,"sqlite_integrity":"ok","azure_deployed":False}

if __name__=="__main__":
    p=argparse.ArgumentParser(description=__doc__);p.add_argument("jsonl",type=Path)
    a=p.parse_args()
    with tempfile.TemporaryDirectory() as temp:
        print(json.dumps(drill(a.jsonl,Path(temp)/"orders.sqlite"),indent=2))
