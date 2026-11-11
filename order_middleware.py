"""Durable local topic, two consumer inboxes and versioned order effects."""
import argparse
from contextlib import contextmanager
from datetime import date
import hashlib
import json
from pathlib import Path
import re
import sqlite3

SUBSCRIPTIONS = ("fulfillment", "audit")
MAX_BODY = 32_768
HEX = re.compile(r"[a-f0-9]{64}")
SQL = """
CREATE TABLE IF NOT EXISTS raw_messages(
 raw_sha TEXT PRIMARY KEY, raw_bytes BLOB NOT NULL, original_bytes INTEGER NOT NULL,
 captured_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP);
CREATE TABLE IF NOT EXISTS events(
 event_id TEXT PRIMARY KEY, order_id INTEGER NOT NULL, version INTEGER NOT NULL,
 total_minor INTEGER NOT NULL, payload BLOB NOT NULL, payload_sha TEXT NOT NULL,
 raw_sha TEXT NOT NULL REFERENCES raw_messages, UNIQUE(order_id,version));
CREATE TABLE IF NOT EXISTS quarantine(
 quarantine_id INTEGER PRIMARY KEY, raw_sha TEXT NOT NULL REFERENCES raw_messages,
 reason TEXT NOT NULL, created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP, UNIQUE(raw_sha,reason));
CREATE TABLE IF NOT EXISTS ingress_repairs(
 quarantine_id INTEGER PRIMARY KEY REFERENCES quarantine, event_id TEXT NOT NULL REFERENCES events,
 reason TEXT NOT NULL, repaired_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP);
CREATE TABLE IF NOT EXISTS outbox(
 seq INTEGER PRIMARY KEY, event_id TEXT NOT NULL UNIQUE REFERENCES events, relayed INTEGER NOT NULL DEFAULT 0);
CREATE INDEX IF NOT EXISTS pending_relay ON outbox(seq) WHERE relayed=0;
CREATE TABLE IF NOT EXISTS deliveries(
 delivery_id INTEGER PRIMARY KEY, subscription TEXT NOT NULL CHECK(subscription IN ('audit','fulfillment')),
 event_id TEXT NOT NULL REFERENCES events, state TEXT NOT NULL DEFAULT 'ready',
 attempts INTEGER NOT NULL DEFAULT 0, last_error TEXT);
CREATE INDEX IF NOT EXISTS ready_deliveries ON deliveries(subscription,delivery_id) WHERE state='ready';
CREATE TABLE IF NOT EXISTS receipts(
 subscription TEXT NOT NULL, event_id TEXT NOT NULL REFERENCES events, outcome TEXT NOT NULL,
 applied_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY(subscription,event_id));
CREATE TABLE IF NOT EXISTS audit_log(
 event_id TEXT PRIMARY KEY REFERENCES events, order_id INTEGER NOT NULL, version INTEGER NOT NULL,
 total_minor INTEGER NOT NULL, payload_sha TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS fulfillment_state(
 order_id INTEGER PRIMARY KEY, version INTEGER NOT NULL, event_id TEXT NOT NULL REFERENCES events,
 status TEXT NOT NULL, total_minor INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS dead_letters(
 dead_id INTEGER PRIMARY KEY, subscription TEXT NOT NULL,event_id TEXT NOT NULL REFERENCES events,
 delivery_id INTEGER NOT NULL REFERENCES deliveries, error TEXT NOT NULL,
 opened_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP, resolution TEXT, reason TEXT,
 replacement_event TEXT REFERENCES events, resolved_at TEXT);
CREATE UNIQUE INDEX IF NOT EXISTS one_open_dead ON dead_letters(subscription,event_id) WHERE resolution IS NULL;
"""

class ContractError(ValueError):
    """Stable, non-sensitive classification for a rejected envelope."""

class DeliveryFailure(RuntimeError):
    """A simulated transient failure before a consumer commits its effects."""

def canonical_bytes(payload):
    return json.dumps(payload,sort_keys=True,separators=(",",":"),ensure_ascii=True,allow_nan=False).encode()

def payload_digest(payload):
    return hashlib.sha256(canonical_bytes(payload)).hexdigest()

def total_minor(payload):
    return sum((line["quantity"] * line["unit_price_minor"] * (10000-line["discount_bps"]) + 5000) // 10000
               for line in payload["lines"])

def require(condition, code):
    if not condition:
        raise ContractError(code)

def int_range(n, low, high):
    return type(n) is int and low <= n <= high

def unique_keys(pairs):
    result = {}
    for k,v in pairs:
        require(k not in result,"duplicate_json_key")
        result[k] = v
    return result

def decode_order(raw):
    require(type(raw) is bytes and 0 < len(raw) <= MAX_BODY,"invalid_body_size")
    try:
        obj = json.loads(raw.decode("utf-8"),object_pairs_hook=unique_keys)
    except ContractError:
        raise
    except (ValueError, RecursionError) as exc:
        raise ContractError("invalid_json") from exc
    require(isinstance(obj,dict) and set(obj)=={
        "schema_version","event_id","order_id","version","status","order_date","lines","source_sha256"
    },"invalid_order_fields")
    require(obj["schema_version"]=="order.v1","unsupported_schema")
    for key in ("event_id","source_sha256"):
        require(isinstance(obj[key],str) and HEX.fullmatch(obj[key]),"invalid_"+key)
    for key in ("order_id","version"):
        require(int_range(obj[key],1,2**31-1),"invalid_"+key)
    require(obj["status"]==("submitted" if obj["version"]==1 else "amended"),"invalid_status")
    try:
        order_date=date.fromisoformat(obj["order_date"])
    except (TypeError,ValueError) as exc:
        raise ContractError("invalid_order_date") from exc
    require(order_date.isoformat()==obj["order_date"],"invalid_order_date")
    lines=obj["lines"]
    require(isinstance(lines,list) and 1 <= len(lines) <= 256,"invalid_line_count")
    previous=0
    for line in lines:
        require(isinstance(line,dict) and set(line)=={"product_id","quantity","unit_price_minor","discount_bps"},
                "invalid_line_fields")
        require(int_range(line["product_id"],previous+1,2**31-1),"invalid_product_grain")
        require(int_range(line["quantity"],1,32767),"invalid_quantity")
        require(int_range(line["unit_price_minor"],0,2**31-1),"invalid_price")
        require(int_range(line["discount_bps"],0,10000),"invalid_discount")
        previous=line["product_id"]
    return obj

def sub_name(name):
    if name not in SUBSCRIPTIONS:
        raise ValueError("unknown subscription")
    return name

def bounded_limit(n):
    if not int_range(n,1,100000):
        raise ValueError("limit must be between 1 and 100000")
    return n

def operator_reason(value):
    if not isinstance(value,str) or not 1 <= len(value.strip()) <= 240 or not value.isprintable():
        raise ValueError("supply a printable operator reason of at most 240 characters")
    return value.strip()

class OrderStore:
    def __init__(self, db_path):
        self.db=sqlite3.connect(db_path,isolation_level=None,timeout=10)
        self.db.row_factory=sqlite3.Row
        try:
            self.db.execute("PRAGMA foreign_keys=ON")
            self.db.execute("PRAGMA journal_mode=WAL")
            self.db.execute("PRAGMA synchronous=FULL")
            version=self.db.execute("PRAGMA user_version").fetchone()[0]
            if version not in (0,1):
                raise ValueError("unsupported warehouse contract version")
            self.db.executescript("BEGIN IMMEDIATE;\n"+SQL+"\nPRAGMA user_version=1; COMMIT;")
        except BaseException:
            self.db.close()
            raise

    def __enter__(self):
        return self

    def __exit__(self,*_):
        self.db.close()

    @contextmanager
    def transaction(self):
        self.db.execute("BEGIN IMMEDIATE")
        try:
            yield
            self.db.commit()
        except BaseException:
            self.db.rollback()
            raise

    def _archive(self,raw):
        if type(raw) is not bytes:
            raise TypeError("ingest takes bytes")
        sha=hashlib.sha256(raw).hexdigest()
        self.db.execute("INSERT OR IGNORE INTO raw_messages(raw_sha,raw_bytes,original_bytes) VALUES(?,?,?)",
                        (sha,raw[:MAX_BODY],len(raw)))
        return sha

    def _quarantine(self,sha,reason):
        self.db.execute("INSERT OR IGNORE INTO quarantine(raw_sha,reason) VALUES(?,?)",(sha,reason))
        qid=self.db.execute("SELECT quarantine_id FROM quarantine WHERE raw_sha=? AND reason=?",(sha,reason)).fetchone()[0]
        return {"status":"quarantined","quarantine_id":qid,"reason":reason}

    def _record_oversize(self,sha,prefix,size):
        with self.transaction():
            self.db.execute("INSERT OR IGNORE INTO raw_messages(raw_sha,raw_bytes,original_bytes) VALUES(?,?,?)",
                            (sha,prefix,size))
            return self._quarantine(sha,"invalid_body_size")

    def _accept(self,obj,raw_sha):
        event_id,oid,version=obj["event_id"],obj["order_id"],obj["version"]
        payload=canonical_bytes(obj)
        prior=self.db.execute("SELECT payload FROM events WHERE event_id=?",(event_id,)).fetchone()
        if prior:
            require(prior["payload"]==payload,"conflicting_event_id")
            return "duplicate"
        require(not self.db.execute("SELECT 1 FROM events WHERE order_id=? AND version=?",(oid,version)).fetchone(),
                "conflicting_order_version")
        self.db.execute("INSERT INTO events VALUES(?,?,?,?,?,?,?)",
                        (event_id,oid,version,total_minor(obj),payload,hashlib.sha256(payload).hexdigest(),raw_sha))
        self.db.execute("INSERT INTO outbox(event_id) VALUES(?)",(event_id,))
        return "accepted"

    def ingest(self,raw):
        with self.transaction():
            sha=self._archive(raw)
            try:
                obj=decode_order(raw)
                result=self._accept(obj,sha)
            except ContractError as exc:
                return self._quarantine(sha,str(exc))
            return {"status":result,"event_id":obj["event_id"]}

    def repair(self,qid,replacement_bytes,reason):
        reason=operator_reason(reason)
        obj=decode_order(replacement_bytes)
        with self.transaction():
            if not self.db.execute("SELECT 1 FROM quarantine WHERE quarantine_id=?",(qid,)).fetchone():
                raise ValueError("unknown quarantine ID")
            previous=self.db.execute("SELECT e.payload FROM ingress_repairs r JOIN events e USING(event_id) WHERE quarantine_id=?",(qid,)).fetchone()
            if previous:
                require(previous[0]==canonical_bytes(obj),"different_repair_already_recorded")
                return {"status":"already_repaired","event_id":obj["event_id"]}
            self._accept(obj,self._archive(replacement_bytes))
            self.db.execute("INSERT INTO ingress_repairs(quarantine_id,event_id,reason) VALUES(?,?,?)",(qid,obj["event_id"],reason))
            return {"status":"repaired","event_id":obj["event_id"]}

    def relay(self,limit=1000):
        bounded_limit(limit)
        with self.transaction():
            rows=self.db.execute("SELECT seq,event_id FROM outbox WHERE relayed=0 ORDER BY seq LIMIT ?",(limit,)).fetchall()
            for row in rows:
                self.db.executemany("INSERT INTO deliveries(subscription,event_id) VALUES(?,?)",[(s,row["event_id"]) for s in SUBSCRIPTIONS])
                self.db.execute("UPDATE outbox SET relayed=1 WHERE seq=?",(row["seq"],))
            return len(rows)

    def redeliver(self,subscription,event_id,copies=1):
        sub_name(subscription)
        bounded_limit(copies)
        with self.transaction():
            if not self.db.execute("SELECT 1 FROM outbox WHERE event_id=? AND relayed=1",(event_id,)).fetchone():
                raise ValueError("event has not been relayed")
            self.db.executemany("INSERT INTO deliveries(subscription,event_id) VALUES(?,?)",[(subscription,event_id)]*copies)
        return copies

    def _apply(self,subscription,row,obj):
        event_id=row["event_id"]
        if subscription=="audit":
            self.db.execute("INSERT INTO audit_log VALUES(?,?,?,?,?)",
                            (event_id,row["order_id"],row["version"],row["total_minor"],row["payload_sha"]))
            return "applied"
        changed=self.db.execute("""INSERT INTO fulfillment_state VALUES(?,?,?,?,?)
            ON CONFLICT(order_id) DO UPDATE SET version=excluded.version,event_id=excluded.event_id,
            status=excluded.status,total_minor=excluded.total_minor WHERE excluded.version>fulfillment_state.version""",
            (row["order_id"],row["version"],event_id,obj["status"],row["total_minor"])).rowcount
        return "applied" if changed else "stale"

    def _consume_one(self,subscription,did,handler,max_attempts):
        with self.transaction():
            row=self.db.execute("""SELECT d.attempts,e.* FROM deliveries d JOIN events e USING(event_id)
                WHERE d.delivery_id=? AND d.state='ready' AND d.subscription=?""",(did,subscription)).fetchone()
            if row is None: # Another worker already handled the selected delivery.
                return None
            event_id=row["event_id"]
            attempts=row["attempts"]+1
            seen=self.db.execute("SELECT 1 FROM receipts WHERE subscription=? AND event_id=?",(subscription,event_id)).fetchone()
            outcome="duplicates"
            if not seen:
                obj=json.loads(row["payload"])
                try:
                    if handler:
                        handler(obj.copy())
                except DeliveryFailure:
                    # Keep exception text out of durable telemetry; retry the whole business action.
                    status="dead" if attempts>=max_attempts else "ready"
                    self.db.execute("UPDATE deliveries SET state=?,attempts=?,last_error='delivery_failure' WHERE delivery_id=?",
                                    (status,attempts,did))
                    if status=="ready":
                        return "retried"
                    self.db.execute("""INSERT OR IGNORE INTO dead_letters(subscription,event_id,delivery_id,error)
                        VALUES(?,?,?,'delivery_failure')""",(subscription,event_id,did))
                    return "dead_lettered"
                outcome=self._apply(subscription,row,obj)
                self.db.execute("INSERT INTO receipts(subscription,event_id,outcome) VALUES(?,?,?)",(subscription,event_id,outcome))
                self.db.execute("""UPDATE dead_letters SET resolution='satisfied',resolved_at=CURRENT_TIMESTAMP
                    WHERE subscription=? AND event_id=? AND resolution IS NULL""",(subscription,event_id))
            self.db.execute("UPDATE deliveries SET state='done',attempts=?,last_error=NULL WHERE delivery_id=?",(attempts,did))
            return outcome

    def consume(self,subscription,limit=1000,handler=None,max_attempts=3):
        sub_name(subscription)
        bounded_limit(limit)
        if not int_range(max_attempts,1,1000):
            raise ValueError("invalid attempt budget")
        ids=[r[0] for r in self.db.execute("""SELECT delivery_id FROM deliveries
            WHERE subscription=? AND state='ready' ORDER BY delivery_id LIMIT ?""",(subscription,limit))]
        counts=dict(applied=0,stale=0,duplicates=0,retried=0,dead_lettered=0)
        for did in ids:
            outcome=self._consume_one(subscription,did,handler,max_attempts)
            if outcome:
                counts[outcome]+=1
        return counts

    def redrive(self,subscription,event_id,reason):
        sub_name(subscription)
        reason=operator_reason(reason)
        with self.transaction():
            dead=self.db.execute("""SELECT dead_id,resolution FROM dead_letters WHERE subscription=? AND event_id=?
                ORDER BY dead_id DESC LIMIT 1""",(subscription,event_id)).fetchone()
            if not dead:
                raise ValueError("unknown dead letter")
            if dead["resolution"] is not None:
                return {"status":"already_resolved","resolution":dead["resolution"]}
            self.db.execute("""UPDATE dead_letters SET resolution='redriven',reason=?,resolved_at=CURRENT_TIMESTAMP
                WHERE dead_id=?""",(reason,dead["dead_id"]))
            did=self.db.execute("INSERT INTO deliveries(subscription,event_id) VALUES(?,?)",(subscription,event_id)).lastrowid
            return {"status":"redriven","delivery_id":did}

    def repair_dead_letter(self,subscription,event_id,replacement_bytes,reason):
        sub_name(subscription)
        reason=operator_reason(reason)
        obj=decode_order(replacement_bytes)
        with self.transaction():
            dead=self.db.execute("""SELECT d.*,e.order_id,e.version FROM dead_letters d JOIN events e USING(event_id)
                WHERE d.subscription=? AND d.event_id=? ORDER BY dead_id DESC LIMIT 1""",(subscription,event_id)).fetchone()
            if not dead:
                raise ValueError("unknown dead letter")
            if dead["resolution"] is not None:
                require(dead["resolution"]=="superseded" and dead["replacement_event"]==obj["event_id"],
                        "dead_letter_already_resolved")
                previous=self.db.execute("SELECT payload FROM events WHERE event_id=?",(obj["event_id"],)).fetchone()
                require(previous[0]==canonical_bytes(obj),"different_repair_already_recorded")
                return {"status":"already_repaired","event_id":obj["event_id"]}
            require(obj["order_id"]==dead["order_id"] and obj["version"]>dead["version"] and obj["event_id"]!=event_id,
                    "replacement_requires_new_version_and_id_for_same_order")
            self._accept(obj,self._archive(replacement_bytes))
            self.db.execute("""UPDATE dead_letters SET resolution='superseded',replacement_event=?,reason=?,resolved_at=CURRENT_TIMESTAMP
                WHERE dead_id=?""",(obj["event_id"],reason,dead["dead_id"]))
            self.db.execute("INSERT INTO receipts(subscription,event_id,outcome) VALUES(?,?,'superseded')",(subscription,event_id))
            return {"status":"repaired","event_id":obj["event_id"]}

    def summary(self):
        with self.transaction():
            count=lambda query,*args: self.db.execute(query,args).fetchone()[0]
            return dict(events=count("SELECT count(*) FROM events"),outbox_pending=count("SELECT count(*) FROM outbox WHERE relayed=0"),
                quarantined=count("SELECT count(*) FROM quarantine"),ingress_repairs=count("SELECT count(*) FROM ingress_repairs"),
                audit_effects=count("SELECT count(*) FROM audit_log"),fulfillment_orders=count("SELECT count(*) FROM fulfillment_state"),
                receipts={s:count("SELECT count(*) FROM receipts WHERE subscription=?",s) for s in SUBSCRIPTIONS},
                ready={s:count("SELECT count(*) FROM deliveries WHERE subscription=? AND state='ready'",s) for s in SUBSCRIPTIONS},
                dead_open={s:count("SELECT count(*) FROM dead_letters WHERE subscription=? AND resolution IS NULL",s) for s in SUBSCRIPTIONS})

def publish_jsonl(store, source):
    """Consume LF-delimited bytes. Oversized records retain their full digest and bounded prefix."""
    counts=dict(accepted=0,duplicate=0,quarantined=0)
    while first:=source.readline(MAX_BODY+2):
        body=first.removesuffix(b"\n")
        if first.endswith(b"\n") or len(body)<=MAX_BODY:
            result=store.ingest(body)
        else:
            digest=hashlib.sha256(body)
            size=len(body)
            while remainder:=source.readline(MAX_BODY+2):
                fragment=remainder.removesuffix(b"\n")
                digest.update(fragment)
                size+=len(fragment)
                if remainder.endswith(b"\n"):
                    break
            result=store._record_oversize(digest.hexdigest(),body[:MAX_BODY],size)
        counts[result["status"]]+=1
    return counts

def replacement_bytes(path):
    with path.open("rb") as source:
        return source.read(MAX_BODY+1)

def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("database",type=Path)
    cmds=parser.add_subparsers(dest="cmd",required=True)
    publish=cmds.add_parser("publish")
    publish.add_argument("jsonl",type=Path)
    r=cmds.add_parser("relay")
    r.add_argument("--limit",type=int,default=1000)
    c=cmds.add_parser("consume")
    c.add_argument("subscription",choices=SUBSCRIPTIONS)
    c.add_argument("--limit",type=int,default=1000)
    c.add_argument("--max-attempts",type=int,default=3)
    repeat=cmds.add_parser("redeliver")
    repeat.add_argument("subscription",choices=SUBSCRIPTIONS)
    repeat.add_argument("event_id")
    repeat.add_argument("--copies",type=int,default=1)
    rd=cmds.add_parser("redrive")
    rd.add_argument("subscription",choices=SUBSCRIPTIONS)
    rd.add_argument("event_id")
    rd.add_argument("--reason",required=True)
    for name in ("repair","repair-dead-letter"):
        fix=cmds.add_parser(name)
        if name=="repair":
            fix.add_argument("quarantine_id",type=int)
        else:
            fix.add_argument("subscription",choices=SUBSCRIPTIONS)
            fix.add_argument("event_id")
        fix.add_argument("replacement_json",type=Path)
        fix.add_argument("--reason",required=True)
    cmds.add_parser("summary")
    args=parser.parse_args()
    with OrderStore(args.database) as store:
        if args.cmd=="publish":
            with args.jsonl.open("rb") as f:
                result=publish_jsonl(store,f)
        elif args.cmd=="summary": result=store.summary()
        elif args.cmd=="relay": result={"relayed":store.relay(args.limit)}
        elif args.cmd=="consume": result=store.consume(args.subscription,args.limit,max_attempts=args.max_attempts)
        elif args.cmd=="redeliver": result={"queued":store.redeliver(args.subscription,args.event_id,args.copies)}
        elif args.cmd=="redrive": result=store.redrive(args.subscription,args.event_id,args.reason)
        elif args.cmd=="repair": result=store.repair(args.quarantine_id,replacement_bytes(args.replacement_json),args.reason)
        else: result=store.repair_dead_letter(args.subscription,args.event_id,replacement_bytes(args.replacement_json),args.reason)
    print(json.dumps(result,sort_keys=True))

if __name__=="__main__":
    main()
