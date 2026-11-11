# Order messages and durable local processing

One message represents a full snapshot of one order version. Event identity and business version serve different purposes. The same event can be delivered many times. Fulfillment projects the highest version received for each order, while audit records each accepted version once regardless of arrival order.

## Wire format

Each UTF-8 JSON object contains exactly these fields. Unknown keys, repeated JSON keys, Booleans in integer fields and malformed calendar dates enter quarantine.

| Field | Meaning |
|---|---|
| `schema_version` | The string `order.v1`. |
| `event_id` | Lowercase 64-character hexadecimal text. The source generator creates this synthetic identity from its generator name, pinned source SHA, order ID and version. The middleware treats it as an opaque identity. |
| `source_sha256` | Lowercase SHA-256 of the source bytes. The source adapter checks those bytes. The middleware validates the field format. |
| `order_id`, `version` | Positive integers up to 2,147,483,647. Versions may arrive out of order or with gaps. |
| `status` | Version 1 has status `submitted`. Later versions have status `amended`. These are synthetic lifecycle labels. |
| `order_date` | Source calendar date in `YYYY-MM-DD` form. |
| `lines` | One to 256 lines, sorted in ascending order by distinct positive integer `product_id`. Each line contains only `product_id`, `quantity`, `unit_price_minor` and `discount_bps`. |

Quantity ranges from 1 to 32,767. Unit price ranges from 0 to 2,147,483,647 integer minor units. Discounts are integer basis points in the inclusive range 0–10,000. Integer arithmetic computes the discounted line amount as `(quantity * unit_price_minor * (10000 - discount_bps) + 5000) // 10000`. This rounds half up for nonnegative amounts. The order total sums the individually rounded lines. Freight, tax, an exchange rate and customer information are outside this message.

Canonical JSON sorts object keys, uses ASCII escapes and removes insignificant separator whitespace. The accepted payload digest describes these canonical bytes. The raw archive independently hashes the exact bytes supplied, so equivalent encodings retain distinct provenance. The CLI treats LF as the record delimiter. A carriage return preceding LF counts as input whitespace.

## Transactions and database grain

SQLite uses WAL, `synchronous=FULL`, foreign keys and `BEGIN IMMEDIATE` for write transactions. There is one writer at a time. Independent subscriptions have separate queues and receipts.

| Table | Grain and purpose |
|---|---|
| `raw_messages` | One row per exact raw SHA. Retains a bounded prefix, original byte count and capture time. The body limit is 32 KiB. Oversized inputs retain their complete input digest and a 32 KiB prefix. |
| `events` | One canonical payload per event ID, additionally unique on `(order_id, version)`, with order total, canonical digest and first accepted raw digest. |
| `quarantine`, `ingress_repairs` | One quarantine entry per `(raw_sha, reason)` and at most one accepted repair for that entry. The old bytes and the operator reason remain available. |
| `outbox` | One publication intent per accepted event. A partial index covers entries awaiting relay. |
| `deliveries` | One delivery attempt stream per queue copy, with subscription, event ID, state, attempts and error class. Redelivery creates an extra row. A partial index selects ready copies. |
| `receipts` | One terminal business outcome per `(subscription, event_id)`. Outcomes include applied, stale and explicitly superseded. |
| `audit_log` | One business audit row per processed event ID. Every unique processed version is visible. |
| `fulfillment_state` | One current snapshot per order. Only a strictly greater incoming version replaces it. |
| `dead_letters` | Failed delivery, first failure class, operator resolution, reason, replacement identity and timestamps. A partial unique index allows one open case per subscription and event. |

1. **Ingest.** Persist raw bytes, validate identity and business-version uniqueness, then commit the canonical event and its outbox intent together. An identical canonical replay returns `duplicate`. A reused event ID with different content or a second event ID for an existing order/version enters quarantine.
2. **Relay.** Insert one copy for each subscription and mark the outbox record relayed within the same transaction. Failure during fanout rolls back both copies.
3. **Consume.** Select a bounded set of ready delivery IDs, then claim and process each under its own transaction. Apply the effect, store the receipt and settle that copy together. An existing receipt skips the handler. A lower fulfillment version records a stale receipt while the higher snapshot remains current. Concurrent workers recheck ready state under the write lock.
4. **Fail and recover.** A controlled `DeliveryFailure` increments the attempt counter. The default budget is three attempts. Each consume invocation attempts a given selected copy at most once. After exhaustion the copy becomes dead. Unexpected failures roll back that delivery. A new invocation can resume after reopening the database.

The local effect consists entirely of database writes. The optional Python handler must perform only the deterministic pre-effect check used in the failure drills. External shipping, billing or message sends would require an independently idempotent downstream API or another transactional outbox.

## Commands

Run from the repository root. Generate the workload with the source adapter first.

```bash
python3 order_middleware.py data/orders.sqlite publish data/submissions.jsonl
python3 order_middleware.py data/orders.sqlite relay
python3 order_middleware.py data/orders.sqlite consume audit
python3 order_middleware.py data/orders.sqlite consume fulfillment
python3 order_middleware.py data/orders.sqlite summary
```

Use `--limit` for a bounded relay or consume pass, and repeat until the pending count reaches zero. Replaying the same source file returns duplicate classifications and creates no new publication intent.

After an operator resolves a transient dependency failure, `redrive SUBSCRIPTION EVENT_ID --reason TEXT` creates a new queue copy and retains the old dead-letter history. An immediate identical redrive returns the earlier resolution. A new budget starts when the redriven copy is consumed.

For a malformed ingress payload, `repair QUARANTINE_ID REPLACEMENT_JSON --reason TEXT` validates the new file and links an accepted event to the original quarantine entry. Relay and consume it normally. For an accepted payload whose business action failed permanently, `repair-dead-letter SUBSCRIPTION EVENT_ID REPLACEMENT_JSON --reason TEXT` requires the same order, a greater version and a new event ID. It suppresses future copies of the superseded event for that subscription and sends the correction through the normal outbox to both subscriptions. Audit history and other subscribers' receipts are preserved.

Backup and restore the database with SQLite's backup API or a coordinated checkpoint. Copying a live database file alone can omit committed WAL changes. Recreating the database by replaying source messages reconstructs business state, while recovery of operator decisions and historical failure attempts requires a database backup. Retention is unbounded for this 1,000-message lab.
