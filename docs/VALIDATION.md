# Observed local validation, 11 November 2026

The test population uses the Microsoft Northwind sample and 170 synthetic order amendments. It covers the local integration model and Azure SDK adapter behavior on loopback. Azure resources remain unprovisioned.

| Check | Observed result |
|---|---|
| Source access and replay | Bounded HTTPS download of the pinned 1,049,720-byte script. The source adapter extracted 830 orders and 2,155 details and generated 1,000 distinct event IDs and order/version pairs. The retained source reproduced the JSONL byte for byte. [Source receipt](../examples/source-smoke.json). |
| Source and middleware tests | 32 deterministic tests passed, including 8 source, 15 middleware, 7 cloud adapter and 2 SQL projection tests. Scenarios cover failures between effects and receipts, conflicting IDs and versions, concurrent workers, invalid payloads, redrive and repair. |
| Independent consumers | With fulfillment interrupted, audit committed 1,000 entries and fulfillment had zero current orders. Both consumers subsequently suppressed ten extra deliveries each. Ten additional duplicate ingress submissions also produced no extra outbox events. |
| Recovery | One invalid quantity was quarantined and repaired. One failed fulfillment delivery reached the dead-letter queue, then succeeded after recorded redrive and database reopen. After replaying all 1,000 original events the state and pending counts matched. |
| Reconciliation | All 1,000 audit records and 830 latest fulfillment orders matched a separate Decimal calculation over the input workload. A total of 169 late-arriving older versions received stale fulfillment receipts. SQLite reported zero foreign-key violations and integrity ok. [Drill output](../examples/middleware-validation.json). |
| Local Storage emulator | Three blobs created, three identical blobs reused, three payloads read with pinned ETags. Ten extra deliveries per subscriber left six effective receipts, three audit entries and two current order states. A failed multi-action table transaction rolled back and a stale ETag was rejected. [Azurite results](../examples/storage-emulator.json). |
| Python packaging | A clean archive layout imported both Function definitions using the pinned SDKs. Binding discovery found an Event Grid trigger and a POST apply route. A JSON ServiceBusMessage and an Int64 Table property were constructed locally. |
| SQL | Four operational queries ran against a drained 1,000-submission database. All expected receipts were present. [Observed SQL results](../examples/sql-observations.json). |
| Infrastructure | Bicep 0.46.1 compiled the entry template, its three conditional modules and dev parameters. The explicit prod parameters failed the dev-only guard. The static validator checked both disabled consumers and conditional settlement. [Compilation output](../examples/infra-validation.json). |

Run the commands in the README and adapter guide to reproduce each local check. The local scripted outage, source digest, generated SQL projection, and emulator cannot establish real distributed delivery behavior. Cloud acceptance still needs an approved Azure SQL data load, container image build and execution, IAM and network denial tests, Event Grid retry/reconciliation, native broker locking and DLQ redrive, Table effects, monitoring, measured performance and cleanup.
