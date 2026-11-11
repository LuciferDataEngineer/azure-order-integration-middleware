# Export, publish and settle an order

The export job has two stages. It reads a bounded, ordered projection from Azure SQL, then verifies all 1,000 canonical JSON bodies against the reviewed workload digest before creating the first blob. The SQL view contains order IDs, dates and item-level numerical fields. The historical input uses the Northwind sample schema. [Export view](../sql/export_view.sql).

The Azure SQL path uses an Entra access token from the export job's user-assigned identity with ODBC Driver 18, TLS certificate validation and a SELECT-only grant on the approved view. The manual job reads at most 2,156 rows to detect an extra row in the 2,155-line input. A changed database projection requires a newly reviewed source version. The sample SQL install script needs an administrator's separate review and adaptation to Azure SQL Database before a managed run.

The exporter creates one object at `orders/events/{event_id}.json`. Its metadata contains the canonical body SHA-256. A retry after a partial export checks the length, metadata and the actual ETag-conditional blob bytes of previously created objects. Reusing an identical blob avoids generating a second create notification. A blob batch completes per object. A 1,000-object export can be partially delivered during an interruption.

Event Grid invokes `publish_order` for each BlobCreated notification. The Function checks the storage account, exact subject and URL, reads from its configured container using the notification ETag, verifies metadata, validates the order schema, and compares its canonical bytes and event ID before sending to `order-events`. Separate notifications for one blob use the same Service Bus message ID. The broker duplicate window limits duplicate sends for a finite time. Application receipts persist at the consumers. Publisher failures are retried by Event Grid subject to its retention and retry configuration. For failed or expired notification delivery, operators must inventory the intended immutable blobs and explicitly resubmit the original identities.

Each Logic App calls its own apply endpoint. The HTTP routes depend on the declared platform authentication and verify the trusted principal ID against the expected workflow identity. Verify direct and cross-consumer access denials before enabling workflows. A successful apply returns 200 after committing or recognizing a receipt. Failures during database operations propagate so the workflow leaves its message unsettled. An invalid order or conflicting identity returns 422 or 409. The bounded broker delivery limit controls movement to the native DLQ.

The Table adapter stores one partition per consumer. Row keys hold the event receipt, order/version claim and either an audit row or the current order. One entity-group transaction writes all affected rows. Fulfillment uses an ETag on replacements and re-reads contested state. One partition per consumer supports identity checks across order IDs and is a deliberate throughput bound for a demonstration. The local replay and repair CLI operates on SQLite. A cloud operator must inspect and explicitly authorize native DLQ replay or a new, traceable corrected event. [Service Bus delivery and duplicates](https://learn.microsoft.com/en-us/azure/service-bus-messaging/service-bus-message-loss-and-duplicates) and [Table entity-group transactions](https://learn.microsoft.com/en-us/rest/api/storageservices/performing-entity-group-transactions).

## Credential-free checks

After fetching the pinned SQL source as shown in the README, prepare the exact cloud export messages locally:

```bash
python3 order_export.py --prepare-source data/raw/3cc62b3fca6d244a47dbde698b809331e4f85988a0685b2b370717d431e94871/instnwnd.sql
python3 package_function.py data/order-function.zip
```

The package contains the Function entry point, host configuration, pinned Python SDK requirements and two original application modules. Future Flex Consumption deployment requires a Python 3.12-compatible remote build. The Dockerfile prepares an optional export image. The source package and SDK imports were checked locally. The image, Azure SQL view, Event Grid, Service Bus, workflow settlement, IAM and cloud restore require managed-environment validation. [Deployment runbook](AZURE.md).

For the isolated local Blob and Table integration drill, use Node.js and Python in the repository root:

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -r azure/functions/requirements.txt
npm install --prefix data/emulator --no-audit --no-fund --ignore-scripts azurite@3.37.0
.venv/bin/python validate_storage.py --azurite data/emulator/node_modules/.bin/azurite
```

Azurite binds to loopback with temporary ports, local data and a random ephemeral account key. The SDK test uses three synthetic event bodies and captures the sender callback in memory. It exercises actual Storage SDK range reads, ETags, duplicate inserts, transactions and rollback against Azurite. Event Grid and Service Bus transport are simulated in this drill. [Observed emulator results](../examples/storage-emulator.json).
