"""Service contracts for the project Function App. No Azure SDK required here."""
from hashlib import sha256
import re
from uuid import UUID

from order_middleware import MAX_BODY, ContractError, canonical_bytes, decode_order, total_minor

LIMIT = MAX_BODY
SUBSCRIPTIONS = {"audit", "fulfillment"}


class BridgeConflict(ValueError):
    """A permanent conflict requiring operator investigation."""


class ConditionalWrite(Exception):
    """Another writer won a conditional table transaction."""


def workflow_authorized(consumer, expected_id, platform_id):
    if consumer not in SUBSCRIPTIONS or not expected_id or not platform_id:
        return False
    try:
        return UUID(expected_id) == UUID(platform_id)
    except (ValueError, TypeError):
        return False


def publish_created(event_type, subject, event_data, account, read_version, send):
    """Read the exact bounded blob version and send its canonical order to the topic."""
    if event_type != "Microsoft.Storage.BlobCreated":
        raise ValueError("Expected a BlobCreated notification")
    match = re.fullmatch(r"/blobServices/default/containers/orders/blobs/(events/([0-9a-f]{64})\.json)", subject)
    if not match or not re.fullmatch(r"[a-z0-9]{3,24}", account):
        raise ValueError("Unexpected source subject or storage account")
    name, identity = match.groups()
    expected_url = f"https://{account}.blob.core.windows.net/orders/{name}"
    if event_data.get("url") != expected_url or not event_data.get("eTag"):
        raise ValueError("Blob URL or version is outside the expected upload contract")
    raw, metadata_sha = read_version(name, event_data["eTag"], LIMIT)
    if len(raw) > LIMIT or sha256(raw).hexdigest() != metadata_sha:
        raise BridgeConflict("Invalid blob size or digest")
    order = decode_order(raw)
    if order["event_id"] != identity or canonical_bytes(order) != raw:
        raise BridgeConflict("Blob name, body or canonical encoding differs from the event contract")
    send(identity, raw)
    return identity


def apply_order(consumer, raw, backend):
    """One receipt, version claim and business effect in a single partition transaction."""
    if consumer not in SUBSCRIPTIONS:
        raise ValueError("Unknown consumer")
    if len(raw) > LIMIT:
        raise ContractError("invalid_body_size")
    order = decode_order(raw)
    payload = canonical_bytes(order)
    fingerprint = sha256(payload).hexdigest()
    identity, oid, version = order["event_id"], order["order_id"], order["version"]
    base = {"PartitionKey": consumer, "EventId": identity, "OrderId": oid,
            "Version": version, "PayloadSHA256": fingerprint}
    receipt_key = "r-" + identity
    version_key = f"v-{oid}-{version}"
    state_key = f"current-{oid}"
    for _ in range(4):
        receipt = backend.get(consumer, receipt_key)
        if receipt:
            if receipt[0]["PayloadSHA256"] != fingerprint:
                raise BridgeConflict("Event ID has a different payload")
            return "duplicate"
        claim = backend.get(consumer, version_key)
        if claim:
            # A concurrently committed same-ID claim includes a receipt. Re-read it.
            if claim[0]["EventId"] == identity and claim[0]["PayloadSHA256"] == fingerprint:
                continue
            raise BridgeConflict("Order version has a different event")
        effect = {**base, "Status":order["status"], "TotalMinor":total_minor(order)}
        operations = [("create", {**base, "RowKey":version_key}, None)]
        outcome = "applied"
        if consumer == "audit":
            operations.append(("create", {**effect, "RowKey":"audit-"+identity, "Body":payload}, None))
        else:
            state = backend.get(consumer, state_key)
            if state and state[0]["Version"] >= version:
                outcome = "stale"
            else:
                operations.append(("replace" if state else "create",
                                   {**effect, "RowKey":state_key,"Body":payload},
                                   state[1] if state else None))
        operations.append(("create", {**base,"RowKey":receipt_key,"Outcome":outcome}, None))
        try:
            backend.submit(operations)
            return outcome
        except ConditionalWrite:
            continue
    raise RuntimeError("Contended Table transaction; retry delivery")
