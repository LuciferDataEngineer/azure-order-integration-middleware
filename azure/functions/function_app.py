"""Azure Functions Python v2. Package with package_function.py before a future deployment."""
from contextlib import closing
import os

import azure.functions as func
from azure.core import MatchConditions
from azure.core.exceptions import HttpResponseError, ResourceNotFoundError
from azure.data.tables import EdmType, EntityProperty, TableClient, UpdateMode
from azure.identity import ManagedIdentityCredential
from azure.servicebus import ServiceBusClient, ServiceBusMessage
from azure.storage.blob import BlobClient

from order_bridge import BridgeConflict, ConditionalWrite, apply_order, publish_created, workflow_authorized
from order_middleware import ContractError

app = func.FunctionApp()


class Tables:
    def __init__(self, client):
        self.client = client

    def get(self, partition, key):
        try:
            row = self.client.get_entity(partition, key)
            return row, row.metadata["etag"]
        except ResourceNotFoundError as exc:
            if getattr(exc.error_code, "value", exc.error_code) not in ("ResourceNotFound", "EntityNotFound"):
                raise
            return None

    def submit(self, operations):
        batch = []
        for verb, entity, etag in operations:
            if "TotalMinor" in entity:
                entity = {**entity, "TotalMinor":EntityProperty(entity["TotalMinor"],EdmType.INT64)}
            if verb == "replace":
                batch.append(("update", entity, {"mode":UpdateMode.REPLACE, "etag":etag,
                              "match_condition":MatchConditions.IfNotModified}))
            else:
                batch.append(("create", entity))
        try:
            self.client.submit_transaction(batch)
        except HttpResponseError as exc:
            if exc.status_code in (409,412):
                raise ConditionalWrite() from exc
            raise


def read_blob_version(client, etag, limit):
    props=client.get_blob_properties(timeout=15)
    if props.size > limit or props.size == 0 or props.etag.strip('"') != etag.strip('"'):
        raise BridgeConflict("Blob is oversized, empty, or has changed")
    raw=client.download_blob(offset=0,length=props.size,etag=props.etag,
        match_condition=MatchConditions.IfNotModified,timeout=30).readall()
    return raw, (props.metadata or {}).get("sha256")


@app.function_name(name="publish_order")
@app.event_grid_trigger(arg_name="notification")
def publish_order(notification: func.EventGridEvent):
    account = os.environ["ORDERS_STORAGE_ACCOUNT"]
    topic = os.environ["ORDERS_TOPIC"]
    with closing(ManagedIdentityCredential()) as identity:
        def read_version(name, etag, limit):
            with BlobClient(f"https://{account}.blob.core.windows.net", "orders", name, credential=identity) as client:
                return read_blob_version(client, etag, limit)
        def send(event_id, raw):
            with ServiceBusClient(os.environ["ORDERS_SERVICEBUS_NAMESPACE"],identity,retry_total=2) as bus:
                with bus.get_topic_sender(topic) as sender:
                    sender.send_messages(ServiceBusMessage(raw,message_id=event_id,content_type="application/json"),timeout=30)
        publish_created(notification.event_type,notification.subject,notification.get_json(),account,read_version,send)


@app.function_name(name="apply_order_delivery")
@app.route(route="apply/{consumer}",methods=["POST"],auth_level=func.AuthLevel.ANONYMOUS)
def consume_order(req: func.HttpRequest):
    consumer=req.route_params.get("consumer","")
    expected=os.environ.get("ORDERS_"+consumer.upper()+"_PRINCIPAL_ID","") if consumer in ("audit","fulfillment") else ""
    if not workflow_authorized(consumer,expected,req.headers.get("X-MS-CLIENT-PRINCIPAL-ID")):
        return func.HttpResponse(status_code=403)
    with closing(ManagedIdentityCredential()) as identity:
        account=os.environ["ORDERS_STORAGE_ACCOUNT"]
        with TableClient(f"https://{account}.table.core.windows.net",os.environ["ORDERS_EFFECTS_TABLE"],credential=identity) as client:
            try:
                outcome=apply_order(consumer,req.get_body(),Tables(client))
            except BridgeConflict:
                return func.HttpResponse("Conflicting event or version",status_code=409)
            except ContractError:
                return func.HttpResponse("Invalid order contract",status_code=422)
    return func.HttpResponse(outcome,status_code=200)
