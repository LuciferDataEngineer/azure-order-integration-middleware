"""Bounded offline-to-Azure storage adapter drill using three synthetic messages and local Azurite."""
import argparse
import base64
from contextlib import closing
import importlib.util
import json
import os
from pathlib import Path
import socket
import subprocess
import tempfile
import time

from azure.core.credentials import AzureNamedKeyCredential
from azure.data.tables import TableServiceClient
from azure.storage.blob import BlobServiceClient
from order_bridge import ConditionalWrite, apply_order, publish_created
from order_export import upload_orders
from order_source import canonical, digest, fixture_bytes, make_submissions, parse_orders

ROOT=Path(__file__).resolve().parent


def ports():
    sockets=[]
    try:
        for _ in range(3):
            s=socket.socket()
            sockets.append(s)
            s.bind(("127.0.0.1",0))
        return [s.getsockname()[1] for s in sockets]
    finally:
        for s in sockets:
            s.close()


def require(ok, message):
    if not ok:
        raise RuntimeError(message)


def check_clients(blobstore, tables, adapter):
    sample=fixture_bytes()
    records=list(make_submissions(parse_orders(sample),digest(sample),3))
    bodies=[(row["event_id"],canonical(row)) for row in records]
    with blobstore.create_container("orders") as blobs:
        first=upload_orders(blobs,bodies)
        repeated=upload_orders(blobs,bodies)
        require(first==dict(created=3,reused=0) and repeated==dict(created=0,reused=3),"Blob replay mismatch")
        sent=[]
        for identity, body in bodies:
            name=f"events/{identity}.json"
            props=blobs.get_blob_client(name).get_blob_properties()
            def read_version(key,etag,limit):
                return adapter.read_blob_version(blobs.get_blob_client(key),etag,limit)
            publish_created("Microsoft.Storage.BlobCreated","/blobServices/default/containers/orders/blobs/"+name,
                 {"url":f"https://ordersdemo.blob.core.windows.net/orders/{name}","eTag":props.etag},
                 "ordersdemo",read_version,lambda key,data:sent.append((key,data)))
        require(sent==bodies,"Pinned Blob+ETag reads changed canonical order bodies")
    with tables.create_table("OrderEffects") as client:
        backend=adapter.Tables(client)
        # Latest amendment arrives first, followed by both original versions.
        for sub in ("audit","fulfillment"):
            for i in (2,0,1):
                apply_order(sub,sent[i][1],backend)
            for _ in range(10):
                require(apply_order(sub,sent[0][1],backend)=="duplicate","Repeated body changed effects")
        rows=list(client.list_entities())
        receipts=[r for r in rows if r["RowKey"].startswith("r-")]
        audits=[r for r in rows if r["RowKey"].startswith("audit-")]
        current=[r for r in rows if r["RowKey"].startswith("current-")]
        require(len(receipts)==6 and len(audits)==3 and len(current)==2 and
                backend.get("fulfillment","current-1")[0]["Version"]==2,"Table effects mismatch")
        require(backend.get("fulfillment","r-"+sent[0][0])[0]["Outcome"]=="stale","Older version replaced the current one")
        # Prove a failure in action 2 of the entity-group transaction rolls back action 1.
        try:
            backend.submit([("create",{"PartitionKey":"audit","RowKey":"probe-never-visible"},None),
                            ("create",{"PartitionKey":"audit","RowKey":audits[0]["RowKey"]},None)])
        except ConditionalWrite:
            pass
        else:
            raise RuntimeError("Expected Table transaction conflict")
        require(backend.get("audit","probe-never-visible") is None,"Partial Table transaction observed")
        probe={"PartitionKey":"probe","RowKey":"etag","Step":1}
        backend.submit([("create",probe,None)])
        first_etag=backend.get("probe","etag")[1]
        backend.submit([("replace",{**probe,"Step":2},first_etag)])
        try:
            backend.submit([("replace",{**probe,"Step":3},first_etag)])
        except ConditionalWrite:
            pass
        else:
            raise RuntimeError("Stale ETag replaced a newer Table version")
        require(backend.get("probe","etag")[0]["Step"]==2,"Conditional update regression")
    return {"environment":"localhost Azurite, synthetic orders, captured send callback",
            "blobs_created":first["created"],"identical_blobs_reused":repeated["reused"],
            "canonical_bodies_recovered_by_etag":len(sent),"extra_deliveries_per_consumer":10,
            "business_receipts":len(receipts),"audit_effects":len(audits),"fulfillment_states":len(current),
            "stale_version_suppressed":True,"transaction_rollback_verified":True,"stale_etag_rejected":True,"azure_deployed":False}


def validate(binary):
    spec=importlib.util.spec_from_file_location("orders_function_app",ROOT/"azure/functions/function_app.py")
    adapter=importlib.util.module_from_spec(spec);spec.loader.exec_module(adapter)
    bp,qp,tp=ports()
    account="ordersdemo"
    key=base64.b64encode(os.urandom(48)).decode()
    credential=AzureNamedKeyCredential(account,key)
    with tempfile.TemporaryDirectory() as work:
        args=[str(binary.resolve()),"--silent","--disableTelemetry","--skipApiVersionCheck","--location",work]
        for service,port in (("blob",bp),("queue",qp),("table",tp)):
            args += [f"--{service}Host","127.0.0.1",f"--{service}Port",str(port)]
        process=subprocess.Popen(args,env={**os.environ,"AZURITE_ACCOUNTS":account+":"+key},
                                 stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
        try:
            for port in (bp,tp):
                deadline=time.monotonic()+15
                while True:
                    try:
                        with closing(socket.create_connection(("127.0.0.1",port),timeout=.2)):break
                    except OSError:
                        if time.monotonic()>=deadline or process.poll() is not None:raise RuntimeError("Azurite did not start")
                        time.sleep(.1)
            with BlobServiceClient(f"http://127.0.0.1:{bp}/{account}",credential) as bs, \
                 TableServiceClient(f"http://127.0.0.1:{tp}/{account}",credential=credential) as ts:
                return check_clients(bs,ts,adapter)
        finally:
            process.terminate()
            try: process.wait(timeout=6)
            except subprocess.TimeoutExpired:
                process.kill();process.wait()

if __name__=="__main__":
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument("--azurite",type=Path,required=True)
    print(json.dumps(validate(parser.parse_args().azurite),indent=2))
