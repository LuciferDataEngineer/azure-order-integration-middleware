"""Export the reviewed Northwind projection to create-only order blobs."""
import argparse
from contextlib import closing
from datetime import date
import hashlib
import os
from pathlib import Path
import re
import struct
from uuid import UUID

import order_source

WORKLOAD_SHA = "f7bd136bfbcd536264b1a60190045bc8602072a604e9855f79000c872d4e02a7"
SELECT_LINES = """SELECT TOP (2156) order_id, order_date, product_id, quantity, unit_price_minor, discount_bps
FROM dbo.PortfolioOrderLines ORDER BY order_id, product_id"""


def sql_orders(rows):
    orders = []
    previous = (0, 0)
    for oid, day, pid, quantity, minor, discount in rows:
        if (oid,pid) <= previous or minor != int(minor) or not isinstance(day,date):
            raise ValueError("Unexpected order grain, price precision or date")
        previous = oid,pid
        if not orders or orders[-1]["order_id"] != oid:
            orders.append({"order_id":oid, "order_date":day.isoformat(), "lines":[]})
        if orders[-1]["order_date"] != day.isoformat():
            raise ValueError("Dates disagree within an order")
        orders[-1]["lines"].append({"product_id":pid, "quantity":quantity, "unit_price_minor":int(minor),
                                    "discount_bps":discount})
    return orders


def checked_bodies(orders):
    if len(orders) != 830 or sum(len(o["lines"]) for o in orders) != 2155:
        raise ValueError("Expected the reviewed sample database projection")
    output = [(o["event_id"], order_source.canonical(o))
              for o in order_source.make_submissions(orders,order_source.PIN,1000)]
    digest = hashlib.sha256(b"".join(body+b"\n" for _,body in output)).hexdigest()
    if digest != WORKLOAD_SHA:
        raise ValueError("The SQL view differs from the reviewed sample workload")
    return output


def put_immutable(bodies, create, read_existing):
    created = reused = 0
    for event_id, body in bodies:
        name = f"events/{event_id}.json"
        digest = hashlib.sha256(body).hexdigest()
        if create(name,body,digest):
            created += 1
        elif read_existing(name,len(body),digest) == body:
            reused += 1
        else:
            raise ValueError("An existing event name contains different bytes")
    return {"created":created,"reused":reused}


def upload_orders(blobs,bodies):
    from azure.core import MatchConditions
    from azure.core.exceptions import ResourceExistsError
    from azure.storage.blob import ContentSettings
    def create(name,body,digest):
        try:
            blobs.upload_blob(name,body,overwrite=False,metadata={"sha256":digest},
                              content_settings=ContentSettings(content_type="application/json"),timeout=30)
            return True
        except ResourceExistsError:
            return False
    def read_existing(name,expected_size,digest):
        client=blobs.get_blob_client(name)
        props=client.get_blob_properties(timeout=15)
        if props.size != expected_size or (props.metadata or {}).get("sha256") != digest:
            raise ValueError("Existing blob size or digest conflicts")
        return client.download_blob(offset=0,etag=props.etag,match_condition=MatchConditions.IfNotModified,
                                    length=props.size,timeout=30).readall()
    return put_immutable(bodies,create,read_existing)


def cloud_export():
    if os.environ.get("ORDERS_EXPORT_ENABLED") != "true":
        raise ValueError("The manual export job is disabled")
    import pyodbc
    from azure.identity import ManagedIdentityCredential
    from azure.storage.blob import ContainerClient

    server=os.environ["ORDERS_SQL_SERVER"]
    account=os.environ["ORDERS_STORAGE_ACCOUNT"]
    if not re.fullmatch(r"[a-z0-9-]+\.database\.windows\.net",server) or not re.fullmatch(r"[a-z0-9]{3,24}",account):
        raise ValueError("Invalid SQL server or storage account")
    if (os.environ["ORDERS_SQL_DATABASE"],os.environ["ORDERS_CONTAINER"],os.environ["ORDERS_PREFIX"]) != ("NorthwindLab","orders","events/"):
        raise ValueError("Unexpected lab database, container or path")
    with closing(ManagedIdentityCredential(client_id=str(UUID(os.environ["AZURE_CLIENT_ID"])))) as identity:
        token=identity.get_token("https://database.windows.net/.default").token.encode("utf-16le")
        packed=struct.pack("<I",len(token))+token
        dsn=f"Driver={{ODBC Driver 18 for SQL Server}};Server=tcp:{server},1433;Database=NorthwindLab;Encrypt=yes;TrustServerCertificate=no"
        with closing(pyodbc.connect(dsn,attrs_before={1256:packed},timeout=15,autocommit=True)) as db:
            db.timeout=30
            with closing(db.cursor()) as cursor:
                bodies=checked_bodies(sql_orders(cursor.execute(SELECT_LINES)))
        with ContainerClient(f"https://{account}.blob.core.windows.net","orders",credential=identity) as blobs:
            return upload_orders(blobs,bodies)


if __name__=="__main__":
    p=argparse.ArgumentParser(description=__doc__)
    g=p.add_mutually_exclusive_group(required=True)
    g.add_argument("--prepare-source",type=Path,help="Read the retained pinned source SQL locally")
    g.add_argument("--export-cloud",action="store_true")
    a=p.parse_args()
    if a.prepare_source:
        with a.prepare_source.open("rb") as f:
            raw=order_source.bounded_read(f)
        if order_source.digest(raw)!=order_source.PIN:
            raise ValueError("Source does not match the reviewed sample")
        print({"events":len(checked_bodies(order_source.parse_orders(raw))),"workload_sha256":WORKLOAD_SHA})
    else:
        print(cloud_export())
