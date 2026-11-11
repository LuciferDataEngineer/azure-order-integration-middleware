import copy
from hashlib import sha256
import unittest

from order_source import fixture_bytes, parse_orders, make_submissions, canonical, digest
from order_bridge import BridgeConflict, ConditionalWrite, apply_order, publish_created, workflow_authorized
from order_export import put_immutable, sql_orders


class TableDouble:
    """All operations either commit together or fail on a stale ETag/occupied key."""
    def __init__(self):
        self.rows={}
        self.before_submit=None
        self.lost_ack=False

    def get(self,partition,key):
        return self.rows.get((partition,key))

    def submit(self,ops):
        if self.before_submit:
            hook=self.before_submit
            self.before_submit=None
            hook()
        staged=copy.deepcopy(self.rows)
        for verb,entity,etag in ops:
            key=entity["PartitionKey"],entity["RowKey"]
            old=staged.get(key)
            if (verb=="create" and old) or (verb=="replace" and (not old or old[1]!=etag)):
                raise ConditionalWrite
            staged[key]=(dict(entity),1 if old is None else old[1]+1)
        self.rows=staged
        if self.lost_ack:
            self.lost_ack=False
            raise TimeoutError("acknowledgment lost in test")


class BridgeTests(unittest.TestCase):
    def setUp(self):
        raw=fixture_bytes()
        self.orders=list(make_submissions(parse_orders(raw),digest(raw),3))
        self.first=canonical(self.orders[0])
        self.second=canonical(self.orders[2])
        self.event=self.orders[0]["event_id"]
        self.name=f"events/{self.event}.json"
        self.subject="/blobServices/default/containers/orders/blobs/"+self.name
        self.data={"url":"https://ordersdemo.blob.core.windows.net/orders/"+self.name,"eTag":"abc"}
        self.db=TableDouble()

    def test_duplicate_notifications_preserve_bus_identity(self):
        sent=[]
        def read(name,etag,limit):
            self.assertEqual((name,etag),(self.name,"abc"))
            return self.first,sha256(self.first).hexdigest()
        for _ in range(2):
            publish_created("Microsoft.Storage.BlobCreated",self.subject,self.data,"ordersdemo",read,
                            lambda identity,body:sent.append((identity,body)))
        self.assertEqual(sent,[(self.event,self.first)]*2)

    def test_wrong_blob_url_hash_or_order_id_never_sends(self):
        wrong=canonical(self.orders[1])
        for data,raw,metadata_hash in [({**self.data,"url":"https://other.example/orders"} ,self.first,digest(self.first)),
                                       (self.data,self.first,"0"*64),(self.data,wrong,digest(wrong))]:
            with self.subTest(data=data,hash=metadata_hash):
                with self.assertRaises(ValueError):
                    publish_created("Microsoft.Storage.BlobCreated",self.subject,data,"ordersdemo",
                                    lambda *_:(raw,metadata_hash),lambda *_:self.fail("unexpected publish"))

    def test_conditional_transaction_rechecks_newer_order(self):
        self.db.before_submit=lambda:apply_order("fulfillment",self.second,self.db)
        self.assertEqual(apply_order("fulfillment",self.first,self.db),"stale")
        self.assertEqual(self.db.get("fulfillment","current-1")[0]["Version"],2)
        self.assertEqual(apply_order("fulfillment",self.first,self.db),"duplicate")

    def test_lost_ack_and_independent_audit(self):
        self.db.lost_ack=True
        with self.assertRaises(TimeoutError):
            apply_order("fulfillment",self.first,self.db)
        before=copy.deepcopy(self.db.rows)
        self.assertEqual(apply_order("fulfillment",self.first,self.db),"duplicate")
        self.assertEqual(self.db.rows,before)
        self.assertEqual(apply_order("audit",self.first,self.db),"applied")
        self.assertEqual(apply_order("audit",self.first,self.db),"duplicate")

    def test_identity_and_version_collisions_never_overwrite(self):
        apply_order("audit",self.first,self.db)
        before=copy.deepcopy(self.db.rows)
        for changes in ({"order_id":77},{"event_id":"e"*64}):
            with self.assertRaises(BridgeConflict):
                apply_order("audit",canonical({**self.orders[0],**changes}),self.db)
        self.assertEqual(self.db.rows,before)

    def test_workflow_route_requires_configured_matching_principal(self):
        pid="00000000-0000-0000-0000-000000000001"
        self.assertTrue(workflow_authorized("audit",pid,pid))
        for sub,x,y in [("audit","",""),("audit",pid,None),("fulfillment",pid,pid[:-1]+"2"),("other",pid,pid)]:
            self.assertFalse(workflow_authorized(sub,x,y))

    def test_blob_export_reuses_matching_data_and_refuses_divergence(self):
        rows=[(self.event,self.first)]
        self.assertEqual(put_immutable(rows,lambda *_:False,lambda *_:self.first),{"created":0,"reused":1})
        with self.assertRaises(ValueError):
            put_immutable(rows,lambda *_:False,lambda *_:self.second)


class SqlProjectionTests(unittest.TestCase):
    def test_ordered_view_rows_reproduce_a_source_order_and_items(self):
        from datetime import date
        from decimal import Decimal
        day=date(1996,7,4)
        orders=sql_orders([(10248,day,11,12,Decimal("1400.0000"),0),
                           (10248,day,42,10,Decimal("980.0000"),500)])
        self.assertEqual(orders, [{"order_id":10248, "order_date":"1996-07-04","lines":[
            {"product_id":11,"quantity":12,"unit_price_minor":1400,"discount_bps":0},
            {"product_id":42,"quantity":10,"unit_price_minor":980,"discount_bps":500}]}])

    def test_duplicate_lines_and_fractional_minor_units_refuse_export(self):
        from datetime import date
        from decimal import Decimal
        row=(10248,date(1996,7,4),11,12,Decimal("1400"),0)
        for rows in ([row,row], [(*row[:4],Decimal("1400.0001"),0)]):
            with self.assertRaises(ValueError):
                sql_orders(rows)
