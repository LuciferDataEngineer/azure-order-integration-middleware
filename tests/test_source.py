"""Deterministic checks for Northwind projection, versioned messages and replay."""
from hashlib import sha256
from io import BytesIO
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

import order_source as source


class NorthwindSourceTests(unittest.TestCase):
    def setUp(self):
        self.raw = source.fixture_bytes()

    def test_projection_excludes_unrelated_columns_and_preserves_price_precision(self):
        text = self.raw.replace(b"NULL,NULL,NULL,0,NULL", b"NULL,NULL,NULL,0,'private fixture value'")
        events = list(source.make_submissions(source.parse_orders(text), source.digest(text), 3))
        self.assertNotIn(b"private fixture value", source.canonical(events))
        self.assertEqual(events[0]["lines"], [{"product_id":1, "quantity":3,
                                              "unit_price_minor":545, "discount_bps":500}])
        self.assertEqual(events[0]["order_date"], "2000-01-01")
        self.assertEqual(set(events[0]), {"schema_version","source_sha256","event_id","order_id",
                                         "order_date","version","status","lines"})

    def test_schema_change_or_duplicate_header_rejected(self):
        with self.assertRaisesRegex(source.SourceError, "column order"):
            source.parse_orders(self.raw.replace(b'"EmployeeID"', b'"AnotherField"'))
        with self.assertRaisesRegex(source.SourceError, "source order ID"):
            source.parse_orders(self.raw.replace(b'VALUES (2,NULL', b'VALUES (1,NULL'))

    def test_detail_grain_and_join_are_checked(self):
        with self.assertRaisesRegex(source.SourceError, "source line grain"):
            source.parse_orders(self.raw + b'INSERT "Order Details" VALUES(1,1,5.45,3,0.05)\n')
        with self.assertRaisesRegex(source.SourceError, "Orphan"):
            source.parse_orders(self.raw.replace(b"VALUES(1,1,", b"VALUES(800,1,"))
        with self.assertRaisesRegex(source.SourceError, "header without lines"):
            source.parse_orders(self.raw.split(b'INSERT "Order Details" VALUES(2')[0])

    def test_unrepresentable_literal_is_never_rounded_into_a_valid_amount(self):
        for token in (b"5.451", b"5.45000000000000000000000000000000000001"):
            with self.subTest(token=token), self.assertRaisesRegex(source.SourceError, "precision"):
                source.parse_orders(self.raw.replace(b"5.45", token))
        with self.assertRaisesRegex(source.SourceError, "precision"):
            source.parse_orders(self.raw.replace(b"0.05", b"1.0001"))

    def test_synthetic_amendment_preserves_base_and_stable_identity(self):
        orders = source.parse_orders(self.raw)
        events = list(source.make_submissions(orders, source.digest(self.raw), 3))
        self.assertEqual([(e["order_id"],e["version"],e["status"]) for e in events],
                         [(1,1,"submitted"), (2,1,"submitted"), (1,2,"amended")])
        self.assertEqual((orders[0]["lines"][0]["quantity"],events[2]["lines"][0]["quantity"]), (3,4))
        self.assertEqual(len({e["event_id"] for e in events}), 3)
        self.assertEqual(events, list(source.make_submissions(orders, source.digest(self.raw), 3)))
        orders[0]["lines"][0]["quantity"] = 32767
        with self.assertRaisesRegex(source.SourceError, "Amendment"):
            list(source.make_submissions(orders, source.digest(self.raw), 3))

    def test_local_acquisition_refuses_changed_input_or_corrupt_raw_cache(self):
        with TemporaryDirectory() as tmp, patch.object(source,"SOURCE_BYTES",len(self.raw)), \
                patch.object(source,"PIN",source.digest(self.raw)):
            root = Path(tmp)
            local = root/"source.sql"
            local.write_bytes(self.raw)
            source.acquire(root, local)
            archived = root/"raw"/source.digest(self.raw)/"instnwnd.sql"
            self.assertEqual(archived.read_bytes(), self.raw)
            local.write_bytes(self.raw[:-1])
            with self.assertRaisesRegex(source.SourceError,"SHA-256"):
                source.acquire(root,local)
            local.write_bytes(self.raw)
            archived.write_bytes(b"corrupt")
            with self.assertRaisesRegex(source.SourceError,"retained"):
                source.acquire(root,local)
            self.assertEqual(len(list(archived.parent.joinpath("captures").glob("*.json"))), 1)

    def test_bounded_reader_accepts_exact_limit_and_rejects_one_extra_byte(self):
        self.assertEqual(source.bounded_read(BytesIO(b"1234"),max_bytes=4), b"1234")
        with self.assertRaisesRegex(source.SourceError,"byte budget"):
            source.bounded_read(BytesIO(b"12345"),max_bytes=4)

    def test_fixture_rebuild_is_exact_and_retains_last_manifest_on_corruption(self):
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            receipt = source.build(root,fixture=True)
            original = (root/"source-smoke.json").read_bytes()
            rows = (root/receipt["deliveries_path"]).read_bytes()
            self.assertEqual(sha256(rows).hexdigest(), receipt["submissions_sha256"])
            self.assertEqual(receipt, source.build(root,fixture=True))
            (root/receipt["deliveries_path"]).write_bytes(b"corrupt")
            with self.assertRaisesRegex(source.SourceError,"retained"):
                source.build(root,fixture=True)
            self.assertEqual(original, (root/"source-smoke.json").read_bytes())
