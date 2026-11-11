"""Project a pinned Northwind sample into deterministic, versioned order messages."""
from __future__ import annotations

import argparse
from collections import defaultdict
from contextlib import contextmanager
from datetime import datetime, timezone
from decimal import Decimal
import hashlib
import json
import os
from pathlib import Path
import re
import tempfile
import time
from urllib.request import HTTPRedirectHandler, Request, build_opener
from uuid import uuid4

UPSTREAM = "https://github.com/microsoft/sql-server-samples"
COMMIT = "8fe3fd0c9a825fef3ae6d601e97537b612d7b214"
FILE_PATH = "samples/databases/northwind-pubs/instnwnd.sql"
URL = f"https://raw.githubusercontent.com/microsoft/sql-server-samples/{COMMIT}/{FILE_PATH}"
PIN = "3cc62b3fca6d244a47dbde698b809331e4f85988a0685b2b370717d431e94871"
SOURCE_BYTES = 1_049_720
MAX_BYTES = 2 * 1024 * 1024
GENERATOR = "northwind-submission-generator-v1"
COLUMNS = ("OrderID", "CustomerID", "EmployeeID", "OrderDate", "RequiredDate",
           "ShippedDate", "ShipVia", "Freight", "ShipName", "ShipAddress",
           "ShipCity", "ShipRegion", "ShipPostalCode", "ShipCountry")
ORDER_START = re.compile(r'^INSERT INTO "Orders"\s*$', re.M)
# Match the declared column order and just the projected literal prefix. Remaining
# fields are never evaluated or exported. The optional N prefix denotes SQL Unicode.
ORDER_PREFIX = re.compile(
    r'INSERT INTO "Orders"\s*\(([^()]{1,300})\)\s*VALUES\s*\(\s*'
    r"(\d+),\s*(?:N?'(?:''|[^'\r\n])*'|NULL),\s*(?:\d+|NULL),\s*N?'(\d{1,2}/\d{1,2}/\d{4})',"
)
DETAIL_PREFIX = 'INSERT "Order Details"'
DETAIL = re.compile(r'INSERT "Order Details" VALUES\(\s*(\d+),\s*(\d+),\s*(\d+(?:\.\d+)?),'
                    r'\s*(\d+),\s*(\d+(?:\.\d+)?)\s*\)\s*;?')


class SourceError(ValueError):
    """A source or projection contract failed."""


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise SourceError("Unexpected redirect from the pinned GitHub content URL")


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def canonical(record):
    return json.dumps(record, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False).encode()


def require(ok, message):
    if not ok:
        raise SourceError(message)


def scaled(value, factor, highest):
    require(len(value) <= 64, "Source numeric literal exceeds its size limit")
    numerator, denominator = Decimal(value).as_integer_ratio()
    result, remainder = divmod(numerator * factor, denominator)
    require(remainder == 0 and 0 <= result <= highest,
            "Source monetary or discount literal is outside the supported precision or range")
    return result


def parse_orders(raw):
    """Read only header IDs/dates and five numeric detail literals. Never execute SQL."""
    require(0 < len(raw) <= MAX_BYTES, "Input is empty or exceeds the source byte budget")
    text = raw.decode("utf-8-sig")
    dates = {}
    for start in ORDER_START.finditer(text):
        m = ORDER_PREFIX.match(text, start.start())
        require(m is not None, "Unsupported Orders INSERT layout")
        columns = tuple(part.strip().strip('"') for part in m[1].split(","))
        require(columns == COLUMNS, "Orders column order differs from the reviewed layout")
        order = int(m[2])
        require(0 < order <= 2**31-1 and order not in dates, "Duplicate or invalid source order ID")
        dates[order] = datetime.strptime(m[3], "%m/%d/%Y").date().isoformat()

    items = defaultdict(dict)
    for line in text.splitlines():
        if not line.startswith(DETAIL_PREFIX):
            continue
        m = DETAIL.fullmatch(line)
        require(m is not None, "Unsupported order-detail INSERT layout")
        order, product, price, quantity, discount = m.groups()
        order, product, quantity = int(order), int(product), int(quantity)
        require(order in dates, "Orphan source detail")
        require(0 < product <= 2**31-1 and product not in items[order], "Duplicate or invalid source line grain")
        require(0 < quantity <= 32767, "Quantity outside reviewed source domain")
        items[order][product] = {"product_id": product, "quantity": quantity,
            "unit_price_minor": scaled(price, 100, 2**31-1), "discount_bps": scaled(discount, 10_000, 10_000)}
    require(dates and set(dates) == set(items), "Source contains no orders or has a header without lines")
    require(all(len(v) <= 256 for v in items.values()), "Order exceeds line-count contract")
    return [{"order_id": key, "order_date": dates[key],
             "lines": [items[key][product] for product in sorted(items[key])]} for key in sorted(dates)]


def make_submissions(orders, source_sha, target):
    """Emit v1 in numeric order and v2 for the first target-len(orders) IDs."""
    require(re.fullmatch("[0-9a-f]{64}", source_sha), "Invalid source digest")
    require(len(orders) <= target <= min(1000, 2 * len(orders)), "Unsupported workload size")
    for ordinal in range(target):
        source = orders[ordinal % len(orders)]
        version = 1 + ordinal // len(orders)
        order_id = source["order_id"]
        lines = [line.copy() for line in source["lines"]]
        if version == 2:
            require(lines[0]["quantity"] < 32767, "Amendment exceeds the supported quantity range")
            lines[0]["quantity"] += 1
        yield {"schema_version": "order.v1", "event_id": digest(canonical([GENERATOR,source_sha,order_id,version])),
               "order_id":order_id, "version":version, "status":"submitted" if version==1 else "amended",
               "order_date":source["order_date"], "lines":lines, "source_sha256":source_sha}


@contextmanager
def temporary(directory):
    directory.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=directory,delete=False) as f:
        staged = Path(f.name)
    try:
        yield staged
    finally:
        staged.unlink(missing_ok=True)


def put(path, raw, immutable=False):
    with temporary(path.parent) as staged:
        staged.write_bytes(raw)
        if immutable:
            try:
                os.link(staged, path)
            except FileExistsError:
                require(path.stat().st_size == len(raw) and digest(path.read_bytes()) == digest(raw),
                        "Previously retained source or delivery bytes differ")
        else:
            os.replace(staged, path)


def bounded_read(stream, *, max_bytes=MAX_BYTES, budget=60):
    result = bytearray()
    deadline = time.monotonic() + budget
    while True:
        require(time.monotonic() < deadline, "Source read exceeded the time budget")
        block = stream.read(min(64*1024,max_bytes+1-len(result)))
        if not block:
            return bytes(result)
        result.extend(block)
        require(len(result) <= max_bytes, "Source exceeds the byte budget")


def acquire(data_dir, source_file=None):
    if source_file is None:
        req = Request(URL, headers={"User-Agent":"NorthwindOrderPortfolio/1.0"})
        source = build_opener(NoRedirect()).open(req, timeout=15)
        mode = "github_https"
    else:
        source = Path(source_file).open("rb")
        mode = "local_verified_copy"
    with source:
        raw = bounded_read(source)
    require(len(raw)==SOURCE_BYTES and digest(raw)==PIN, "Pinned source size or SHA-256 mismatch")
    captured = datetime.now(timezone.utc).isoformat()
    put(data_dir/"raw"/PIN/"instnwnd.sql", raw, immutable=True)
    receipt = {"access_mode":mode,"access_utc":captured,"commit":COMMIT,"url":URL,
               "source_bytes":len(raw),"source_sha256":PIN}
    put(data_dir/"raw"/PIN/"captures"/(uuid4().hex+".json"),canonical(receipt)+b"\n")
    return raw, receipt


def fixture_bytes():
    columns = ",".join('"'+c+'"' for c in COLUMNS)
    orders = [f'INSERT INTO "Orders"\n({columns})\nVALUES ({i},NULL,1,\'1/{i}/2000\','
              "NULL,NULL,NULL,0,NULL,NULL,NULL,NULL,NULL,NULL)" for i in (1,2)]
    return ("\n".join(orders + ['INSERT "Order Details" VALUES(1,1,5.45,3,0.05)',
                               'INSERT "Order Details" VALUES(2,2,2.50,2,0)'])+"\n").encode()


def build(data_dir, source_file=None, fixture=False):
    data_dir=Path(data_dir)
    if fixture:
        raw=fixture_bytes()
        receipt={"source_sha256":digest(raw),"source_bytes":len(raw),"access_mode":"invented_fixture"}
    else:
        raw,receipt=acquire(data_dir, source_file)
    orders=parse_orders(raw)
    base_lines=sum(len(o["lines"]) for o in orders)
    if not fixture:
        require((len(orders),base_lines)==(830,2155), "Pinned header/detail counts failed")
    events=list(make_submissions(orders,receipt["source_sha256"],3 if fixture else 1000))
    payload=b"".join(canonical(e)+b"\n" for e in events)
    payload_sha=digest(payload)
    immutable=Path("deliveries")/GENERATOR/payload_sha/"submissions.jsonl"
    put(data_dir/immutable,payload,immutable=True)
    info={**receipt,"generator":GENERATOR,"source_order_count":len(orders),"source_order_details":base_lines,
          "first_source_order_date":min(o["order_date"] for o in orders),
          "last_source_order_date":max(o["order_date"] for o in orders),
          "distinct_products":len({x["product_id"] for o in orders for x in o["lines"]}),
          "submissions":len(events),"version_1":len(orders),"synthetic_version_2":len(events)-len(orders),
          "unique_event_ids":len({e["event_id"] for e in events}),"unique_order_versions":len({(e["order_id"],e["version"]) for e in events}),
          "submissions_bytes":len(payload),"submissions_sha256":payload_sha,"deliveries_path":immutable.as_posix()}
    require(info["unique_event_ids"]==info["unique_order_versions"]==len(events),"Workload identity collision")
    put(data_dir/"submissions.jsonl",payload)
    put(data_dir/"source-smoke.json",(json.dumps(info,indent=2)+"\n").encode())
    return info


if __name__=="__main__":
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir",type=Path,default=Path("data"))
    group=parser.add_mutually_exclusive_group()
    group.add_argument("--source-file",type=Path,help="Rebuild offline from a retained, pinned upstream SQL file")
    group.add_argument("--fixture",action="store_true",help="Generate three invented submissions without network access")
    opts=parser.parse_args()
    print(json.dumps(build(opts.data_dir,opts.source_file,opts.fixture),indent=2))
