# Northwind source, synthetic messages and provenance

The input is Microsoft’s Northwind sample database initialization script. The SQL file credits “Copyright Microsoft, Inc. 1994 - 2000”. The repository supplies the Microsoft SQL Server Sample Code MIT license. The local adapter projects fictional order headers and item details. Historical date labels belong to the sample business. [Pinned Northwind script](https://github.com/microsoft/sql-server-samples/blob/8fe3fd0c9a825fef3ae6d601e97537b612d7b214/samples/databases/northwind-pubs/instnwnd.sql), [upstream license](https://github.com/microsoft/sql-server-samples/blob/8fe3fd0c9a825fef3ae6d601e97537b612d7b214/license.txt) and [preserved license text](MICROSOFT-LICENSE.txt).

## Observed input

On 11 November 2026 in Melbourne, the adapter downloaded the pinned public script over HTTPS. Its 1,049,720 bytes matched SHA-256 `3cc62b3fca6d244a47dbde698b809331e4f85988a0685b2b370717d431e94871`. It contained 830 order headers, 2,155 order-detail rows and 77 distinct products. The first order date was 4 July 1996 and the last was 6 May 1998. [Aggregate source receipt](../examples/source-smoke.json).

The parser reads numeric order and product IDs, the order date, line quantity, unit price and discount. It validates the expected order-header column sequence and checks that each detail joins to one header and has a distinct product within the order. Prices must convert exactly to hundredths of the sample price unit and discounts to basis points. The original script provides other fields including shipping and contact data. The public workload uses an explicit field projection, and the adapter reads SQL as text.

## Creating the workload

The generator sorts orders by numeric order ID and lines by product ID. Each of the 830 sample orders becomes a version-1 submission. The 170 lowest order IDs also receive an explicitly synthetic version-2 amendment. For an amendment, the first product’s quantity increases by one. All status labels, versions and event identities are synthetic. Combining the base and amended counts gives **1,000 distinct logical submissions**. Transport retry injections are additional deliveries of these identities.

Event identity is SHA-256 of compact JSON containing the generator version, source digest, numeric order ID and version in that order. The order date remains the sample order date on both versions. The generator writes one canonical JSON object per line, sorted keys, compact separators and a terminating LF. A second run against the retained pinned source produced identical bytes. The observed JSONL contains 468,693 bytes and has SHA-256 `f7bd136bfbcd536264b1a60190045bc8602072a604e9855f79000c872d4e02a7`.

## Reproduce and recover

Run in the project root with Python 3.12 or later:

```bash
# Download the pinned public SQL file, verify it and produce the 1,000-event workload.
python3 order_source.py --data-dir data

# Rebuild offline from the retained, verified bytes.
python3 order_source.py --data-dir data/rebuilt \
  --source-file data/raw/3cc62b3fca6d244a47dbde698b809331e4f85988a0685b2b370717d431e94871/instnwnd.sql

# Independent, fully invented two-order example plus one amendment.
python3 order_source.py --fixture --data-dir data/demo

python3 -m unittest discover -s tests -p test_source.py -v
```

Acquisition uses a 15-second socket timeout, a 2 MiB byte ceiling, and a 60-second budget checked between reads. Exact pinned size and digest must match before an immutable source is retained. Each successful acquisition writes its own capture receipt. Changed material at an existing content path raises an error.

The `deliveries_path` in `data/source-smoke.json` identifies an immutable JSONL file under the data directory. To resume a specific run, use that file and verify its digest against the receipt. The convenience copy `data/submissions.jsonl` and the latest receipt are individually replaced, with the receipt written last. A concurrently replaced convenience copy should be checked against the chosen receipt or replaced by the immutable delivery path. The `--fixture` path uses a separate invented input digest. The locally retained SQL script and row-level deliveries live under the ignored data directory.
