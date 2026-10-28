"""Independently audit verifier wire JSONL or legacy trace logs using Decimal.

No production decoder, book mutation, SBE code, or third-party packages are used.
Exit 1 on inconsistent snapshots, sequence gaps, or insufficient observations.
"""

import argparse
import gzip
import hashlib
import json
from decimal import Decimal, getcontext
from pathlib import Path

getcontext().prec = 50


def audit(path):
    books, sequences, cases, gaps = {}, {}, [], []
    frames = deltas = checks = mismatches = clocked = 0
    digest = hashlib.sha256()
    with Path(path).open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    source_file = gzip.open(path, "rb") if Path(path).suffix == ".gz" else Path(path).open("rb")
    with source_file as source:
        for raw in source:
            line = raw.decode("utf-8")
            if ".jsonl" in Path(path).suffixes:
                row = json.loads(line)
                frame = json.loads(row["frame"])
            elif "verification exchange frame" in line:
                text = json.JSONDecoder().raw_decode(line.split(" frame=", 1)[1])[0]
                frame = json.loads(text)
            else:
                continue
            frames += 1
            msg = frame.get("msg", {})
            sid, seq = frame.get("sid"), frame.get("seq")
            if sid is not None and seq is not None:
                if sid in sequences and seq != sequences[sid] + 1:
                    gaps.append({"sid": sid, "previous": sequences[sid], "current": seq})
                sequences[sid] = seq
            kind = frame.get("type")
            if kind not in ("orderbook_snapshot", "orderbook_delta"):
                continue
            clocked += msg.get("clock") is not None
            symbol = msg["market_ticker"]
            if kind == "orderbook_snapshot":
                actual = {}
                for side in ("yes", "no"):
                    modern = msg.get(side + "_dollars_fp")
                    # Kalshi omits an empty side in a complete snapshot.
                    for price, quantity in modern if modern is not None else msg.get(side) or []:
                        price = Decimal(str(price)) / (1 if modern is not None else 100)
                        quantity = Decimal(str(quantity))
                        if quantity:
                            actual[(side, price)] = quantity
                if symbol in books:
                    checks += 1
                    expected = books[symbol]
                    differences = [
                        {"side": key[0], "price": str(key[1]),
                         "reconstructed_quantity": str(expected.get(key, 0)),
                         "snapshot_quantity": str(actual.get(key, 0)),
                         "offset": str(actual.get(key, 0) - expected.get(key, 0))}
                        for key in sorted(set(actual) | set(expected))
                        if actual.get(key, 0) != expected.get(key, 0)
                    ]
                    if differences:
                        mismatches += 1
                        if len(cases) < 10:
                            cases.append({"symbol": symbol, "seq": seq, "clock": msg.get("clock"),
                                          "sending_ts_ms": frame.get("sending_ts_ms"),
                                          "levels": differences})
                books[symbol] = actual
            else:
                modern = "price_dollars" in msg
                price = Decimal(str(msg["price_dollars"] if modern else msg["price"])) / (1 if modern else 100)
                delta = Decimal(str(msg["delta_fp"] if "delta_fp" in msg else msg["delta"]))
                key = (msg["side"], price)
                book = books[symbol]  # A delta before its initial snapshot is invalid.
                quantity = book.get(key, 0) + delta
                if quantity < 0:
                    raise ValueError(f"negative quantity at sequence {seq}")
                if quantity:
                    book[key] = quantity
                else:
                    book.pop(key, None)
                deltas += 1
    return {"method": "Independent raw JSON and Decimal; no production code",
            "input": str(path), "sha256": digest.hexdigest(), "frames": frames,
            "deltas": deltas, "snapshot_checks": checks, "clocked_market_frames": clocked,
            "sequence_gaps": gaps, "snapshot_mismatches": mismatches, "cases": cases,
            "passed": checks > 0 and deltas > 0 and not gaps and mismatches == 0}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input", type=Path)
    parser.add_argument("--report", type=Path)
    args = parser.parse_args()
    result = audit(args.input)
    if args.report:
        with args.report.open("x", encoding="utf-8") as output:
            json.dump(result, output, indent=2)
            output.write("\n")
    print(json.dumps({key: value for key, value in result.items() if key != "cases"}, indent=2))
    raise SystemExit(0 if result["passed"] else 1)
