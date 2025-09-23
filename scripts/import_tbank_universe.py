from __future__ import annotations

import argparse
import csv
from pathlib import Path
from typing import Iterable

import yaml

DEFAULT_OUTPUT = Path("data/tbank_universe.yml")
DELIMITERS = ",;\t"


def _normalize_symbol(raw: str) -> str | None:
    symbol = (raw or "").strip().upper()
    if not symbol:
        return None
    if ";" in symbol:
        symbol = symbol.split(";", 1)[0].strip().upper()
    return symbol or None


def _guess_type(symbol: str) -> str:
    if symbol.startswith("SU") or any(ch.isdigit() for ch in symbol[2:]):
        return "bond"
    if symbol.startswith("FX") or symbol.startswith("SB") or symbol.startswith("VTB"):
        return "etf"
    return "stock"


def _iter_rows(path: Path) -> Iterable[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig") as fh:
        sample = fh.read(4096)
        fh.seek(0)
        try:
            dialect = csv.Sniffer().sniff(sample, delimiters=DELIMITERS)
        except csv.Error:
            dialect = csv.get_dialect("excel")
        try:
            has_header = csv.Sniffer().has_header(sample)
        except csv.Error:
            has_header = True

        if has_header:
            reader = csv.DictReader(fh, dialect=dialect)
            for row in reader:
                yield {k.strip().upper(): (v or "").strip() for k, v in row.items()}
        else:
            reader = csv.reader(fh, dialect=dialect)
            for row in reader:
                if not row:
                    continue
                ticker = row[0] if len(row) > 0 else ""
                sec_type = row[1] if len(row) > 1 else ""
                yield {"TICKER": ticker, "TYPE": sec_type}


def load_csv(path: Path) -> dict[str, set[str]]:
    result = {"stocks": set(), "bonds": set(), "etfs": set()}
    for row in _iter_rows(path):
        ticker_raw = row.get("TICKER") or row.get("TICKER;BOARD") or ""
        symbol = _normalize_symbol(ticker_raw)
        if not symbol:
            continue
        sec_type = row.get("TYPE", "").strip().lower()
        if sec_type not in {"stock", "bond", "etf"}:
            sec_type = _guess_type(symbol)
        result.setdefault(sec_type + "s", set()).add(symbol)
    return result


def write_yaml(data: dict[str, set[str]], output: Path) -> None:
    structured = {key: sorted(values) for key, values in data.items() if values}
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", encoding="utf-8") as fh:
        yaml.safe_dump(structured, fh, allow_unicode=True, sort_keys=False)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Import T-Bank universe from CSV")
    parser.add_argument("input", type=Path, help="CSV with TICKER and optional TYPE column")
    parser.add_argument(
        "--out",
        type=Path,
        default=DEFAULT_OUTPUT,
        help="Output YAML file (default: data/tbank_universe.yml)",
    )
    args = parser.parse_args(argv)

    data = load_csv(args.input)
    write_yaml(data, args.out)
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
