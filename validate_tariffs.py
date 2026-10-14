import argparse
import csv
import sys
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path

DEFAULT_CSV = Path("data/reference/tariffs.csv")
COLUMNS = ["valid_from", "valid_to", "start_time", "end_time", "band_name", "price_per_kwh"]
REQUIRED_BANDS = {"peak", "intermediate", "off-peak"}
MAX_ROWS = 100
MIN_PERIODS = 2
ONE_SECOND = timedelta(seconds=1)


@dataclass(frozen=True)
class TariffRow:
    valid_from: date
    valid_to: date
    start_time: time
    end_time: time
    band_name: str
    price_per_kwh: Decimal

    @property
    def period(self) -> tuple[date, date]:
        return self.valid_from, self.valid_to

    def applies_to(self, ts: datetime) -> bool:
        return self.valid_from <= ts.date() <= self.valid_to and self.start_time <= ts.time() <= self.end_time


def load_tariffs(path: Path = DEFAULT_CSV) -> list[TariffRow]:
    with open(path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames != COLUMNS:
            raise ValueError(f"columns {reader.fieldnames} do not match {COLUMNS}")
        rows = []
        for line, raw in enumerate(reader, start=2):
            try:
                rows.append(TariffRow(
                    valid_from=date.fromisoformat(raw["valid_from"]),
                    valid_to=date.fromisoformat(raw["valid_to"]),
                    start_time=time.fromisoformat(raw["start_time"]),
                    end_time=time.fromisoformat(raw["end_time"]),
                    band_name=raw["band_name"].strip(),
                    price_per_kwh=Decimal(raw["price_per_kwh"]),
                ))
            except (ValueError, InvalidOperation) as exc:
                raise ValueError(f"line {line}: {exc}") from exc
    return rows


def _seconds(t: time) -> int:
    return t.hour * 3600 + t.minute * 60 + t.second


def validation_errors(rows: list[TariffRow]) -> list[str]:
    errors = []

    if len(rows) >= MAX_ROWS:
        errors.append(f"{len(rows)} rows, expected fewer than {MAX_ROWS}")

    periods = sorted({r.period for r in rows})
    if len(periods) < MIN_PERIODS:
        errors.append(f"{len(periods)} validity periods, expected at least {MIN_PERIODS}")

    missing = REQUIRED_BANDS - {r.band_name for r in rows}
    if missing:
        errors.append(f"missing bands: {sorted(missing)}")
    unknown = {r.band_name for r in rows} - REQUIRED_BANDS
    if unknown:
        errors.append(f"unknown bands: {sorted(unknown)}")

    for r in rows:
        if r.price_per_kwh <= 0:
            errors.append(f"{r.band_name} {r.valid_from}: price_per_kwh must be > 0")
        if r.valid_from > r.valid_to:
            errors.append(f"period {r.valid_from} to {r.valid_to}: valid_from is after valid_to")
        if r.start_time > r.end_time:
            errors.append(f"{r.band_name} {r.valid_from}: start_time is after end_time")

    # periods: no overlap, no gap
    for (_, prev_to), (next_from, _) in zip(periods, periods[1:]):
        if next_from <= prev_to:
            errors.append(f"periods overlap: one ends {prev_to}, next starts {next_from}")
        elif next_from - prev_to > timedelta(days=1):
            errors.append(f"gap between periods: {prev_to} to {next_from}")

    # bands inside each period: 00:00:00 to 23:59:59, no gap, no overlap
    for period in periods:
        bands = sorted((r for r in rows if r.period == period), key=lambda r: r.start_time)
        label = f"period {period[0]} to {period[1]}"
        if bands[0].start_time != time(0, 0, 0):
            errors.append(f"{label}: first band starts at {bands[0].start_time}, expected 00:00:00")
        if bands[-1].end_time != time(23, 59, 59):
            errors.append(f"{label}: last band ends at {bands[-1].end_time}, expected 23:59:59")
        for prev, nxt in zip(bands, bands[1:]):
            step = _seconds(nxt.start_time) - _seconds(prev.end_time)
            if step < 1:
                errors.append(f"{label}: {prev.band_name} and {nxt.band_name} overlap at {nxt.start_time}")
            elif step > 1:
                errors.append(f"{label}: gap between {prev.end_time} and {nxt.start_time}")

    return errors


def band_for(rows: list[TariffRow], ts: datetime) -> TariffRow:
    """The single tariff row that applies at ts. Raises if there is none or more than one."""
    matches = [r for r in rows if r.applies_to(ts)]
    if len(matches) != 1:
        raise LookupError(f"{ts}: {len(matches)} bands apply, expected exactly 1")
    return matches[0]


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate the F7 tariff calendar")
    parser.add_argument("--csv", type=Path, default=DEFAULT_CSV)
    args = parser.parse_args()

    rows = load_tariffs(args.csv)
    errors = validation_errors(rows)
    periods = sorted({r.period for r in rows})
    print(f"{args.csv}: {len(rows)} rows, {len(periods)} validity periods, "
          f"bands {sorted({r.band_name for r in rows})}")
    if errors:
        print("Tariff calendar is NOT valid:")
        for e in errors:
            print(f"  - {e}")
        sys.exit(1)
    print("Tariff calendar is valid: exactly one band and price for any date and time.")


if __name__ == "__main__":
    main()