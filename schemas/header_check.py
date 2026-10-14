import csv
from pathlib import Path


def read_header(path: Path) -> list[str]:
    with open(path, newline="", encoding="utf-8-sig") as f:
        return next(csv.reader(f))


def header_mismatch(path: Path, expected: list[str]) -> str | None:
    actual = [name.strip() for name in read_header(path)]
    if actual == expected:
        return None
    return f"{path.name}: header {actual} does not match schema {expected}"


def assert_header(path: Path, expected: list[str]) -> None:
    problem = header_mismatch(path, expected)
    if problem:
        raise ValueError(problem)