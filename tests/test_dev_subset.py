import json
from pathlib import Path

import pytest

from dev_subset import DEV_SUBSET_FILE, choose_blocks, load_dev_blocks, required_items, select_files

ROOT = Path(__file__).resolve().parent.parent


def households_in(block: str, n: int, group: str, tariff: str, acorn: str) -> list[dict]:
    start = int(block.split("_")[1]) * 100
    return [
        {"LCLid": f"MAC{start + i:06d}", "stdorToU": tariff, "Acorn": acorn, "Acorn_grouped": group, "file": block}
        for i in range(n)
    ]


@pytest.fixture
def households():
    return (
        households_in("block_0", 50, "Affluent", "Std", "ACORN-A")
        + households_in("block_1", 50, "Affluent", "ToU", "ACORN-B")
        + households_in("block_2", 50, "Comfortable", "Std", "ACORN-F")
        + households_in("block_3", 50, "Comfortable", "Std", "ACORN-H")
        + households_in("block_4", 50, "Adversity", "Std", "ACORN-L")
        + households_in("block_5", 50, "Adversity", "ToU", "ACORN-Q")
        + households_in("block_6", 2, "ACORN-", "Std", "ACORN-")
    )


def test_subset_covers_every_relevant_group_and_both_tariffs(households):
    blocks = choose_blocks(households)
    subset = [h for h in households if h["file"] in blocks]
    present = {("acorn_grouped", h["Acorn_grouped"]) for h in subset} | {("tariff", h["stdorToU"]) for h in subset}
    assert required_items(households) <= present


def test_tiny_groups_are_not_forced_into_the_subset(households):
    assert ("acorn_grouped", "ACORN-") not in required_items(households)
    assert "block_6" not in choose_blocks(households)


def test_subset_has_at_least_200_households_and_is_much_smaller_than_the_dataset(households):
    blocks = choose_blocks(households)
    size = sum(1 for h in households if h["file"] in blocks)
    assert 200 <= size < len(households)


def test_choice_is_deterministic(households):
    assert choose_blocks(households) == choose_blocks(list(reversed(households)))


def test_only_the_chosen_block_files_are_read(tmp_path):
    paths = [tmp_path / f"block_{i}.csv" for i in range(12)]
    selected = select_files(paths, ["block_1", "block_10"])
    assert [p.name for p in selected] == ["block_1.csv", "block_10.csv"]


def test_missing_block_file_is_an_error(tmp_path):
    with pytest.raises(FileNotFoundError, match="block_7"):
        select_files([tmp_path / "block_1.csv"], ["block_1", "block_7"])


def test_committed_subset_file_is_valid():
    path = ROOT / DEV_SUBSET_FILE
    if not path.exists():
        pytest.skip("run `python dev_subset.py` to create the subset file")
    blocks = load_dev_blocks(path)
    assert blocks and all(b.startswith("block_") for b in blocks)
    assert json.loads(path.read_text(encoding="utf-8"))["households"] >= 1
    assert (ROOT / "docs" / "data" / "dev-subset.md").exists()