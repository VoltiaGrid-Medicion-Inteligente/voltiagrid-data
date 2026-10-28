# Initial load of F1 and F2 into raw (US-23, T-23.3)

`load_raw.py` uploads the Kaggle files **unchanged** to the raw bucket, with the paths of the
*Data lake layout* page in `voltiagrid-docs` (T-23.4), and writes a `_manifest.json` per partition.

## What is loaded

| Source | Files | Size | Key prefix |
|---|---|---|---|
| F1 half-hourly readings | 112 `block_*.csv` | 7,501 MB | `source=f1/dataset=halfhourly/ingest_date=<date>/` |
| F1 households | `informations_households.csv` | 0.2 MB | `source=f1/dataset=households/ingest_date=<date>/` |
| F2 hourly weather | `weather_hourly_darksky.csv` | 1.9 MB | `source=f2/dataset=weather_hourly/ingest_date=<date>/` |

## How to run it

The Kaggle files must be under `data/raw/` (any subfolder). The bucket is the `raw_bucket` output of
`infra/envs/data` in `voltiagrid-api`.

```powershell
aws sso login --profile voltia-dev
$env:AWS_PROFILE = "voltia-dev"
python -m pip install -r requirements.txt
$raw = "<raw bucket>"
python load_raw.py --bucket $raw --dry-run     # plan only: 114 files
python load_raw.py --bucket $raw --dev         # first the 4 dev blocks, to check access and speed
python load_raw.py --bucket $raw               # full load
```

- If the load stops (network, laptop asleep), run the same command again with `--ingest-date <date of the first run>`.
  Files already uploaded with the same size are skipped.
- Keep the laptop plugged in and the sleep mode disabled during the full load.

## Load time

| Run | Files | MB | Time | Speed | Connection | Date |
|---|---|---|---|---|---|---|
| dev subset (`--dev`) | 6 uploaded (blocks 7, 11, 52, 86 + households + weather) | 268.3 | not recorded | not recorded | WiFi | 2026-10-28 |
| full, run 1 | 39 uploaded (blocks 0 to 40 except 7 and 11) | about 2,679 | interrupted while uploading `block_41` (connection lost) | not recorded | WiFi | 2026-10-28 |
| full, run 2 (resumed with `--ingest-date 2026-10-28`) | 69 uploaded, 45 skipped | 4,556.2 | 14.2 min | 5.3 MB/s | WiFi | 2026-10-28 |
| **total** | **114 files** | **7,503.5** | | | | |

At 5.3 MB/s the whole 7,503 MB takes about 24 minutes in one run.

What happened during the load:
- The first `--dev` attempt failed with `TokenRetrievalError: Token has expired` (expired SSO session). After `aws sso login` it ran again and uploaded the 6 files.
- Run 1 lost the internet connection while uploading `block_41`. Run 2, started with the same `--ingest-date`, skipped the 45 files already in the bucket and uploaded the other 69. This proves that the load resumes.
- The `_manifest.json` of the half-hourly partition lists 112 files and 7,865,680,017 bytes (7,501.3 MB, same as the profile of US-03). Its `started_at` and `finished_at` are those of run 2, the run that finished.

The script prints the last line `Done: N uploaded, N skipped, X MB in Y min (Z MB/s)`; copy those values here.

## Check the result

```powershell
aws s3 ls "s3://$raw/source=f1/dataset=halfhourly/" --recursive --summarize --human-readable | Select-Object -Last 3
aws s3 cp "s3://$raw/source=f1/dataset=halfhourly/ingest_date=<date>/_manifest.json" -
```

`Total Objects` must be 113 (112 blocks + manifest) and `Total Size` about 7.3 GiB.

## Cost

About 7.5 GB in S3 Standard: about 0.17 USD per month for the first 30 days, then lifecycle rules move it to
cheaper classes (T-23.2). Uploading is free; each PUT costs 0.000005 USD.