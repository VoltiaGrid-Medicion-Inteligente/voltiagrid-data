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

The script prints the last line `Done: 69 uploaded, 45 skipped | 4,556.2 | 14.2 min | 5.3 MB/s`; copy those values here.