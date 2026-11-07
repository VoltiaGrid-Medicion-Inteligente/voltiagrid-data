"""Data quality checks of the lake after a run (US-49, T-49.3): counts, nulls and ranges.

Plain data: each check is a dataset, a name and a Spark SQL condition that a *bad* row meets. jobs/quality_checks.py
counts the bad rows of the run's partitions; any bad row fails the check. The limits are the ones of the rules:
25 kWh in 30 minutes is the out_of_range flag of the cleaning (US-30), 48 intervals a day, shares between 0 and 1.
"""

MAX_KWH_PER_INTERVAL = 25.0   # same limit as jobs.clean_readings (US-30)

# dataset -> [(check, condition of a bad row)]
ROW_CHECKS = {
    "clean": [
        ("meter_id not null", "meter_id IS NULL"),
        ("date not null", "date IS NULL"),
        ("interval only missing for an invalid clock", "interval_start IS NULL AND quality_flag <> 'invalid_clock'"),
        ("valid energy in 0..25 kWh",
         f"is_valid AND (energy_kwh IS NULL OR energy_kwh < 0 OR energy_kwh > {MAX_KWH_PER_INTERVAL})"),
        ("flagged readings carry no energy", "NOT is_valid AND energy_kwh IS NOT NULL"),
    ],
    "filled": [
        ("meter_id and interval not null", "meter_id IS NULL OR interval_start IS NULL"),
        ("energy in 0..25 kWh", f"energy_kwh < 0 OR energy_kwh > {MAX_KWH_PER_INTERVAL}"),
        ("no energy only when unavailable", "energy_kwh IS NULL AND coalesce(estimation_method, '') <> 'unavailable'"),
        ("estimated means a method", "is_estimated <> (estimation_method IS NOT NULL)"),
    ],
    "circuit_load": [
        ("load and capacity not negative", "load_pct < 0 OR capacity_kva <= 0"),
    ],
    "fact_consumption": [
        ("band and price present", "band_name IS NULL OR price_per_kwh IS NULL"),
        ("energy and amount not negative", "energy_kwh < 0 OR amount < 0"),
        ("estimated part of the energy", "estimated_kwh > energy_kwh + 0.0001 OR estimated_intervals > intervals"),
    ],
    "billing_status": [
        ("estimated share in 0..1", "estimated_pct < 0 OR estimated_pct > 1"),
        ("billable or inspection, never both", "is_billable = inspection_order"),
    ],
}

INTERVALS_PER_DAY = 48
