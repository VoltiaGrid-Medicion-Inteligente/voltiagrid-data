# Traceability matrix (US-05)

Every business rule of the brief (RN-01 to RN-11) points to the module that implements it and to its test in `tests/rules/test_acceptance.py`. Acceptance criteria CA-01 to CA-10 are the scenarios those tests check. RN-11 / CA-10 is an API rule, so its test lives in `voltiagrid-api`.

Pending tests are marked `xfail(strict=True)`. When a rule is implemented, its test passes, strict mode reports it as a failure (XPASS), and the `xfail` mark is removed. Update the **Status** column in the same PR.

## Business rules

| Rule  | What it says                                                                                                                                             | CA    | Target module               | Test                                                                                               | Status                                             |
| ----- | -------------------------------------------------------------------------------------------------------------------------------------------------------- | ----- | --------------------------- | -------------------------------------------------------------------------------------------------- | -------------------------------------------------- |
| RN-01 | Normalize each reading to the nearest 30-minute interval if it is within 5 minutes; otherwise mark it as invalid clock                                   | CA-01 | `pipeline.cleaning`         | `test_ca_01_reading_at_10_33_is_normalized_and_10_41_is_invalid_clock`                             | Pending                                            |
| RN-02 | Two readings for the same meter and interval: the one with the highest concentrator sequence wins                                                        | CA-02 | `pipeline.cleaning`         | `test_ca_02_duplicate_interval_keeps_highest_sequence`                                             | Pending                                            |
| RN-03 | Gaps up to 2 h by linear interpolation; longer gaps by the average profile of the same weekday over the last 4 weeks; every estimated reading is flagged | CA-03 | `pipeline.estimation`       | `test_ca_03_90_min_gap_interpolated_5_hour_gap_by_profile_both_flagged`                            | Pending                                            |
| RN-04 | A customer cannot be billed with more than 10% estimated readings in the month; an inspection order is created instead                                   | CA-04 | `pipeline.billing`          | `test_ca_04_customer_with_12_pct_estimated_gets_inspection_order_and_no_bill`                      | Pending                                            |
| RN-05 | Each interval's consumption is assigned to the tariff band (F7) valid on the reading date                                                                | —     | `validate_tariffs.band_for` | `test_rn_05_consumption_gets_the_band_valid_on_the_reading_date`                                   | **Implemented** (lookup; Spark assignment pending) |
| RN-06 | Transformer loss = delivered (F5) − sum of its meters; above 12% for 3 consecutive days → suspicious transformer                                         | CA-05 | `pipeline.losses`           | `test_ca_05_transformer_with_15_pct_loss_for_3_days_is_suspicious_and_meters_evaluated`            | Pending                                            |
| RN-07 | Suspicious meter: zero consumption for 48 h or more in a suspicious transformer, or a drop above 60% versus its 8-week average without a contract change | CA-05 | `pipeline.losses`           | `test_ca_05_transformer_with_15_pct_loss_for_3_days_is_suspicious_and_meters_evaluated`            | Pending                                            |
| RN-08 | Demand-response event if forecast temperature > 30 °C and projected load > 85% of circuit capacity; max 3 h, peak band only                              | CA-06 | `pipeline.demand_response`  | `test_ca_06_event_only_when_both_conditions_and_max_3_hours`                                       | Pending                                            |
| RN-09 | Saving = baseline (same intervals of the previous 5 working days without event) − actual; incentive only if saving ≥ 10%                                 | CA-07 | `pipeline.demand_response`  | `test_ca_07_no_incentive_with_8_pct_saving_incentive_with_11_pct`                                  | Pending                                            |
| RN-10 | A customer can take part in at most 8 events per month                                                                                                   | —     | `pipeline.demand_response`  | `test_rn_10_customer_participates_in_at_most_8_events_per_month`                                   | Pending                                            |
| RN-11 | A customer's interval data can only be read by that customer and authorized areas | CA-10 | voltiagrid-api `ConsumptionService` (US-13) | voltiagrid-api `tests/test_ca10_isolation.py::test_ca_10_customer_cannot_read_another_customers_consumption` | **Implemented** |

## Acceptance criteria without a business rule

| CA    | What it checks                                                                                  | Target                    | Test                                                                         | Status  |
| ----- | ----------------------------------------------------------------------------------------------- | ------------------------- | ---------------------------------------------------------------------------- | ------- |
| CA-08 | A 24 h resend of accumulated readings from 1,000 meters is processed without loss or duplicates | `pipeline.control_counts` | `test_ca_08_24h_resend_of_1000_meters_without_loss_or_duplicates`            | Pending |
| CA-09 | The full F1 history is processed in the cloud, reporting time, resources and cost               | `docs/data/cloud-run.md`  | `test_ca_09_full_f1_history_processed_in_cloud_with_time_resources_and_cost` | Pending |

## Coverage check

- CA-01 to CA-10: one test each (10 tests): nine here and CA-10 in `voltiagrid-api`.
- RN-01 to RN-11: every rule points to a module and a test; RN-05 and RN-10 have their own tests because the brief gives them no CA.
- Fixtures for the scenarios are in `tests/rules/conftest.py`.
