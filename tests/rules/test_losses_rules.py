"""pipeline.losses beyond the CA-05 scenario (RN-06, RN-07)."""

from pipeline.losses import evaluate_transformer, is_suspicious_meter, is_suspicious_transformer


def test_transformer_needs_3_consecutive_days_strictly_above_12_percent():
    assert is_suspicious_transformer([0.13, 0.13, 0.13]) is True
    assert is_suspicious_transformer([0.13, 0.13, 0.05, 0.13, 0.13]) is False
    assert is_suspicious_transformer([0.12, 0.12, 0.12]) is False
    assert is_suspicious_transformer([0.2, 0.2, None, 0.2]) is False


def test_meter_rules():
    assert is_suspicious_meter(zero_streak_intervals=96) is True
    assert is_suspicious_meter(zero_streak_intervals=95) is False
    assert is_suspicious_meter(drop_pct=0.61) is True
    assert is_suspicious_meter(drop_pct=0.60) is False
    assert is_suspicious_meter(drop_pct=0.9, contract_changed=True) is False
    assert is_suspicious_meter(zero_streak_intervals=96, contract_changed=True) is True  # zeros are not explained


def test_meters_are_evaluated_only_on_a_suspicious_transformer():
    meters = [{"meter_id": "M1", "zero_streak_intervals": 120}, {"meter_id": "M2", "drop_pct": 0.1}]
    assert evaluate_transformer([0.05, 0.15, 0.15, 0.15], meters)["suspicious_meters"] == ["M1"]
    normal = evaluate_transformer([0.05, 0.05, 0.05], meters)
    assert normal == {"is_suspicious": False, "meters_evaluated": False, "suspicious_meters": []}
