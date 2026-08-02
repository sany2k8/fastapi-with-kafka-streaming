"""The scoring model is pure, so it needs no database and no Kafka."""

import pytest

from app.fraud.risk_engine import assess, classify
from app.fraud.rules import RuleHit


@pytest.mark.parametrize(
    ("score", "expected"),
    [
        (0, "LOW"),
        (29, "LOW"),
        (30, "MEDIUM"),
        (69, "MEDIUM"),
        (70, "HIGH"),
        (100, "HIGH"),
        (101, "CRITICAL"),
        (120, "CRITICAL"),  # every rule firing at once
    ],
)
def test_classify_bands(score: int, expected: str) -> None:
    assert classify(score) == expected


def test_no_hits_is_low_and_approved() -> None:
    result = assess([], fraud_threshold=70)
    assert result.risk_score == 0
    assert result.risk_level == "LOW"
    assert result.decision == "approved"
    assert result.event_type == "payment.approved"
    assert result.reasons == []


def test_the_prd_scenario_scores_80_and_blocks() -> None:
    """Frequency + new device + country change = 80 -> HIGH -> blocked."""
    hits = [
        RuleHit("high_transaction_frequency", 40),
        RuleHit("new_device", 10),
        RuleHit("country_change", 30),
    ]
    result = assess(hits, fraud_threshold=70)

    assert result.risk_score == 80
    assert result.risk_level == "HIGH"
    assert result.decision == "blocked"
    assert result.event_type == "fraud.detected"
    assert set(result.reasons) == {"high_transaction_frequency", "new_device", "country_change"}


def test_medium_is_still_approved() -> None:
    """30-69 carries reasons but does not block - one threshold, one decision."""
    result = assess([RuleHit("country_change", 30), RuleHit("new_device", 10)], fraud_threshold=70)
    assert result.risk_score == 40
    assert result.risk_level == "MEDIUM"
    assert result.decision == "approved"


def test_threshold_is_inclusive() -> None:
    assert assess([RuleHit("x", 70)], fraud_threshold=70).decision == "blocked"
    assert assess([RuleHit("x", 69)], fraud_threshold=70).decision == "approved"
