"""Pure scoring: hits in, score + level + decision out.

No I/O, no Kafka, no database - which is exactly why it is trivial to test.
"""

from dataclasses import dataclass

from app.fraud.rules import RuleHit

#: Max possible score is 40 + 20 + 20 + 30 + 10 = 120, so CRITICAL is reachable
#: only when every rule fires at once.
LEVEL_BANDS = (
    (0, 29, "LOW"),
    (30, 69, "MEDIUM"),
    (70, 100, "HIGH"),
    (101, 10_000, "CRITICAL"),
)


@dataclass(frozen=True)
class RiskAssessment:
    risk_score: int
    risk_level: str
    decision: str  # "approved" | "blocked"
    reasons: list[str]

    @property
    def event_type(self) -> str:
        return "fraud.detected" if self.decision == "blocked" else "payment.approved"


def classify(score: int) -> str:
    for low, high, level in LEVEL_BANDS:
        if low <= score <= high:
            return level
    return "CRITICAL"


def assess(hits: list[RuleHit], *, fraud_threshold: int) -> RiskAssessment:
    score = sum(hit.score for hit in hits)
    return RiskAssessment(
        risk_score=score,
        risk_level=classify(score),
        # One threshold, one decision. MEDIUM is still approved - it just
        # carries reasons, so you can see why it was borderline.
        decision="blocked" if score >= fraud_threshold else "approved",
        reasons=[hit.reason for hit in hits],
    )
