"""All configuration in one place. No bare os.environ anywhere else."""

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # --- Infrastructure ------------------------------------------------------
    kafka_bootstrap_servers: str = "localhost:29098"
    database_url: str = "postgresql+asyncpg://fraud:fraud@localhost:5436/fraud"
    log_json: bool = False

    # --- Kafka topology ------------------------------------------------------
    topic_payment_events: str = "payment.events"
    topic_fraud_events: str = "fraud.events"
    #: 3 partitions => up to 3 consumers per group can work in parallel.
    topic_partitions: int = 3

    #: Consumer groups. Each group gets its own copy of every message and its
    #: own committed offsets - that is what makes this fan-out, not a queue.
    group_fraud_detector: str = "fraud-detector"
    group_decision_writer: str = "decision-writer"
    group_audit_logger: str = "audit-logger"

    # --- Risk rules ----------------------------------------------------------
    rule_frequency_count: int = 20
    rule_frequency_window_seconds: int = 30
    rule_frequency_score: int = 40

    rule_large_amount: float = 5000.0
    rule_large_amount_score: int = 20

    rule_blocked_count: int = 5
    rule_blocked_window_seconds: int = 300
    rule_blocked_score: int = 20

    rule_country_window_seconds: int = 3600
    rule_country_score: int = 30

    rule_new_device_score: int = 10

    #: score >= this  =>  decision "blocked", event "fraud.detected"
    fraud_threshold: int = 70

    @property
    def consumer_groups(self) -> list[str]:
        return [
            self.group_fraud_detector,
            self.group_decision_writer,
            self.group_audit_logger,
        ]

    @property
    def topics(self) -> list[str]:
        return [self.topic_payment_events, self.topic_fraud_events]


@lru_cache
def get_settings() -> Settings:
    return Settings()
