"""All settings come from environment variables, with safe defaults for local dev."""
import os


def _float(name: str, default: float) -> float:
    return float(os.getenv(name, default))


def _int(name: str, default: int) -> int:
    return int(os.getenv(name, default))


class Settings:
    def __init__(self) -> None:
        self.database_url = os.getenv(
            "DATABASE_URL", "postgresql://relay:relay@localhost:5432/relay"
        )
        # Protects POST /endpoints (registering arbitrary URLs). Empty = disabled.
        self.admin_api_key = os.getenv("ADMIN_API_KEY", "")
        # Public URL of this service, used to build the demo receiver URL.
        self.public_base_url = os.getenv("PUBLIC_BASE_URL", "http://localhost:8000")

        # Worker
        self.worker_count = _int("WORKER_COUNT", 4)
        self.claim_batch_size = _int("CLAIM_BATCH_SIZE", 10)
        self.lease_seconds = _int("LEASE_SECONDS", 30)
        self.delivery_timeout = _float("DELIVERY_TIMEOUT", 5.0)
        self.idle_sleep = _float("IDLE_SLEEP", 0.5)

        # Retries
        self.max_attempts = _int("MAX_ATTEMPTS", 6)
        self.backoff_base = _float("BACKOFF_BASE", 1.0)
        self.backoff_cap = _float("BACKOFF_CAP", 30.0)

        # Demo housekeeping
        self.demo_retention_minutes = _int("DEMO_RETENTION_MINUTES", 60)


settings = Settings()
