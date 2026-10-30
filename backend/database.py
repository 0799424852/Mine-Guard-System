import os

from sqlalchemy import text
from sqlmodel import Field, SQLModel, create_engine

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+psycopg://miner:minerpass@localhost:5432/minerdb",
)

engine = create_engine(DATABASE_URL, echo=False, pool_pre_ping=True)


class Telemetry(SQLModel, table=True):
    __tablename__ = "telemetry"

    id: int | None = Field(default=None, primary_key=True)
    ts: float = Field(index=True)
    miner_id: str = Field(index=True)
    zone: str
    ch4_lel: float
    co_ppm: float
    o2_pct: float
    h2s_ppm: float
    body_temp_c: float
    heart_rate: int
    x: int
    y: int


class Alert(SQLModel, table=True):
    __tablename__ = "alerts"

    id: int | None = Field(default=None, primary_key=True)
    ts: float = Field(index=True)
    miner_id: str
    zone: str
    level: str  # "WARN" | "EVACUATE"
    metric: str
    value: float


class Evacuation(SQLModel, table=True):
    __tablename__ = "evacuations"

    id: int | None = Field(default=None, primary_key=True)
    ts: float = Field(index=True)
    zone: str
    reason: str
    triggered_by: str
    status: str = "OPEN"  # "OPEN" | "CLOSED"


import time  # Add this import at the top of the file

def init_db() -> None:
    """Create tables and convert telemetry to a TimescaleDB hypertable."""
    max_retries = 30
    for attempt in range(max_retries):
        try:
            SQLModel.metadata.create_all(engine)
            with engine.begin() as conn:
                conn.execute(text("CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;"))
                try:
                    conn.execute(
                        text(
                            "SELECT create_hypertable('telemetry', 'ts', "
                            "if_not_exists => TRUE, migrate_data => TRUE);"
                        )
                    )
                except Exception as exc:
                    print(f"[database] hypertable creation skipped: {exc}")
            print("[database] Database initialized successfully.")
            return
        except Exception as e:
            print(f"[database] Waiting for database... attempt {attempt + 1}/{max_retries}. Error: {e}")
            time.sleep(2)
    
    raise RuntimeError("Could not connect to database after multiple retries.")