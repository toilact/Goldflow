import pytest
from gold_pipeline.ingestion.config import Settings

def test_from_env_reads_values():
    env = {
        "FRED_API_KEY": "abc",
        "DATABASE_URL": "postgresql+psycopg2://u:p@h:5432/gold",
        "TEST_DATABASE_URL": "postgresql+psycopg2://u:p@h:5432/gold_test",
        "INGEST_START": "2020-01-01",
        "INGEST_END": "2021-01-01",
    }
    s = Settings.from_env(env)
    assert s.fred_api_key == "abc"
    assert s.ingest_start == "2020-01-01"

def test_from_env_missing_required_raises():
    with pytest.raises(ValueError, match="FRED_API_KEY"):
        Settings.from_env({})
