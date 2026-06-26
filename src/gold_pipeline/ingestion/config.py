"""Typed configuration loaded from environment / .env."""
from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass

from dotenv import load_dotenv


@dataclass(frozen=True)
class Settings:
    fred_api_key: str
    database_url: str
    test_database_url: str
    ingest_start: str
    ingest_end: str

    @classmethod
    def from_env(cls, environ: Mapping[str, str] | None = None) -> "Settings":
        if environ is None:
            load_dotenv()
            environ = os.environ

        def require(key: str) -> str:
            val = environ.get(key)
            if not val:
                raise ValueError(f"Missing required env var: {key}")
            return val

        return cls(
            fred_api_key=require("FRED_API_KEY"),
            database_url=require("DATABASE_URL"),
            test_database_url=environ.get("TEST_DATABASE_URL", ""),
            ingest_start=environ.get("INGEST_START", "2015-01-01"),
            ingest_end=environ.get("INGEST_END", "2025-01-01"),
        )
