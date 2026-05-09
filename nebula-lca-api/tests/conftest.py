"""Pytest conftest for nebula-lca-api tests.

Ensures KEEP_LATEST_VERSIONS_PER_PROJECT=20 and AUTO_PRUNE_ON_STARTUP=0 are set
before app.main is imported (settings singleton).

Forcibly redirects DATABASE_URL to a per-session temporary SQLite file so tests
never touch the real lca_demo.db — even if the CI environment or .env sets it.
"""

import os
import sys
import tempfile
import uuid
from pathlib import Path

# These MUST be set BEFORE app.main is imported (settings singleton)
os.environ["KEEP_LATEST_VERSIONS_PER_PROJECT"] = "20"
os.environ["AUTO_PRUNE_ON_STARTUP"] = "0"

# -- forcibly redirect DATABASE_URL to a temp file -------------------------- #
# NOTE: We do NOT use setdefault here — even if a parent conftest or .env
# file sets DATABASE_URL to something else, tests MUST run against an
# isolated temp database.
_TEST_DB = Path(tempfile.gettempdir()) / f"nebula_test_{uuid.uuid4().hex}.db"
os.environ["DATABASE_URL"] = f"sqlite:///{_TEST_DB}"
