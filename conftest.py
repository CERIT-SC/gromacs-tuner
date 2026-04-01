"""Root conftest: set TUNER_DB to a temp path before any imports touch /data."""

import os
import tempfile

# Must happen before api.db is imported so init_db() uses the tmp path.
_tmp_db = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
os.environ.setdefault("TUNER_DB", _tmp_db.name)
