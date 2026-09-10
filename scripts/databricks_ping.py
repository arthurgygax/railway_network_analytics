"""Prove we can write to and read from the Unity Catalog volume.

Free Edition blocks outbound network access, so Databricks cannot pull from Kafka.
This is the inbound path that replaces it. Diagnostic, not application code.
"""

import io
import json
import os
from datetime import UTC, datetime

from databricks.sdk import WorkspaceClient

VOLUME = "/Volumes/railway/raw/landing"

w = WorkspaceClient()  # reads DATABRICKS_HOST / DATABRICKS_TOKEN
print("workspace:", os.environ["DATABRICKS_HOST"])

# One JSONL record, shaped like a real StopObservation so the notebook read is meaningful.
record = {
    "stop_id": "ping-0000000000-0",
    "station_name": "connectivity test",
    "train_category": "PING",
    "observed_at": datetime.now(UTC).isoformat(),
}
name = f"ping-{datetime.now(UTC):%Y%m%dT%H%M%S}.jsonl"
body = (json.dumps(record) + "\n").encode("utf-8")

w.files.upload(f"{VOLUME}/{name}", io.BytesIO(body), overwrite=True)
print(f"uploaded {name} ({len(body)} bytes)")

print("\nvolume now contains:")
for entry in w.files.list_directory_contents(VOLUME):
    print(f"  {entry.name:40} {entry.file_size} bytes")

back = w.files.download(f"{VOLUME}/{name}").contents.read()
print(f"\nread back: {back.decode().strip()}")
assert json.loads(back) == record, "round-trip mismatch"
print("round-trip OK")