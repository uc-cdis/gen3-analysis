import base64
import json
from typing import Any, Dict, Optional


def encode_cursor(pit_id: str, after_key: Optional[Dict[str, Any]]) -> str:
    payload = {"pit_id": pit_id, "after_key": after_key}
    raw = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode("utf-8")


def decode_cursor(cursor: str) -> dict:
    raw = base64.urlsafe_b64decode(cursor.encode("utf-8"))
    return json.loads(raw)
