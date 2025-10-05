# connector/connector.py
import os, json, time, requests
from typing import Optional
from fivetran_connector_sdk import ConnectorApp, Stream, PrimaryKey, Cursor, State, Record

# Config (env first, then sample file)
_cfg = {}
try:
    with open(os.path.join(os.path.dirname(__file__), "config.sample.json"), "r", encoding="utf-8") as f:
        _cfg = json.load(f)
except Exception:
    pass

BASE_URL = os.getenv("BASE_URL", _cfg.get("BASE_URL", "http://127.0.0.1:5055"))
TIMEOUT = int(os.getenv("REQUEST_TIMEOUT_SEC", _cfg.get("REQUEST_TIMEOUT_SEC", 15)))

def _fetch(endpoint: str, since: Optional[str]):
    params = {"since": since} if since else {}
    for attempt in range(3):
        try:
            r = requests.get(f"{BASE_URL}/{endpoint}", params=params, timeout=TIMEOUT)
            r.raise_for_status()
            return r.json()
        except Exception:
            if attempt == 2: raise
            time.sleep(1 + attempt)
    return []

app = ConnectorApp(
    name="fitsense_connector",
    version="0.1.0",
    streams=[
        Stream(
            name="activities",
            primary_key=PrimaryKey(["activity_id"]),
            cursor=Cursor("updated_at"),
            json_schema={
                "type":"object",
                "properties":{
                    "activity_id":{"type":"string"},
                    "user_id":{"type":"string"},
                    "start_ts":{"type":"string","format":"date-time"},
                    "end_ts":{"type":"string","format":"date-time"},
                    "type":{"type":"string"},
                    "calories":{"type":"number"},
                    "steps":{"type":"integer"},
                    "avg_hr":{"type":"number"},
                    "max_hr":{"type":"number"},
                    "distance_km":{"type":"number"},
                    "updated_at":{"type":"string","format":"date-time"},
                    "source_json":{}
                },
                "required":["activity_id","user_id","start_ts","end_ts","updated_at"]
            }
        ),
        Stream(
            name="sleep",
            primary_key=PrimaryKey(["sleep_id"]),
            cursor=Cursor("updated_at"),
            json_schema={
                "type":"object",
                "properties":{
                    "sleep_id":{"type":"string"},
                    "user_id":{"type":"string"},
                    "date":{"type":"string","format":"date"},
                    "duration_min":{"type":"integer"},
                    "stages_json":{},
                    "updated_at":{"type":"string","format":"date-time"}
                },
                "required":["sleep_id","user_id","date","updated_at"]
            }
        ),
        Stream(
            name="hr_series",
            primary_key=PrimaryKey(["user_id","ts"]),
            cursor=Cursor("updated_at"),
            json_schema={
                "type":"object",
                "properties":{
                    "user_id":{"type":"string"},
                    "ts":{"type":"string","format":"date-time"},
                    "bpm":{"type":"integer"},
                    "updated_at":{"type":"string","format":"date-time"}
                },
                "required":["user_id","ts","updated_at"]
            }
        ),
    ],
)

@app.read
def read(stream: str, state: State):
    since = state.get(stream)  # last bookmark or None
    rows = _fetch(stream, since)
    latest = since
    for row in rows:
        up = row.get("updated_at")
        if up and (latest is None or up > latest):
            latest = up
        yield Record(data=row)
    if latest:
        state[stream] = latest
