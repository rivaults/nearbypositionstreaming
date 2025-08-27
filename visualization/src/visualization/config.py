from typing import Any, List, Dict

LISTEN_HOST="webapp"
CONNECT_HOST="localhost"

PHONE_PATTERN: List[Dict[str, Any]] = [
    dict(offset="0", repeat="1", dash=dict(pixelSize=1, pathOptions=dict(color="#ef8354", weight=12))),
    dict(offset="18", repeat="25", arrowHead=dict(pixelSize=6, polygon=False, pathOptions=dict(color="#fff", stroke=True)))
]