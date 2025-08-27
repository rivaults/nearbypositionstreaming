import asyncio
import json
from asyncio import CancelledError, Task
from datetime import datetime, timedelta
from typing import (
    Any,
    Callable,
    Coroutine,
)

from pinot_connect import Cursor, QueryOptions
from pinot_connect.rows import dict_row
from quart import websocket, Quart

from . import create_db

LISTEN_HOST = "websocket"

app = Quart(__name__)
conn = create_db()
IN_STREAM_OP = "select * from in_gps where id = {} AND CAST({} AS TIMESTAMP) < event ORDER BY event"
NEARBY_STREAM_OP = "select * from in_gps where id = {} AND CAST({} AS TIMESTAMP) < event ORDER BY event"
NEARBY_OP = 'select nearby, "timestamp", event from out_nearby where id = {} AND CAST({} AS TIMESTAMP) < event ORDER BY event'

WINDOWS_TIME: int = 150
TYPE_MAIN_STREAM: int = 0
TYPE_NEARBY_STREAM: int = 1


class TaskManager:
    def __init__(self):
        self._tasks: dict[int, Task] = dict()
        self._timeouts: dict[int, float] = dict()

    @property
    def tasks(self):
        return self._tasks

    @property
    def timeouts(self):
        return self._timeouts

    def get_timeout(self, key: int) -> int | None:
        if self.timeouts.get(key) is None:
            msg = f"Task {key} should not be cancelled"
            raise KeyError(msg)
        return self.timeouts.get(key)

    def set_timeout(self, key: int, timeout: int) -> None:
        self.timeouts[key] = timeout

    def add(self, key: int, task: Task, timeout: float = None) -> None:
        if key in self.tasks:
            raise KeyError("Task already exist")
        self.tasks[key] = task
        if timeout is not None:
            self.timeouts[key] = timeout
            app.logger.info("Add task %d with timeout %d", key, timeout)

    def __contains__(self, key: int) -> bool:
        return key in self.tasks

    def remove(self, key: int, task: Task) -> None:
        task.cancel()
        del self.tasks[key]
        del self.timeouts[key]
        app.logger.info("Remove task %d", key)

    def cancel_all(self) -> None:
        for t in self.tasks.values():
            t.cancel()
        self.tasks.clear()
        self.timeouts.clear()


def log_realtime(event: datetime) -> None:
    app.logger.info("realtime diff: %s", str(datetime.now() - event))


async def fetch_send(
    cursor: Cursor,
    op: str,
    _id: int,
    _type: int,
    start_timestamp: int = 0,
    manager: TaskManager = None,
) -> float:
    cursor.execute(op)
    row = cursor.fetchone()
    if row is None:
        return start_timestamp
    if manager is None:
        raise ValueError("TaskManager cannot be None")
    data = {
        "id": row["id"],
        "event": start_timestamp,
        "type": _type,
        "points": [[]],
    }
    has_data = False
    should_cancel = False
    while row is not None:
        is_split = timedelta(minutes=5) < (
            row["event"] - datetime.fromtimestamp(data["event"])
        )
        if is_split and 0 < data["event"]:
            data["points"].append([])
        if (
            _type == TYPE_NEARBY_STREAM
            and manager.get_timeout(_id) < row["event"].timestamp()
        ):
            should_cancel = True
            break
        log_realtime(row["event"])
        data["event"] = 1 + row["event"].timestamp()
        data["points"][-1].append((row["latitude"], row["longitude"]))
        has_data = True
        row = cursor.fetchone()
    if has_data:
        await websocket.send(json.dumps(data))
    if should_cancel:
        manager.remove(_id, asyncio.current_task())
    return data["event"]


async def add_task(
    cursor: Cursor,
    op: str,
    _: int,
    _type: int,
    start_timestamp: int = 0,
    manager: TaskManager = None,
) -> float:
    if manager is None:
        raise ValueError("TaskManager cannot be None")
    cursor.execute(op)
    current = cursor.fetchone()
    if current is None:
        return start_timestamp
    last_timestamp = start_timestamp
    while current is not None:
        current["timestamp"] = current["timestamp"].timestamp()
        current["event"] = current["event"].timestamp()
        if current["nearby"] in manager:
            manager.set_timeout(current["nearby"], current["timestamp"])
        else:
            manager.add(
                current["nearby"],
                asyncio.create_task(
                    ws_task(
                        NEARBY_STREAM_OP,
                        current["nearby"],
                        _type,
                        fetch_send,
                        start_timestamp=current["timestamp"] - WINDOWS_TIME,
                        manager=manager,
                    )
                ),
                current["timestamp"],
            )
        last_timestamp = current["event"] + 1
        current = cursor.fetchone()
    return last_timestamp


async def ws_loop(
    cursor: Cursor,
    query_p: str,
    _id: int,
    _type: int,
    fn: Callable[
        [Cursor, str, int, int, int, ...],
        Coroutine[Any, Any, float],
    ],
    start_timestamp: int = 0,
    **kwargs,
) -> None:
    _timestamp = start_timestamp
    while True:
        timestamp_ms = int(_timestamp * 1000)
        query = query_p.format(_id, timestamp_ms)
        _timestamp = await fn(cursor, query, _id, _type, _timestamp, **kwargs)
        await asyncio.sleep(1)


async def ws_task(
    op: str,
    _id: int,
    _type: int,
    fn: Callable[
        [Cursor, str, int, int, int, ...],
        Coroutine[Any, Any, float],
    ],
    **kwargs,
) -> None:
    cursor = None
    try:
        cursor = conn.cursor(
            row_factory=dict_row,
            query_options=QueryOptions(use_multi_stage_engine=True),
        )
        await ws_loop(cursor, op, _id, _type, fn, **kwargs)
    except CancelledError:
        cursor.close()
        raise


@app.websocket("/data")
async def ws_in():
    await websocket.accept()
    manager = TaskManager()
    try:
        _id = await websocket.receive()
        while True:
            manager.add(
                _id,
                asyncio.create_task(
                    ws_task(
                        IN_STREAM_OP, _id, TYPE_MAIN_STREAM, fetch_send, manager=manager
                    )
                ),
            )
            manager.add(
                -1,
                asyncio.create_task(
                    ws_task(
                        NEARBY_OP, _id, TYPE_NEARBY_STREAM, add_task, manager=manager
                    )
                ),
            )
            _id = await websocket.receive()
            manager.cancel_all()
    except CancelledError:
        manager.cancel_all()


@app.get("/health")
async def healthcheck():
    return "OK", 200


def main() -> None:
    app.run(host=LISTEN_HOST, port=5000, use_reloader=True, debug=True)
