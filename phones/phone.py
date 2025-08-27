import random
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Generator, Any

from confluent_kafka import Producer
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import (
    SerializationContext,
    MessageField,
)

from constants import BROKER, TOPIC, SCHEMA_REGISTRY_SUBJECT
from schema import get_schema_from_registry

IN = Path("/app/assets")
PRODUCER_CONF = {
    'bootstrap.servers': BROKER,
    'queue.buffering.max.messages': 1000000,
    'compression.type': 'snappy'
}
SERIALIZER_CONF = {
    'auto.register.schemas': False,
    'use.latest.version': True
}


def get_id(t_path : str) -> int:
    return int(t_path.split("/")[1])


def get_record(t_id : int, line : str) -> dict[str, Any]:
    lat, long, _, alt, _, l_date, l_time = line.strip().split(",")
    altitude = int(round(float(alt)))
    timestamp = int(datetime.strptime(f"{l_date} {l_time}", "%Y-%m-%d %H:%M:%S").timestamp())
    return {
        "id" : t_id,
        "timestamp": timestamp,
        "latitude": lat,
        "longitude": long,
        "altitude": altitude,
    }


def extract_records(t_id: int, curr_path : Path) -> Generator[dict[str, Any], None, None]:
    f = open(curr_path)
    for i, line in enumerate(f):
        if 5 < i:
            yield get_record(t_id, line)
    f.close()


def extract(t_id : int, paths : Generator[Path, None, None]) -> Generator[dict[str, Any], None, None]:
    for p in sorted(paths):
        yield from extract_records(t_id, p)


def emit_gps_positions(t_id : int, t_path: Path, producer: Producer) -> None:
    registry_client, schema = get_schema_from_registry(SCHEMA_REGISTRY_SUBJECT)
    avro_serializer = AvroSerializer(
        registry_client,
        schema.schema,
        rule_conf=SERIALIZER_CONF
    )
    last = None
    for val in extract(t_id, t_path.iterdir()):
        if last is not None:
            time.sleep(min(15*60, (val["timestamp"] - last["timestamp"])))
        val["event"] = int(datetime.now(timezone.utc).timestamp() * 1000)
        producer.produce(
            TOPIC,
            key = t_id.to_bytes(8, 'big'),
            value = avro_serializer(val, SerializationContext(TOPIC, MessageField.VALUE)),
            timestamp = val["event"]
        )
        last = val
    producer.flush()


def main() -> None:
    threads = []
    i = 0
    producer = Producer(PRODUCER_CONF)
    for sp in IN.iterdir():
        if sp.is_dir():
            i += 1
            sp = sp.joinpath("Trajectory")
            threads.append(threading.Thread(target=emit_gps_positions, args=(i, sp, producer,)))
            threads[-1].start()
    for th in threads:
        th.join()