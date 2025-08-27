from typing import Tuple
from confluent_kafka.schema_registry import SchemaRegistryClient, RegisteredSchema


def get_schema_from_registry(schema_registry_subject: str) -> Tuple[SchemaRegistryClient, RegisteredSchema]:
    registry_client = SchemaRegistryClient({'url': "http://schemaregistry:8085"})
    _schema = registry_client.get_latest_version(schema_registry_subject)
    return registry_client, _schema
