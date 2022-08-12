import glob
import json
import os
import re

from confluent_kafka.schema_registry import (Schema,
                                             SchemaRegistryClient)
from dotenv import load_dotenv

load_dotenv(f'{os.path.dirname(__file__)}/../resources/librdkafka.config')
client = SchemaRegistryClient({'url': os.getenv('schema.registry.url')})

dirname = f"{os.path.dirname(__file__)}/../resources/avro"

for filename in sorted(glob.glob('*.avsc', root_dir=dirname)):
    with open(f'{dirname}/{filename}', 'r') as file:
        parsed = json.load(file)
        name = parsed['name']
        subject = re.sub(r'(?<!^)(?=[A-Z])', '-', name).lower()
        schema = Schema(json.dumps(parsed), 'AVRO')
        client.register_schema(subject, schema)
        rschema = client.lookup_schema(subject, schema)
        print(f'{subject} schema registered {rschema.schema_id}')

