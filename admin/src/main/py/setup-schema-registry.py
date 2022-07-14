import json
import os
import sys

from confluent_kafka.schema_registry import (Schema, SchemaReference,
                                             SchemaRegistryClient)
from dotenv import load_dotenv

load_dotenv(f'{os.path.dirname(__file__)}/../resources/librdkafka.config')

client = SchemaRegistryClient({'url': os.getenv('schema.registry.url')})

dirname = f"{os.path.dirname(__file__)}/../resources/avro"

taskSubmittedReferences = [
    SchemaReference('com.project.Calculus', 'calculus', -1),
    SchemaReference('com.project.ImageCompression', 'image-compression', -1),
    SchemaReference('com.project.TextFormatting', 'text-formatting', -1),
]
for ref in taskSubmittedReferences:
    basename = ref.name.removeprefix('com.project.')
    subject = ref.subject
    with open(f"{dirname}/{basename}.avsc") as avsc_f:
        schema = Schema(json.dumps(json.load(avsc_f)), 'AVRO')
        client.register_schema(subject, schema)
        rschema = client.lookup_schema(subject, schema)
        print(f'{basename} schema registered {rschema.schema_id}')
        ref.version = rschema.version
with open(f"{dirname}/TaskSubmitted.avsc") as avsc_f:
    schema = Schema(json.dumps(json.load(avsc_f)), 'AVRO', taskSubmittedReferences)
    schema_id = client.register_schema('task-submitted', schema)
    print('TaskSubmitted schema registered', schema_id)

taskCompletedReferences = [
    SchemaReference('com.project.ErrorResult', 'error-result', -1),
    SchemaReference('com.project.CalculusResult', 'calculus-result', -1),
    SchemaReference('com.project.ImageCompressionResult', 'image-compression-result', -1),
    SchemaReference('com.project.TextFormattingResult', 'text-formatting-result', -1),
]
for ref in taskCompletedReferences:
    basename = ref.name.removeprefix('com.project.')
    subject = ref.subject
    with open(f"{dirname}/{basename}.avsc") as avsc_f:
        schema = Schema(json.dumps(json.load(avsc_f)), 'AVRO')
        client.register_schema(subject, schema)
        rschema = client.lookup_schema(subject, schema)
        print(f'{basename} schema registered {rschema.schema_id}')
        ref.version = rschema.version
with open(f"{dirname}/TaskCompleted.avsc") as avsc_f:
    schema = Schema(json.dumps(json.load(avsc_f)), 'AVRO', taskCompletedReferences)
    schema_id = client.register_schema('task-completed', schema)
    print('TaskCompleted schema registered', schema_id)