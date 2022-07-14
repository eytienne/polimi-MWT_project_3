# polimi-MWT_project_3
Middleware technologies (2021-2022) Project 3: Compute infrastructure

Tasks with computation times:
- image compression (t = 100)
- text formatting (t = 40)
- some calculus (t = 20)

## Getting started

```
docker-compose up -d

# Kafka set-up
java -jar admin/target/kafka-setup-1.0-SNAPSHOT.jar
pipenv install --system
python ./admin/src/setup-schema-registry.py

# Querying the app
cd client
client.py compress-image https://i.imgur.com/gQgPVcr.jpeg 0.9
```

## Project structure

- /admin - Kafka administration/setup.
- /back - Worker pool management.
- /client - Python CLI to query operations.

## Build

```
./gradlew installDist
```