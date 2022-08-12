# polimi-MWT_project_3
Middleware technologies (2021-2022) Project 3: Compute infrastructure

Tasks with computation times:
- image compression (t = 100)
- word count (t = 40)
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
./gradlew generateAvroJava installDist

cd back
make build
```

## Notes

Pool runners will be deployed on several machines through MPI. Partinioning will allow load paralellism between machines and MPI will be used to consume messages in a round-robin fashion between processes of a same machine.

mpirun -mca plm_rsh_args "-l adam" --host 172.26.0.2:4,172.26.0.3:4 -npernode 4 /app/bin/exe

## Useful commands

docker-compose logs --follow --tail 500 broker

kafka-console-consumer --bootstrap-server localhost:29092 --include '(calculus|image-compression|word-count)' --from-beginning