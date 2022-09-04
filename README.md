# polimi-MWT_project_3
Middleware technologies (2021-2022) Project 3: Compute infrastructure

Tasks with computation times:
- image compression (t = 100)
- word count (t = 40)
- some calculus (t = 20)

## Getting started

### Install

```
pipenv install --system

./gradlew generateAvroJava installDist

sudo apt install librdkafka-dev libopenmpi-dev libssl-dev
# Install https://github.com/libcpr/cpr
# Install libserdes from this specific branch of my fork: https://github.com/eytienne/libserdes/tree/add_specific_serialization_support

cd back
make build

docker-compose build
```

# Run
```
docker-compose up -d

# Create the topics and associated schemas (for the first time)
./admin/build/install/admin-client/bin/admin-client librdkafka.config
python ./admin/src/setup-schema-registry.py

python -m http.server --directory demo/ 8000

# Querying the app
./front/build/install/front-client/bin/front-client count-words \
    http://0.0.0.0:8000/input1.txt \
    http://0.0.0.0:8000/input2.txt \
    http://0.0.0.0:8000/input3.txt \
    http://0.0.0.0:8000/input4.txt
```


## Project structure

- /admin - Kafka administration/setup.
- /back - Worker pool management.
- /client - Python CLI to query operations.

## Notes

Pool runners will be deployed on several machines through MPI. Partinioning will allow load paralellism between machines and MPI allows to execute the tasks using all the cores of the machine.

## Useful commands

```
docker-compose logs --follow --tail 500 broker

kafka-console-consumer --bootstrap-server localhost:29092 --include '(calculus|image-compression|word-count)' --from-beginning
```
