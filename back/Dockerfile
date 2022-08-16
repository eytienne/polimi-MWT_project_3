FROM ubuntu:22.04

RUN apt update && apt install -y libopenmpi-dev

WORKDIR /app

COPY bin ./bin
COPY entrypoint.sh ./

ENTRYPOINT ["/app/entrypoint.sh"]