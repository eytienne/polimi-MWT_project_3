#!/bin/sh
NPROC=$(nproc --all)
exec mpirun -np $NPROC ./bin/runner