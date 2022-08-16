SHELL := bash

build:
	rm -r bin || true
	mkdir bin
	clang++ -g -std=c++20 -Iinclude -L/usr/local/lib \
		-I /opt/vcpkg/installed/x64-linux/include/ -L /opt/vcpkg/installed/x64-linux/lib \
		-o bin/runner src/runner.cpp \
		-lrdkafka -lserdes++ -lserdes -lavrocpp \
		-lmpi -lcpr -lcurl -lz -lssl -lcrypto

generateAvro:
	rm -r src/avro || true
	mkdir src/avro
	for name in ../avro/*.avsc; do \
    	avrogencpp -i $$name -o src/avro/`echo $$name | pcregrep -o1 '(\w+)\.avsc'`.hh -n c || true; \
    done