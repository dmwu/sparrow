#!/usr/bin/env bash
mvn clean
mvn compile
mvn package -Dmaven.test.skip=true
gcc -o0 benchmark.c -o benchmark