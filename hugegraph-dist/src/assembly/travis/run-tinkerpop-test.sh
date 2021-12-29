#!/bin/bash

set -ev

BACKEND=$1
SUITE=$2

if [[ "$SUITE" == "structure" || "$SUITE" == "tinkerpop" ]]; then
    mvn test -P tinkerpop-structure-test,$BACKEND
fi

if [[ "$SUITE" == "process" || "$SUITE" == "tinkerpop" ]]; then
    mvn test -P tinkerpop-process-test,$BACKEND
fi
