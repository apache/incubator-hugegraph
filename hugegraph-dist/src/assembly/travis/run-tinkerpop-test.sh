#!/bin/bash

set -ev

if [[ "$SUITE" == "structure" || "$SUITE" == "tinkerpop" ]]; then
    mvn test -P tinkerpop-structure-test,$BACKEND
fi

if [[ "$SUITE" == "process" || "$SUITE" == "tinkerpop" ]]; then
    mvn test -P tinkerpop-process-test,$BACKEND
fi
