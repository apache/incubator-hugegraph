#!/bin/bash

set -ev

BACKEND=$1

if [[ "$BACKEND" == "memory" ]]; then
    mvn test -P unit-test
fi
