#!/bin/bash

set -ev

if [[ "$BACKEND" == "memory" ]]; then
    mvn test -P unit-test
fi
