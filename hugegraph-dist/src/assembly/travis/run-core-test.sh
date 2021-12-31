#!/bin/bash

set -ev

BACKEND=$1

mvn test -P core-test,$BACKEND
