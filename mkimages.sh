#!/usr/bin/env bash

# prepare parent image
# docker pull ubuntu:version

# build image
docker build -t hugegraph:3.0.0 -f Dockerfile .

# test
# docker run -itd -p 8080:8080 --name hubble hugegraph:3.0.0

# save image
# docker save hugegraph:3.0.0 > hg.tar
