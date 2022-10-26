#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

FROM ubuntu:xenial

LABEL maintainer="HugeGraph Docker Maintainers <hugegraph@googlegroups.com>"

ENV PKG_URL https://github.com/hugegraph

# 1. Install needed dependencies of GraphServer & RocksDB
RUN set -x \
    && apt-get -q update \
    && apt-get -q install -y --no-install-recommends --no-install-suggests \
       curl \
       lsof \
       g++ \
       gcc \
       openjdk-8-jdk \
    && apt-get clean
    # && rm -rf /var/lib/apt/lists/*

# 2. Init HugeGraph Sever
# (Optional) You can set the ip of github to speed up the local build
# && echo "192.30.253.112 github.com\n151.101.44.249 github.global.ssl.fastly.net" >> /etc/hosts \
ENV SERVER_VERSION 0.12.0
RUN set -e \
    && mkdir -p /root/hugegraph-server \
    && curl -L -S ${PKG_URL}/hugegraph/releases/download/v${SERVER_VERSION}/hugegraph-${SERVER_VERSION}.tar.gz -o /root/server.tar.gz \
    && tar xzf /root/server.tar.gz --strip-components 1 -C /root/hugegraph-server \
    && rm /root/server.tar.gz \
    && cd /root/hugegraph-server/ \
    && sed -i "s/^restserver.url.*$/restserver.url=http:\/\/0.0.0.0:8080/g" ./conf/rest-server.properties \
    && sed -n '65p' ./bin/start-hugegraph.sh | grep "&" > /dev/null && sed -i 65{s/\&$/#/g} ./bin/start-hugegraph.sh \
    && sed -n '75p' ./bin/start-hugegraph.sh | grep "exit" > /dev/null && sed -i 75{s/^/#/g} ./bin/start-hugegraph.sh \
    && ./bin/init-store.sh

# 3. Prepare for HugeGraph Studio
ENV STUDIO_VERSION 0.10.0
# (Optional) You can set the ip of github to speed up the local build
# && echo "192.30.253.112 github.com\n151.101.44.249 github.global.ssl.fastly.net" >> /etc/hosts \
RUN set -e \
    && mkdir -p /root/hugegraph-studio \
    && curl -L -S ${PKG_URL}/hugegraph-studio/releases/download/v${STUDIO_VERSION}/hugegraph-studio-${STUDIO_VERSION}.tar.gz -o /root/studio.tar.gz \
    && tar xzf /root/studio.tar.gz --strip-components 1 -C /root/hugegraph-studio \
    && rm /root/studio.tar.gz \
    && cd /root/hugegraph-studio/ \
    && sed -i "s/^studio.server.host.*$/studio.server.host=0.0.0.0/g" ./conf/hugegraph-studio.properties \
    && sed -i "s/^graph.server.host.*$/graph.server.host=0.0.0.0/g" ./conf/hugegraph-studio.properties

EXPOSE 8080 8088
WORKDIR /root
VOLUME /root

ENTRYPOINT ["./hugegraph-server/bin/start-hugegraph.sh"]
