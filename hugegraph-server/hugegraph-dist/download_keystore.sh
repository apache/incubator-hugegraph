#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

curl --version >/dev/null 2>&1 ||
  {
    echo 'ERROR: Please install `curl` first if you need `hugegraph-server.keystore`'
    exit
  }

# TODO: perhaps it's necessary verify the checksum before reusing the existing keystore
if [[ ! -f hugegraph-server.keystore ]]; then
  curl -s -S -L -o hugegraph-server.keystore \
    https://github.com/apache/hugegraph-doc/raw/binary-1.0/dist/server/hugegraph-server.keystore ||
    {
      echo 'ERROR: Download `hugegraph-server.keystore` from GitHub failed, please check your network connection'
      exit
    }
fi

echo 'INFO: Successfully download `hugegraph-server.keystore`'
