#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with this
# work for additional information regarding copyright ownership. The ASF
# licenses this file to You under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.
#

# wait for storage like cassandra
./bin/wait-storage.sh

# set auth if needed 
if [[ $AUTH == "true" ]]; then
    # set password if use do not provide
    if [ -z "$PASSWORD" ]; then
        echo "you have not set the password, we will use the default password"
        PASSWORD="hugegraph"
    fi
    echo "init hugegraph with auth"
    ./bin/enable-auth.sh
    echo "$PASSWORD" | ./bin/init-store.sh
else
    ./bin/init-store.sh
fi

# start hugegraph
./bin/start-hugegraph.sh -j "$JAVA_OPTS" -g zgc

tail -f /dev/null
