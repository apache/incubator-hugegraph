#!/bin/bash
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
CONFIG_PATH=$1

function serial_test() {
    mvn clean test -Dconfig_path=$CONFIG_PATH -Pcore-test

    if [ $? -ne 0 ]; then
        echo "Failed to test."
        exit 1
    fi
}

function parallel_test() {
    # Run tests with background process
    (mvn clean test -Dconfig_path=$CONFIG_PATH -Pcore-test) &
    (mvn clean test -Dconfig_path=$CONFIG_PATH -Punit-test) &
    (mvn clean test -Dconfig_path=$CONFIG_PATH -Ptinkerpop-structure-test) &
    (mvn clean test -Dconfig_path=$CONFIG_PATH -Ptinkerpop-process-test) &

    # Wait for all child process finished
    for i in `seq 0 3`; do
        num=$(echo "$i+1" | bc -l)
        wait %$num
        if [ $? -ne 0 ]; then
            echo "Failed to test."
            exit 1
        fi
    done
}

# Remove dir prefix 'hugegraph-test' as mvn test execute in hugegraph-test
CONFIG_PATH=$(echo $CONFIG_PATH | sed 's/hugegraph-test\///g')

echo "Start test with config $CONFIG_PATH"

# Get the run-mode and run test
if [ $RUNMODE = "serial" ]; then
    echo "Run test in serial mode"
    serial_test
elif [ $RUNMODE = "parallel" ]; then
    echo "Run test in parallel mode"
    parallel_test
else
    echo "RUNMODE can only be 'serial' or 'parallel', but got $RUNMODE"
    echo "Failed to test."
    exit 1
fi

echo "Finish test."
