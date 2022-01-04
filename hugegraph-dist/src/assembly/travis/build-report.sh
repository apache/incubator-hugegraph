#!/bin/bash

set -ev

BACKEND=$1
JACOCO_PORT=$2

OPTION_CLASS_FILES_BACKEND="--classfiles hugegraph-$BACKEND/target/classes/com/baidu/hugegraph"
if [ "$BACKEND" == "memory" ]; then
    # hugegraph-memory is the same as hugegraph-core
    OPTION_CLASS_FILES_BACKEND=""
fi

cd hugegraph-test
mvn jacoco:dump@pull-test-data -Dapp.host=localhost -Dapp.port=$JACOCO_PORT -Dskip.dump=false
cd ../
java -jar $TRAVIS_DIR/jacococli.jar report hugegraph-test/target/jacoco-it.exec \
     --classfiles hugegraph-dist/target/classes/com/baidu/hugegraph \
     --classfiles hugegraph-api/target/classes/com/baidu/hugegraph \
     --classfiles hugegraph-core/target/classes/com/baidu/hugegraph \
     $OPTION_CLASS_FILES_BACKEND --xml report.xml
