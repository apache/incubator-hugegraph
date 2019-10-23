#!/bin/bash

set -ev

cd hugegraph-test
mvn jacoco:dump@pull-test-data -Dapp.host=localhost -Dapp.port=36320 -Dskip.dump=false
cd ../
java -jar $TRAVIS_DIR/jacococli.jar report hugegraph-test/target/jacoco-it.exec \
     --classfiles hugegraph-api/target/classes/com/baidu/hugegraph --xml report.xml
