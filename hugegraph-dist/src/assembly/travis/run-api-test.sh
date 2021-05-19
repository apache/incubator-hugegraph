#!/bin/bash

set -ev

TRAVIS_DIR=`dirname $0`
VERSION=`mvn help:evaluate -Dexpression=project.version -q -DforceStdout`
SERVER_DIR=hugegraph-$VERSION
CONF=$SERVER_DIR/conf/hugegraph.properties
REST_SERVER_CONF=$SERVER_DIR/conf/rest-server.properties
GREMLIN_SERVER_CONF=$SERVER_DIR/conf/gremlin-server.yaml

mvn package -DskipTests

sed -i 's/#auth.authenticator=/auth.authenticator=com.baidu.hugegraph.auth.StandardAuthenticator/' $REST_SERVER_CONF
sed -i 's/gremlin.graph=.*/gremlin.graph=com.baidu.hugegraph.auth.HugeFactoryAuthProxy/' $CONF
echo "
authentication: {
  authenticator: com.baidu.hugegraph.auth.StandardAuthenticator,
  authenticationHandler: com.baidu.hugegraph.auth.WsAndHttpBasicAuthHandler,
  config: {tokens: conf/rest-server.properties}
}" >> $GREMLIN_SERVER_CONF

$TRAVIS_DIR/start-server.sh $SERVER_DIR || (cat $SERVER_DIR/logs/hugegraph-server.log && exit 1)
mvn test -P api-test,$BACKEND || (cat $SERVER_DIR/logs/hugegraph-server.log && exit 1)
$TRAVIS_DIR/build-report.sh
$TRAVIS_DIR/stop-server.sh
