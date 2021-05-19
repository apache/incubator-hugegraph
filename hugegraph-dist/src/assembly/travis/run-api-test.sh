#!/bin/bash

set -ev

TRAVIS_DIR=`dirname $0`
VERSION=`mvn help:evaluate -Dexpression=project.version -q -DforceStdout`
SERVER_DIR=hugegraph-$VERSION
CONF=$TRAVIS_DIR/conf/hugegraph.properties
REST_CONF=$TRAVIS_DIR/conf/rest-server.properties
GREMLIN_CONF=$TRAVIS_DIR/conf/gremlin-server.yaml

mvn package -DskipTests

cp $CONF "${CONF}_bak"
cp $REST_CONF "${REST_CONF}_bak"
cp $GREMLIN_CONF "${GREMLIN_CONF}_bak"

sed -i '$/#auth.authenticator=/auth.authenticator=com.baidu.hugegraph.auth.StandardAuthenticator' $REST_CONF
sed -i '$/gremlin.graph=.*/gremlin.graph=com.baidu.hugegraph.auth.HugeFactoryAuthProxy/' $CONF
echo "
authentication: {
  authenticator: com.baidu.hugegraph.auth.StandardAuthenticator,
  authenticationHandler: com.baidu.hugegraph.auth.WsAndHttpBasicAuthHandler,
  config: {tokens: conf/rest-server.properties}
}" >> $GREMLIN_CONF

$TRAVIS_DIR/start-server.sh $SERVER_DIR || (cat $SERVER_DIR/logs/hugegraph-server.log && exit 1)
mvn test -P api-test,$BACKEND || (cat $SERVER_DIR/logs/hugegraph-server.log && exit 1)
$TRAVIS_DIR/build-report.sh
$TRAVIS_DIR/stop-server.sh

mv "${CONF}_bak" $CONF
mv "${REST_CONF}_bak" $REST_CONF
mv "${GREMLIN_CONF}_bak" $GREMLIN_CONF