#!/bin/bash

set -ev

BACKEND=$1

TRAVIS_DIR=`dirname $0`
VERSION=`mvn help:evaluate -Dexpression=project.version -q -DforceStdout`
SERVER_DIR=hugegraph-$VERSION
CONF=$SERVER_DIR/conf/hugegraph.properties
REST_SERVER_CONF=$SERVER_DIR/conf/rest-server.properties
GREMLIN_SERVER_CONF=$SERVER_DIR/conf/gremlin-server.yaml

mvn package -DskipTests

# config rest-server
sed -i 's/#auth.authenticator=/auth.authenticator=com.baidu.hugegraph.auth.StandardAuthenticator/' $REST_SERVER_CONF
sed -i 's/#auth.admin_token=/auth.admin_token=admin/' $REST_SERVER_CONF

# config hugegraph.properties
sed -i 's/gremlin.graph=.*/gremlin.graph=com.baidu.hugegraph.auth.HugeFactoryAuthProxy/' $CONF

# config gremlin-server
echo "
authentication: {
  authenticator: com.baidu.hugegraph.auth.StandardAuthenticator,
  authenticationHandler: com.baidu.hugegraph.auth.WsAndHttpBasicAuthHandler,
  config: {tokens: conf/rest-server.properties}
}" >> $GREMLIN_SERVER_CONF

$TRAVIS_DIR/start-server.sh $SERVER_DIR $BACKEND || (cat $SERVER_DIR/logs/hugegraph-server.log && exit 1)

# run api-test
mvn test -P api-test,$BACKEND || (cat $SERVER_DIR/logs/hugegraph-server.log && exit 1)
$TRAVIS_DIR/build-report.sh $BACKEND
$TRAVIS_DIR/stop-server.sh
