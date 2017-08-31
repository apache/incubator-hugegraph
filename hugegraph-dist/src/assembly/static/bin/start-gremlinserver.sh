#!/bin/bash

function abs_path() {
    SOURCE="${BASH_SOURCE[0]}"
    while [ -h "$SOURCE" ]; do
        DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
        SOURCE="$(readlink "$SOURCE")"
        [[ $SOURCE != /* ]] && SOURCE="$DIR/$SOURCE"
    done
    echo "$( cd -P "$( dirname "$SOURCE" )" && pwd )"
}

parse_yaml() {
   local prefix=$2
   local s='[[:space:]]*' w='[a-zA-Z0-9_]*' fs=$(echo @|tr @ '\034')
   sed -ne "s|^\($s\)\($w\)$s:$s\"\(.*\)\"$s\$|\1$fs\2$fs\3|p" \
        -e "s|^\($s\)\($w\)$s:$s\(.*\)$s\$|\1$fs\2$fs\3|p"  $1 |
   awk -F$fs '{
      indent = length($1)/2;
      vname[indent] = $2;
      for (i in vname) {if (i > indent) {delete vname[i]}}
      if (length($3) > 0) {
         vn=""; for (i=0; i<indent; i++) {vn=(vn)(vname[i])("_")}
         printf("%s%s%s=\"%s\"\n", "'$prefix'",vn, $2, $3);
      }
   }'
}

BIN=`abs_path`
TOP="$(cd $BIN/../ && pwd)"

eval $(parse_yaml "$TOP/conf/gremlin-server.yaml")

GSRV_HOST=$host
GSRV_PORT=$port

GSRV_STARTUP_TIMEOUT_S=20
GSRV_SHUTDOWN_TIMEOUT_S=20
SLEEP_INTERVAL_S=2

VERBOSE=

# wait_for_startup friendly_name host port timeout_s
function wait_for_startup() {

    local server_name="$1"
    local host="$2"
    local port="$3"
    local timeout_s="$4"

    local now_s=`date '+%s'`
    local stop_s=$(( $now_s + $timeout_s ))

    echo -n "Connecting to $server_name ($host:$port)"
    while [ $now_s -le $stop_s ]; do
        echo -n .
        $BIN/checksocket.sh $host $port >/dev/null 2>&1
        if [ $? -eq 0 ]; then
            echo "OK"
            return 0
        fi
        sleep $SLEEP_INTERVAL_S
        now_s=`date '+%s'`
    done

    echo "timeout exceeded ($timeout_s seconds): could not connect to $host:$port" >&2
    return 1
}

echo "Starting GremlinServer..."
if [ -n "$VERBOSE" ]; then
    "$BIN"/gremlin-server.sh "$TOP"/conf/gremlin-server.yaml &
else
    "$BIN"/gremlin-server.sh "$TOP"/conf/gremlin-server.yaml >/dev/null 2>&1 &
fi
wait_for_startup 'GremlinServer' $GSRV_HOST $GSRV_PORT $GSRV_STARTUP_TIMEOUT_S || {
    echo "See $TOP/logs/hugegremlin-server.log for GremlinServer log output." >&2
}