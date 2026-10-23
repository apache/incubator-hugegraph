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

function command_available() {
    local cmd=$1
    if command -v "$cmd" >/dev/null 2>&1; then
        return 0
    fi
    return 1
}

# read a property from .properties file
function read_property() {
    # file path
    local file_name
    local property_name
    file_name=$1
    # replace "." to "\."
    property_name=$(echo "$2" | sed 's/\./\\\./g')
    cat "$file_name" | sed -n -e "s/^[ ]*//g;/^#/d;s/^$property_name=//p" | tail -1
}

function write_property() {
    local file=$1
    local key=$2
    local value=$3

    local os=$(uname)
    case $os in
        # Note: in mac os should use sed -i '' "xxx" to replace string,
        # otherwise prompt 'command c expects \ followed by text'.
        # See http://www.cnblogs.com/greedy-day/p/5952899.html
        Darwin) sed -i '' "s!$key=.*!$key=$value!g" "$file" ;;
        *) sed -i "s!$key=.*!$key=$value!g" "$file" ;;
    esac
}

function parse_yaml() {
    local file=$1
    local version=$2
    local module=$3

    cat "$file" | tr -d '\n {}'| awk -F',+|:' '''{
        pre="";
        for(i=1; i<=NF; ) {
            if(match($i, /version/)) {
                pre=$i;
                i+=1
            } else {
                result[pre"-"$i] = $(i+1);
                i+=2
            }
        }
    } END {for(e in result) {print e": "result[e]}}''' \
    | grep "$version-$module" | awk -F':' '{print $2}' | tr -d ' ' && echo
}

function process_num() {
    num=$(ps -ef | grep "$1" | grep -v grep | wc -l)
    return "$num"
}

function process_id() {
    pid=$(ps -ef | grep "$1" | grep -v grep | awk '{print $2}')
    return "$pid"
}

# Emit $1 as ONE complete POSIX shell word, INCLUDING the surrounding quotes.
# Callers should NOT add another pair of quotes:
#     /data/team's-logs  ->  '/data/team'\''s-logs'
#
function shell_quote() {
    local sq="'" repl="'\\''"
    printf "'%s'" "${1//$sq/$repl}"
}

# Validate a path-like value. $1=value  $2=label for the error  $3=crlf|cron|classpath|xlog
#   crlf      : reject CR/LF. Call on the RAW value (e.g. $OPTARG) BEFORE
#               canonicalize_dir/canonicalize_file.
#   cron      : crlf + reject '%'.
#   classpath : crlf + reject ':' and ';'. For a directory whose contents are
#               joined into a ':'-separated CLASSPATH (the plugins dir), where
#               a ':' in the path itself would split one entry into two bogus
#               ones. On Cygwin, a classpath converted with 'cygpath -p -w' is
#               re-delimited on ';' instead, so a ';' in the path would do the
#               same splitting there.
#   xlog      : crlf + reject ':'. For a path embedded inside a JVM unified
#               logging (-Xlog) value, whose own grammar ("tag-selection:
#               output:decorators:output-options") also delimits on ':'. An
#               embedded ':' there is not a Bash word-splitting problem --
#               quoting the argv element does not help -- it desyncs the
#               JVM's own parser and the JVM refuses to start.
function reject_unsafe_path() {
    case "$1" in
        *[$'\n\r']*)
            echo "ERROR: $2 must not contain CR/LF characters" >&2
            return 1 ;;
    esac

    case "$3" in
        cron)
            case "$1" in
                *%*)
                    echo "ERROR: $2 must not contain '%' (reserved by cron)" >&2
                    return 1 ;;
            esac ;;
        classpath)
            case "$1" in
                *:*)
                    echo "ERROR: $2 must not contain ':' (CLASSPATH separator)" >&2
                    return 1 ;;
                *';'*)
                    echo "ERROR: $2 must not contain ';' (CLASSPATH separator on Cygwin)" >&2
                    return 1 ;;
            esac ;;
        xlog)
            case "$1" in
                *:*)
                    echo "ERROR: $2 must not contain ':' (JVM unified logging '-Xlog' field separator)" >&2
                    return 1 ;;
            esac ;;
    esac
    return 0
}

# Validate (cron rules) then quote. $1=value  $2=label. Prints the quoted word.
function cron_quote() {
    reject_unsafe_path "$1" "$2" cron || return 1
    shell_quote "$1"
}

# Fail fast when a required config file is missing or unreadable, instead of
# letting startup continue on silent defaults and fail later inside the JVM.
#   $1 = file   $2 = for error message
function ensure_file_readable() {
    local file="$1" description="$2"
    if [ ! -f "$file" ]; then
        echo "ERROR: missing $description: '$file'" >&2
        if [ -n "${CONF_OVERRIDE:-}" ]; then
            echo "       The conf dir passed with -c must contain the same files" >&2
            echo "       as the packaged conf directory." >&2
        fi
        return 1
    fi
    if [ ! -r "$file" ]; then
        echo "ERROR: unreadable $description: '$file'" >&2
        echo "       Check the file's ownership and permissions." >&2
        return 1
    fi
    return 0
}

# A relative path INSIDE a config file is resolved by the JVM against the
# working directory ($TOP), not against the directory the config file itself
# lives in. So when -c points at an external conf dir, a stock value such as
# './storage' still resolves under $TOP and the operator's copy is ignored
# with no message at all. Refuse to start instead, and say exactly what to change.
#   $1 = value read from the config   $2 = property name   $3 = config file
function reject_relative_conf_path() {
    local value="$1" property="$2" file="$3"
    # Only an overridden conf dir can disagree with the working directory.
    [ -z "${CONF_OVERRIDE:-}" ] && return 0
    [ -z "$value" ] && return 0
    case "$value" in
        /*) return 0 ;;
    esac
    local suggestion="${value#./}"
    echo "ERROR: '$property' is the relative path '$value' in $file." >&2
    echo "       Relative paths resolve against the working directory, so with" >&2
    echo "       -c '$CONF_OVERRIDE' this would silently keep using the packaged" >&2
    echo "       location instead of your copy. Set '$property' to an absolute" >&2
    echo "       path (for example '$TOP/$suggestion')." >&2
    return 1
}

function crontab_append() {
    local job="$1"
    crontab -l 2>/dev/null | grep -F -- "$job" >/dev/null 2>&1
    if [ $? -eq 0 ]; then
        return 1
    fi
    # printf, not echo: echo may interpret backslashes or swallow a leading -n/-e
    { crontab -l 2>/dev/null; printf '%s\n' "$job"; } | crontab -
}

function crontab_remove() {
    local job="$1"
    # check exist before remove
    crontab -l 2>/dev/null | grep -F -- "$job" >/dev/null 2>&1
    if [ $? -ne 0 ]; then
        return 0
    fi

    crontab -l 2>/dev/null | grep -Fv -- "$job" | crontab -

    # Check exist after remove
    crontab -l 2>/dev/null | grep -F -- "$job" >/dev/null 2>&1
    if [ $? -eq 0 ]; then
        return 1
    else
        return 0
    fi
}

# wait_for_startup friendly_name host port timeout_s
function wait_for_startup() {
    local pid="$1"
    local server_name="$2"
    local server_url="$3"
    local timeout_s="$4"

    local now_s
    now_s=$(date '+%s')
    local stop_s=$(( now_s + timeout_s ))

    local status

    echo -n "Connecting to $server_name ($server_url)"
    while [ "$now_s" -le "$stop_s" ]; do
        echo -n .
        process_status "$server_name" "$pid" >/dev/null
        if [ $? -eq 1 ]; then
            echo "Starting $server_name failed"
            return 1
        fi

        status=$(curl -o /dev/null -s -k -w '%{http_code}' "$server_url")
        if [[ "$status" -eq 200 || "$status" -eq 401 ]]; then
            echo "OK"
            echo "Started [pid $pid]"
            return 0
        fi
        sleep 2
        now_s=$(date '+%s')
    done

    echo "The operation timed out when attempting to connect to $server_url" >&2
    return 1
}

function free_memory() {
    local free=""
    local os=$(uname)
    if [ "$os" == "Linux" ]; then
        local mem_free=$(cat /proc/meminfo | grep -w "MemFree" | awk '{print $2}')
        local mem_buffer=$(cat /proc/meminfo | grep -w "Buffers" | awk '{print $2}')
        local mem_cached=$(cat /proc/meminfo | grep -w "Cached" | awk '{print $2}')
        if [[ "$mem_free" == "" || "$mem_buffer" == "" || "$mem_cached" == "" ]]; then
            echo "Failed to get free memory"
            exit 1
        fi
        free=$(expr "$mem_free" + "$mem_buffer" + "$mem_cached")
        free=$(expr "$free" / 1024)
    elif [ "$os" == "Darwin" ]; then
        local pages_free pages_inactive
        pages_free=$(vm_stat | awk '/Pages free/{print $0}' | awk -F'[:.]+' '{print $2}' | tr -d " ")
        pages_inactive=$(vm_stat | awk '/Pages inactive/{print $0}' | awk -F'[:.]+' '{print $2}' | tr -d " ")
        if [[ -z "$pages_free" || -z "$pages_inactive" ]]; then
            echo "Failed to get free memory"
            exit 1
        fi
        local pages_available
        pages_available=$(expr "$pages_free" + "$pages_inactive")
        free=$(expr "$pages_available" \* 4096 / 1024 / 1024)
    else
        echo "Unsupported operating system $os"
        exit 1
    fi
    echo $free
}

function calc_xmx() {
    local min_mem=$1
    local max_mem=$2
    # Get machine available memory
    local free=$(free_memory)
    local half_free=$((free/2))

    local xmx=$min_mem
    if [[ "$free" -lt "$min_mem" ]]; then
        exit 1
    elif [[ "$half_free" -ge "$max_mem" ]]; then
        xmx=$max_mem
    elif [[ "$half_free" -lt "$min_mem" ]]; then
        xmx=$min_mem
    else
        xmx=$half_free
    fi
    echo $xmx
}

function remove_with_prompt() {
    local path=$1
    local tips=""

    if [ -d "$path" ]; then
        tips="Remove directory '$path' and all sub files [y/n]?"
    elif [ -f "$path" ]; then
        tips="Remove file '$path' [y/n]?"
    else
        return 0
    fi

    read -p "$tips " yn
    case $yn in
        [Yy]* ) rm -rf "$path";;
        * ) ;;
    esac
}

function ensure_path_writable() {
    local path=$1
    # Ensure input path exist
    if [ ! -d "${path}" ]; then
        mkdir -p ${path}
    fi
    # Check for write permission
    if [ ! -w "${path}" ]; then
        echo "No write permission on directory ${path}"
        exit 1
    fi
}

# Canonicalize a (possibly relative) directory override into an absolute path.
#   $1 = path, $2 = mode (optional)
#   default    : create the directory if missing. -l and -o name directories the
#                scripts write into, so creating them is the expected setup step.
#   "nocreate" : resolve only. The directory is never created, and a missing one
#                is an error. Used for -c: a conf dir is only ever READ, so a
#                typo such as '-c /dat/conf' must fail here naming the wrong
#                directory, instead of silently creating an empty one and
#                failing later on a file missing inside it.
function canonicalize_dir() {
    local path="$1"
    local mode="${2:-create}"
    [ -z "$path" ] && return 0
    if [ "$mode" != "nocreate" ]; then
        mkdir -p "$path" 2>/dev/null
    elif [ ! -d "$path" ]; then
        echo "Error: no such directory '$path'" >&2
        exit 1
    fi
    (cd "$path" 2>/dev/null && pwd) || {
        echo "Error: cannot resolve path '$path'" >&2
        exit 1
    }
}

# Canonicalize a (possibly relative) file override (e.g. the pid file) into an
# absolute path.
#   $1 = path, $2 = mode (optional)
#   default    : create the parent directory if missing (start/restart scripts,
#                which have to be able to write the pid file afterwards).
#   "nocreate" : resolve only. The parent is never created, and a missing one
#                is an error. The stop scripts only ever READ the pid file, so
#                they must not leave an empty directory tree behind for a typo
#                such as '-i /var/run/hugegraf/store.pid'.
function canonicalize_file() {
    local path="$1"
    local mode="${2:-create}"
    [ -z "$path" ] && return 0
    local dir file abs_dir
    dir="$(dirname "$path")"
    file="$(basename "$path")"
    if [ "$mode" != "nocreate" ]; then
        mkdir -p "$dir" 2>/dev/null
    fi
    abs_dir="$(cd "$dir" 2>/dev/null && pwd)" || {
        echo "Error: cannot resolve path '$path'" >&2
        exit 1
    }
    echo "$abs_dir/$file"
}

function get_ip() {
    local os=$(uname)
    local loopback="127.0.0.1"
    local ip=""
    case $os in
        Linux)
            if command_available "ifconfig"; then
                ip=$(ifconfig | grep 'inet addr:' | grep -v "$loopback" | cut -d: -f2 | awk '{ print $1}')
            elif command_available "ip"; then
                ip=$(ip addr | grep 'state UP' -A2 | tail -n1 | awk '{print $2}' | awk -F"/" '{print $1}')
            else
                ip=$loopback
            fi
            ;;
        FreeBSD|OpenBSD|Darwin)
            if command_available "ifconfig"; then
                ip=$(ifconfig | grep -E 'inet.[0-9]' | grep -v "$loopback" | awk '{ print $2}')
            else
                ip=$loopback
            fi
            ;;
        SunOS)
            if command_available "ifconfig"; then
                ip=$(ifconfig -a | grep inet | grep -v "$loopback" | awk '{ print $2} ')
            else
                ip=$loopback
            fi
            ;;
        *) ip=$loopback;;
    esac
    [[ -z "$ip" ]] && ip=$loopback
    echo "$ip"
}

function download() {
    local path=$1
    local link_url=$2

    if [ ! -d "$path" ]; then
        mkdir -p "$path" || {
            echo "Failed to create directory: $path"
            exit 1
        }
    fi

    local filename tmp
    filename=$(basename "${link_url%%[?#]*}")
    local dest="${path}/${filename}"
    # mktemp, not $$: a PID is shared by concurrent background subshells.
    tmp=$(mktemp -- "${path}/.${filename}.XXXXXX") || {
        echo "Failed to create a temporary file in $path"
        return 1
    }

    if command_available "curl"; then
        # -o must appear before -- so it is parsed as an option, not an extra URL.
        if curl -fL -o "$tmp" -- "${link_url}"; then
            mv -f -- "$tmp" "$dest" || { rm -f -- "$tmp"; return 1; }
        else
            rm -f -- "$tmp"
            return 1
        fi
    elif command_available "wget"; then
        local -a progress_opt=()
        if wget --help 2>&1 | grep -q -- '--show-progress'; then
            progress_opt=(-q --show-progress)
        fi
        if wget ${progress_opt[@]+"${progress_opt[@]}"} -O "$tmp" -- "${link_url}"; then
            mv -f -- "$tmp" "$dest" || { rm -f -- "$tmp"; return 1; }
        else
            rm -f -- "$tmp"
            return 1
        fi
    else
        echo "Required wget or curl but they are unavailable"
        exit 1
    fi
}

download_and_verify() {
    local url=$1
    local filepath=$2
    local expected_md5=$3
    local actual_md5

    if [[ -f "$filepath" ]]; then
        echo "File $filepath exists. Verifying MD5 checksum..."
        actual_md5=$(md5sum -- "$filepath" | awk '{ print $1 }')
        if [[ "$actual_md5" != "$expected_md5" ]]; then
            echo "MD5 checksum verification failed for $filepath. Expected: $expected_md5, but got: $actual_md5"
            # Keep the shared destination until a verified temporary file is
            # ready; a stale reader must not delete another caller's replacement.
        else
            echo "MD5 checksum verification succeeded for $filepath."
            return 0
        fi
    fi

    echo "Downloading $filepath..."
    local tmp
    # mktemp, not $$: a PID is shared by concurrent background subshells.
    tmp=$(mktemp -- "${filepath}.XXXXXX") || return 1
    # jemalloc is optional: an unavailable or stalled mirror must not block
    # Store startup indefinitely. Bound both connection and complete transfer.
    if curl -fL --connect-timeout 5 --max-time 30 -o "$tmp" -- "$url"; then
        actual_md5=$(md5sum -- "$tmp" | awk '{ print $1 }')
        if [[ "$actual_md5" != "$expected_md5" ]]; then
            echo "MD5 checksum verification failed for $filepath after download. Expected: $expected_md5, but got: $actual_md5"
            rm -f -- "$tmp"
            return 1
        fi
        # A failed rename must not report success: callers export LD_PRELOAD
        # from $filepath and would otherwise use a library that never installed.
        mv -f -- "$tmp" "$filepath" || { rm -f -- "$tmp"; return 1; }
    else
        rm -f -- "$tmp"
        return 1
    fi

    return 0
}

function ensure_package_exist() {
    local path=$1
    local dir=$2
    local tar=$3
    local link=$4

    if [ ! -d "${path}/${dir}" ]; then
        if [ ! -f "${path}/${tar}" ]; then
            echo "Downloading the compressed package '${tar}'"
            if ! download "${path}" "${link}"; then
                echo "Failed to download, please ensure the network is available and link is valid"
                exit 1
            fi
            echo "[OK] Finished download"
        fi
        echo "Unzip the compressed package '$tar'"
        if ! tar -zxvf "${path}/${tar}" -C "${path}" >/dev/null 2>&1; then
            echo "Failed to unzip, please check the compressed package"
            exit 1
        fi
        echo "[OK] Finished unzip"
    fi
}

###########################################################################

function wait_for_shutdown() {
    local process_name="$1"
    local pid="$2"
    local timeout_s="$3"

    local now_s=$(date '+%s')
    local stop_s=$(( $now_s + $timeout_s ))

    echo -n "Killing $process_name(pid $pid)" >&2
    while [ $now_s -le $stop_s ]; do
        echo -n .
        process_status "$process_name" "$pid" >/dev/null
        if [ $? -eq 1 ]; then
            echo "OK"
            return 0
        fi
        sleep 2
        now_s=$(date '+%s')
    done
    echo "$process_name shutdown timeout(exceeded $timeout_s seconds)" >&2
    return 1
}

function process_status() {
    local process_name="$1"
    local pid="$2"

    ps -p "$pid"
    if [ $? -eq 0 ]; then
        echo "$process_name is running with pid $pid"
        return 0
    else
        echo "The process $process_name does not exist"
        return 1
    fi
}

function kill_process() {
    local process_name="$1"
    local pid="$2"

    if [ -z "$pid" ]; then
        echo "The process $pid does not exist"
        return 0
    fi

    case "$(uname)" in
        CYGWIN*) taskkill /F /PID "$pid" ;;
        *)       kill "$pid" ;;
    esac
}

function kill_process_and_wait() {
    local process_name="$1"
    local pid="$2"
    local timeout_s="$3"

    kill_process "$process_name" "$pid"
    wait_for_shutdown "$process_name" "$pid" "$timeout_s"
}
