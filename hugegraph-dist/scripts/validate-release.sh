#!/usr/bin/env bash
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
# This script is used to validate the release package, including:
# 1. Check the release package name & content
# 2. Check the release package sha512 & gpg signature
# 3. Compile the source package & run server & toolchain
# 4. Run server & toolchain in binary package

URL_PREFIX="https://dist.apache.org/repos/dist/dev/incubator/hugegraph/"
# release version (input by committer)
RELEASE_VERSION=$1
JAVA_VERSION=$2
# git release branch (check it carefully)
#GIT_BRANCH="release-${RELEASE_VERSION}"

RELEASE_VERSION=${RELEASE_VERSION:?"Please input the release version behind script"}

# step1: download svn files
rm -rf dist/"$RELEASE_VERSION" && svn co ${URL_PREFIX}/"$RELEASE_VERSION" dist/"$RELEASE_VERSION"
cd dist/"$RELEASE_VERSION" || exit

# step2: check environment & import public keys
shasum --version 1>/dev/null || exit
gpg --version 1>/dev/null || exit

wget https://downloads.apache.org/incubator/hugegraph/KEYS || exit
gpg --import KEYS
# TODO: how to trust all public keys in gpg list, currently only trust the first one
gpg --list-keys --with-colons | grep pub | cut -d: -f5 | xargs -I {} gpg --edit-key {} trust quit
for key in $(gpg --list-keys --with-colons | awk -F: '/^pub/ {print $5}'); do
  gpg --edit-key "$key" trust quit
done

# step3: check sha512 & gpg signature
for i in *.tar.gz; do
  echo "$i"
  shasum -a 512 --check "$i".sha512 || exit
  eval gpg "${GPG_OPT}" --verify "$i".asc "$i" || exit
done

# step4: validate source packages
ls -lh ./*.tar.gz
for i in *src.tar.gz; do
  echo "$i"
  #### step4.0: check the directory name include "incubating"
  if [[ ! "$i" =~ "incubating" ]]; then
    echo "The package name should include incubating" && exit 1
  fi
  tar xzvf "$i" || exit
  cd "$(basename "$i" .tar.gz)" || exit

  #### step4.1: check the directory include "NOTICE" and "LICENSE" file
  if [[ ! -f "LICENSE" ]]; then
    echo "The package should include LICENSE file" && exit 1
  fi
  if [[ ! -f "NOTICE" ]]; then
    echo "The package should include NOTICE file" && exit 1
  fi
  # ensure doesn't contains empty directory or file
  COUNT=$(find . -type d -empty | wc -l)
  if [[ $COUNT -ne 0 ]]; then
    find . -type d -empty
    echo "The package should not include empty directory, but get $COUNT" # TODO: && exit 1
  fi

  #### step4.2: compile the packages
  if [[ $JAVA_VERSION == 8 && "$i" =~ "computer" ]]; then
    cd .. && echo "skip computer module in java8"
    continue
  fi
  mvn package -DskipTests -ntp && ls -lh
  cd .. || exit
done

#### step4.3: run the compiled packages in server
ls -lh
cd ./*hugegraph-incubating*src/*hugegraph*"${RELEASE_VERSION}" || exit
bin/init-store.sh && sleep 1
bin/start-hugegraph.sh && ls ../../
cd ../../ || exit

#### step4.4: run the compiled packages in toolchain (include loader/tool/hubble)
cd ./*toolchain*src || exit
ls -lh
cd ./*toolchain*"${RELEASE_VERSION}" || exit
ls -lh

##### 4.4.1 test loader
cd ./*loader*"${RELEASE_VERSION}" || exit
bin/hugegraph-loader.sh -f ./example/file/struct.json -s ./example/file/schema.groovy \
  -g hugegraph || exit
cd .. || exit

##### 4.4.2 test tool
cd ./*tool*"${RELEASE_VERSION}" || exit
bin/hugegraph gremlin-execute --script 'g.V().count()' || exit
bin/hugegraph task-list || exit
bin/hugegraph backup -t all --directory ./backup-test || exit
cd .. || exit

##### 4.4.3 test hubble
cd ./*hubble*"${RELEASE_VERSION}" || exit
# TODO: add hubble doc & test it
cat conf/hugegraph-hubble.properties && bin/start-hubble.sh
cd ../../../ || exit

# step5: validate the binary packages
#### step5.0: check the directory name include "incubating"
#### step5.1: check the directory include "NOTICE" and "LICENSE" file
rm -rf ./*src* && ls -lh
for i in *.tar.gz; do
  echo "$i"
  if [[ ! "$i" =~ "incubating" ]]; then
    echo "The package name should include incubating" && exit 1
  fi
  tar xzvf "$i" || exit

  cd "$(basename "$i" .tar.gz)" && ls -lh || exit
  if [[ ! -f "LICENSE" ]]; then
    echo "The package should include LICENSE file" && exit 1
  fi
  if [[ ! -f "NOTICE" ]]; then
    echo "The package should include NOTICE file" && exit 1
  fi
  # ensure doesn't contains empty directory or file
  COUNT=$(find . -type d -empty | wc -l)
  if [[ $COUNT -ne 0 ]]; then
    find . -type d -empty
    echo "The package should not include empty directory, but get $COUNT" # TODO: && exit 1
  fi
  cd - || exit
done

#### step5.2: start the server
cd ./*hugegraph-incubating*"${RELEASE_VERSION}" || exit
bin/init-store.sh && sleep 1
# kill the HugeGraphServer process by jps
jps | grep HugeGraphServer | awk '{print $1}' | xargs kill -9
bin/start-hugegraph.sh && ls ../
cd - || exit

#### step5.2: running toolchain
cd ./*toolchain*"${RELEASE_VERSION}" || exit
ls -lh

##### 5.2.1 test loader
cd ./*loader*"${RELEASE_VERSION}" || exit
bin/hugegraph-loader.sh -f ./example/file/struct.json -s ./example/file/schema.groovy \
  -g hugegraph || exit
cd - || exit

##### 5.2.2 test tool
cd ./*tool*"${RELEASE_VERSION}" || exit
bin/hugegraph gremlin-execute --script 'g.V().count()' || exit
bin/hugegraph task-list || exit
bin/hugegraph backup -t all --directory ./backup-test || exit
cd - || exit

##### 5.2.3 test hubble
cd ./*hubble*"${RELEASE_VERSION}" || exit
# TODO: add hubble doc & test it
cat conf/hugegraph-hubble.properties
bin/stop-hubble.sh && bin/start-hubble.sh
cd - || exit

echo "Finish validate, please check all steps manually again!"
