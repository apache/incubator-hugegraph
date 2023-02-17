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
USER=$3
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
echo -e "5\ny\n" | gpg --batch --command-fd 0 --edit-key $USER trust

echo "trust all pk"
for key in $(gpg --no-tty --list-keys --with-colons | awk -F: '/^pub/ {print $5}'); do
  echo -e "5\ny\n" | gpg --batch --command-fd 0 --edit-key "$key" trust
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
    echo "The package name $i should include incubating" && exit 1
  fi
  tar xzvf "$i" || exit
  cd "$(basename "$i" .tar.gz)" || exit

  #### step4.1: check the directory include "NOTICE" and "LICENSE" file and "DISCLAIMER" file
  if [[ ! -f "LICENSE" ]]; then
    echo "The package $i should include LICENSE file" && exit 1
  fi
  if [[ ! -f "NOTICE" ]]; then
    echo "The package $i should include NOTICE file" && exit 1
  fi
  if [[ ! -f "DISCLAIMER" ]]; then
    echo "The package $i should include DISCLAIMER file" && exit 1
  fi
  # step4.2: ensure doesn't contains *GPL/BCL/JSR-275/RSAL/QPL/SSPL/CPOL/NPL1.*/CC-BY
  COUNT=$(grep -E "GPL|BCL|JSR-275|RSAL|QPL|SSPL|CPOL|NPL1|CC-BY" LICENSE NOTICE | wc -l)
  if [[ $COUNT -ne 0 ]]; then
     grep -E "GPL|BCL|JSR-275|RSAL|QPL|SSPL|CPOL|NPL1.0|CC-BY" LICENSE NOTICE
     echo "The package $i shouldn't include GPL* invalid dependency, but get $COUNT" && exit 1
  fi
  # step4.3: ensure doesn't contains empty directory or file
  COUNT=$(find . -type d -empty | wc -l)
  if [[ $COUNT -ne 0 ]]; then
    find . -type d -empty
    echo "The package $i should not include empty directory, but get $COUNT" # TODO: && exit 1
  fi
  # step4.4: ensure any file should less than 900kb & not include binary file
  COUNT=$(find . -type f -size +900k | wc -l)
  if [[ $COUNT -ne 0 ]]; then
    find . -type f -size +900k
    echo "The package $i shouldn't include file larger than 900kb, but get $COUNT" && exit 1
  fi
  COUNT=$(find . -type f | perl -lne 'print if -B' | grep -v *.txt | wc -l)
  if [[ $COUNT -ne 0 ]]; then
    find . -type f | perl -lne 'print if -B'
    echo "The package $i shouldn't include binary file, but get $COUNT"
  fi

  #### step4.5: test compile the packages
  if [[ $JAVA_VERSION == 8 && "$i" =~ "computer" ]]; then
    cd .. && echo "skip computer module in java8"
    continue
  fi
  mvn package -DskipTests -ntp && ls -lh
  cd .. || exit
done

#### step5: run the compiled packages in server
ls -lh
cd ./*hugegraph-incubating*src/*hugegraph*"${RELEASE_VERSION}" || exit
bin/init-store.sh && sleep 1
bin/start-hugegraph.sh && ls ../../
cd ../../ || exit

#### step6: run the compiled packages in toolchain (include loader/tool/hubble)
cd ./*toolchain*src || exit
ls -lh
cd ./*toolchain*"${RELEASE_VERSION}" || exit
ls -lh

##### step6.1: test loader
cd ./*loader*"${RELEASE_VERSION}" || exit
bin/hugegraph-loader.sh -f ./example/file/struct.json -s ./example/file/schema.groovy \
  -g hugegraph || exit
cd .. || exit

##### step6.2: test tool
cd ./*tool*"${RELEASE_VERSION}" || exit
bin/hugegraph gremlin-execute --script 'g.V().count()' || exit
bin/hugegraph task-list || exit
bin/hugegraph backup -t all --directory ./backup-test || exit
cd .. || exit

##### step6.3: test hubble
cd ./*hubble*"${RELEASE_VERSION}" || exit
# TODO: add hubble doc & test it
cat conf/hugegraph-hubble.properties && bin/start-hubble.sh
cd ../../../ || exit

# step7: validate the binary packages
rm -rf ./*src* && ls -lh
for i in *.tar.gz; do
  echo "$i"
  #### step7.1: check the directory name include "incubating"
  if [[ ! "$i" =~ "incubating" ]]; then
    echo "The package name $i should include incubating" && exit 1
  fi
  tar xzvf "$i" || exit

  #### step7.2: check root dir include "NOTICE"/"LICENSE"/"DISCLAIMER" files & "licenses" dir
  cd "$(basename "$i" .tar.gz)" && ls -lh || exit
  if [[ ! -f "LICENSE" ]]; then
    echo "The package $i should include LICENSE file" && exit 1
  fi
  if [[ ! -f "NOTICE" ]]; then
    echo "The package $i should include NOTICE file" && exit 1
  fi
  if [[ ! -f "DISCLAIMER" ]]; then
    echo "The package $i should include DISCLAIMER file" && exit 1
  fi
  if [[ ! -d "licenses" ]]; then
    echo "The package $i should include licenses dir" && exit 1
  fi
  #### step7.3: ensure doesn't contains *GPL/BCL/JSR-275/RSAL/QPL/SSPL/CPOL/NPL1.*/CC-BY
  COUNT=$(grep -r -E "GPL|BCL|JSR-275|RSAL|QPL|SSPL|CPOL|NPL1|CC-BY" LICENSE NOTICE licenses | wc -l)
  if [[ $COUNT -ne 0 ]]; then
    grep -r -E "GPL|BCL|JSR-275|RSAL|QPL|SSPL|CPQL|NPL1|CC-BY" LICENSE NOTICE licenses
    echo "The package $i shouldn't include GPL* invalid dependency, but get $COUNT" && exit 1
  fi
  #### step7.4: ensure doesn't contains empty directory or file
  COUNT=$(find . -type d -empty | wc -l)
  if [[ $COUNT -ne 0 ]]; then
    find . -type d -empty
    echo "The package $i should not include empty directory, but get $COUNT" # TODO: && exit 1
  fi
  cd - || exit
done

#### step8: start the server
cd ./*hugegraph-incubating*"${RELEASE_VERSION}" || exit
bin/init-store.sh && sleep 1
# kill the HugeGraphServer process by jps
jps | grep HugeGraphServer | awk '{print $1}' | xargs kill -9
bin/start-hugegraph.sh && ls ../
cd - || exit

#### step9: running toolchain
cd ./*toolchain*"${RELEASE_VERSION}" || exit
ls -lh
##### step9.1: test loader
cd ./*loader*"${RELEASE_VERSION}" || exit
bin/hugegraph-loader.sh -f ./example/file/struct.json -s ./example/file/schema.groovy \
  -g hugegraph || exit
cd - || exit

##### step9.2: test tool
cd ./*tool*"${RELEASE_VERSION}" || exit
bin/hugegraph gremlin-execute --script 'g.V().count()' || exit
bin/hugegraph task-list || exit
bin/hugegraph backup -t all --directory ./backup-test || exit
cd - || exit

##### step9.3: test hubble
cd ./*hubble*"${RELEASE_VERSION}" || exit
# TODO: add hubble doc & test it
cat conf/hugegraph-hubble.properties
bin/stop-hubble.sh && bin/start-hubble.sh
cd - || exit

echo "Finish validate, please check all steps manually again!"
