#!/bin/bash

readonly REPO_URL=http://10.14.139.8:8081/artifactory/star-snapshot

mvn --settings ../settings.xml -Dmaven.test.skip=true -DaltDeploymentRepository=star-snapshot::default::${REPO_URL} clean deploy

