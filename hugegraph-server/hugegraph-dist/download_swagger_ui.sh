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
VERSION=4.15.5

curl --version >/dev/null 2>&1 ||
  {
    echo 'ERROR: Please install `curl` first if you need `swagger-ui`'
    exit
  }

# TODO: perhaps it's necessary verify the checksum before reusing the existing tar
if [[ ! -f v$VERSION.tar.gz ]]; then
  curl -s -S -L -o v$VERSION.tar.gz \
    https://github.com/swagger-api/swagger-ui/archive/refs/tags/v$VERSION.tar.gz ||
    {
      echo 'ERROR: Download `swagger-ui` failed, please check your network connection'
      exit
    }
fi

tar zxf v$VERSION.tar.gz -C . >/dev/null 2>&1

echo "window.onload = function() { window.ui = SwaggerUIBundle({
url:'/openapi.json',dom_id:'#swagger-ui',deepLinking:true,layout:'StandaloneLayout',
presets:[SwaggerUIBundle.presets.apis, SwaggerUIStandalonePreset ],
plugins:[SwaggerUIBundle.plugins.DownloadUrl]});};" > \
  swagger-ui-$VERSION/dist/swagger-initializer.js

# conceal the VERSION from the outside
mv swagger-ui-$VERSION swagger-ui
echo 'INFO: Successfully download `swagger-ui`'
