#!/bin/bash
VERSION=4.15.5
curl --version >/dev/null 2>&1 ||
  {
    echo 'ERROR: Please install `curl` first if you need `swagger-ui`'
    exit
  }
# TODO: perhaps it's necessary verify the checksum before reusing the existing tar
if [ ! -f v$VERSION.tar.gz ]; then
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
