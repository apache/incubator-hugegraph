wget --version 1>/dev/null || exit
                                    wget https://github.com/swagger-api/swagger-ui/archive/refs/tags/v4.15.5.tar.gz
                                    tar zxvf v4.15.5.tar.gz
                                    echo "window.onload = function() { window.ui = SwaggerUIBundle({
                                    url:'/openapi.json',dom_id:'#swagger-ui',deepLinking:true,layout:'StandaloneLayout',
                                    presets:[SwaggerUIBundle.presets.apis, SwaggerUIStandalonePreset ],
                                    plugins:[SwaggerUIBundle.plugins.DownloadUrl]});};" > swagger-ui-4.15.5/dist/swagger-initializer.js
                                    cp -r swagger-ui-4.15.5/dist ../apache-hugegraph-incubating-1.0.0/swagger-ui