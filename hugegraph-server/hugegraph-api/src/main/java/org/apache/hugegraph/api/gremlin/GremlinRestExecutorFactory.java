/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hugegraph.api.gremlin;

import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.nio.file.Path;

import org.apache.hugegraph.auth.StandardAuthenticator;
import org.apache.hugegraph.auth.WsAndHttpBasicAuthHandler;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.config.ServerOptions;
import org.apache.hugegraph.server.HugeGraphWsAndHttpChannelizer;
import org.apache.hugegraph.util.E;
import org.apache.tinkerpop.gremlin.server.GremlinServer;
import org.apache.tinkerpop.gremlin.server.Settings;
import org.apache.tinkerpop.gremlin.server.auth.AllowAllAuthenticator;

public final class GremlinRestExecutorFactory {

    private GremlinRestExecutorFactory() {
    }

    public static GremlinRestExecutor standalone(HugeConfig config) {
        E.checkArgument("forward".equals(config.get(ServerOptions.GREMLIN_REST_EXECUTION_MODE)),
                        "embedded requires startup through HugeGraphServer");
        return new ForwardGremlinRestExecutor(config);
    }

    public static GremlinRestExecutor create(HugeConfig config, GremlinServer server,
                                             String restConfigFile) throws IOException {
        if ("forward".equals(config.get(ServerOptions.GREMLIN_REST_EXECUTION_MODE))) {
            return new ForwardGremlinRestExecutor(config);
        }
        Settings settings = server.getServerGremlinExecutor().getSettings();
        URI target = URI.create(config.get(ServerOptions.GREMLIN_SERVER_URL));
        E.checkArgument("http".equals(target.getScheme()) && target.getHost() != null &&
                        target.getUserInfo() == null && target.getFragment() == null &&
                        InetAddress.getByName(target.getHost()).isLoopbackAddress() &&
                        target.getPort() == settings.port &&
                        (InetAddress.getByName(settings.host).isAnyLocalAddress() ||
                         InetAddress.getByName(settings.host).equals(
                                 InetAddress.getByName(target.getHost()))),
                        "embedded requires gremlinserver.url to target this local Gremlin Server");
        E.checkArgument(settings.ssl == null || !settings.ssl.enabled,
                        "embedded does not support a TLS Gremlin listener");
        E.checkArgument(HugeGraphWsAndHttpChannelizer.class.getName().equals(settings.channelizer),
                        "embedded requires HugeGraphWsAndHttpChannelizer");
        E.checkArgument(settings.authorization == null ||
                        settings.authorization.authorizer == null,
                        "embedded does not support a custom Gremlin authorizer");
        String restAuth = config.get(ServerOptions.AUTHENTICATOR);
        String gremlinAuth = settings.authentication.authenticator;
        if (restAuth.isEmpty()) {
            E.checkArgument(AllowAllAuthenticator.class.getName().equals(gremlinAuth),
                            "REST and Gremlin authentication must agree for embedded execution");
        } else {
            E.checkArgument(StandardAuthenticator.class.getName().equals(restAuth) &&
                            restAuth.equals(gremlinAuth) &&
                            WsAndHttpBasicAuthHandler.class.getName().equals(
                                    settings.authentication.authenticationHandler),
                            "embedded only supports matching standard HugeGraph authentication");
            Object tokens = settings.authentication.config == null ? null :
                            settings.authentication.config.get("tokens");
            E.checkArgument(tokens instanceof String &&
                            Path.of(restConfigFile).toRealPath().equals(
                                    Path.of((String) tokens).toRealPath()),
                            "embedded requires the same REST and Gremlin authentication config");
        }
        return new EmbeddedGremlinRestExecutor(config, server.getServerGremlinExecutor());
    }

    public static void initialize(GremlinRestExecutor executor, GremlinServer server) {
        if (executor instanceof EmbeddedGremlinRestExecutor) {
            HugeGraphWsAndHttpChannelizer channelizer =
                    (HugeGraphWsAndHttpChannelizer) server.getChannelizer();
            ((EmbeddedGremlinRestExecutor) executor).initialize(channelizer.jsonSerializer());
        }
    }
}
