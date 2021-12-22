/*
 * Copyright 2017 HugeGraph Authors
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package com.baidu.hugegraph.dist;

import org.apache.tinkerpop.gremlin.server.GremlinServer;
import org.apache.tinkerpop.gremlin.server.Settings;
import org.slf4j.Logger;

import com.baidu.hugegraph.HugeException;
import com.baidu.hugegraph.auth.ContextGremlinServer;
import com.baidu.hugegraph.event.EventHub;
import com.baidu.hugegraph.util.ConfigUtil;
import com.baidu.hugegraph.util.Log;

public class HugeGremlinServer {

    private static final Logger LOG = Log.logger(HugeGremlinServer.class);

    private static final String G_PREFIX = "__g_";

    public static GremlinServer start(String conf, String graphsDir,
                                      EventHub hub) throws Exception {
        // Start GremlinServer with inject traversal source
        LOG.info(GremlinServer.getHeader());
        final Settings settings;
        try {
            settings = Settings.read(conf);
        } catch (Exception e) {
            LOG.error("Can't found the configuration file at {} or " +
                      "being parsed properly. [{}]", conf, e.getMessage());
            throw e;
        }
        // Scan and merge graph confs
        settings.graphs.putAll(ConfigUtil.mergeGraphConfs(conf, graphsDir));

        LOG.info("Configuring Gremlin Server from {}", conf);
        ContextGremlinServer server = new ContextGremlinServer(settings, hub);

        // Inject customized traversal source
        server.injectTraversalSource(G_PREFIX);

        server.start().exceptionally(t -> {
            LOG.error("Gremlin Server was unable to start and will " +
                      "shutdown now: {}", t.getMessage());
            server.stop().join();
            throw new HugeException("Failed to start Gremlin Server");
        }).join();

        return server;
    }
}
