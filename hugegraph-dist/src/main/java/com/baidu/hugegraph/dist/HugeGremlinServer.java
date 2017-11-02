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

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.server.GraphManager;
import org.apache.tinkerpop.gremlin.server.GremlinServer;
import org.apache.tinkerpop.gremlin.server.Settings;
import org.slf4j.Logger;

import com.baidu.hugegraph.HugeException;
import com.baidu.hugegraph.util.Log;

public class HugeGremlinServer {

    private static final Logger LOG = Log.logger(HugeGremlinServer.class);

    private static final String G_PREFIX = "__g_";

    public static void main(String[] args) throws Exception {

        if (args.length != 1) {
            String msg = "HugeGremlinServer can only accept one config files";
            LOG.error(msg);
            throw new HugeException(msg);
        }

        try {
            RegisterUtil.registerBackends();
            start(args[0]);
        } catch (Exception e) {
            LOG.error("HugeGremlinServer error:", e);
            throw e;
        }
        LOG.info("HugeGremlinServer stopped");
    }

    public static void start(String conf) throws Exception {
        // Start GremlinServer with inject traversal source
        startWithInjectTraversal(conf);
    }

    private static void startWithInjectTraversal(String conf)
                                                 throws Exception {
        LOG.info(GremlinServer.getHeader());
        final Settings settings;

        try {
            settings = Settings.read(conf);
        } catch (Exception ex) {
            LOG.error("Can't found the configuration file at {} or " +
                      "being parsed properly. [{}]", conf, ex.getMessage());
            return;
        }

        LOG.info("Configuring Gremlin Server from {}", conf);
        final GremlinServer server = new GremlinServer(settings);

        // Inject customized traversal source
        injectTraversalSource(server);

        server.start().exceptionally(t -> {
            LOG.error("Gremlin Server was unable to start and will " +
                      "shutdown now: {}", t.getMessage());
            server.stop().join();
            return null;
        }).join();
    }

    private static void injectTraversalSource(GremlinServer server) {
        GraphManager graphManager = server.getServerGremlinExecutor()
                                          .getGraphManager();
        for (String graph : graphManager.getGraphNames()) {
            GraphTraversalSource g = graphManager.getGraph(graph).traversal();
            String gName = G_PREFIX + graph;
            if (graphManager.getTraversalSource(gName) != null) {
                throw new HugeException(
                          "Found existing name '%s' in global bindings, " +
                          "it may lead to gremlin query error.", gName);
            }
            // Add a traversal source for all graphs with customed rule.
            graphManager.putTraversalSource(gName, g);
        }
    }
}
