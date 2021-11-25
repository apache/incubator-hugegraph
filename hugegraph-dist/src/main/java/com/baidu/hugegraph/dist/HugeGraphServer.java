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
import org.slf4j.Logger;

import com.baidu.hugegraph.HugeException;
import com.baidu.hugegraph.HugeFactory;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.config.ServerOptions;
import com.baidu.hugegraph.event.EventHub;
import com.baidu.hugegraph.server.RestServer;
import com.baidu.hugegraph.util.ConfigUtil;
import com.baidu.hugegraph.util.Log;

public class HugeGraphServer {

    private static final Logger LOG = Log.logger(HugeGraphServer.class);

    private final GremlinServer gremlinServer;
    private final RestServer restServer;

    public static void register() {
        RegisterUtil.registerBackends();
        RegisterUtil.registerPlugins();
        RegisterUtil.registerServer();
    }

    public HugeGraphServer(String gremlinServerConf, String restServerConf)
                           throws Exception {
        // Only switch on security manager after HugeGremlinServer started
        SecurityManager securityManager = System.getSecurityManager();
        System.setSecurityManager(null);
        HugeConfig restServerConfig = new HugeConfig(restServerConf);
        int threads = restServerConfig.get(
                      ServerOptions.SERVER_EVENT_HUB_THREADS);
        EventHub hub = new EventHub("gremlin=>hub<=rest", threads);
        ConfigUtil.checkGremlinConfig(gremlinServerConf);

        String graphsDir = null;
        if (restServerConfig.get(ServerOptions.GRAPH_LOAD_FROM_LOCAL_CONFIG)) {
            graphsDir = restServerConfig.get(ServerOptions.GRAPHS);
        }

        try {
            // Start GremlinServer
            this.gremlinServer = HugeGremlinServer.start(gremlinServerConf,
                                                         graphsDir, hub);
        } catch (Throwable e) {
            LOG.error("HugeGremlinServer start error: ", e);
            HugeFactory.shutdown(30L);
            throw e;
        } finally {
            System.setSecurityManager(securityManager);
        }

        try {
            // Start HugeRestServer
            this.restServer = HugeRestServer.start(restServerConf, hub);
        } catch (Throwable e) {
            LOG.error("HugeRestServer start error: ", e);
            try {
                this.gremlinServer.stop().get();
            } catch (Throwable t) {
                LOG.error("GremlinServer stop error: ", t);
            }
            HugeFactory.shutdown(30L);
            throw e;
        }
    }

    public void stop() {
        try {
            this.restServer.shutdown().get();
            LOG.info("HugeRestServer stopped");
        } catch (Throwable e) {
            LOG.error("HugeRestServer stop error: ", e);
        }

        
        try {
            this.gremlinServer.stop().get();
            LOG.info("HugeGremlinServer stopped");
        } catch (Throwable e) {
            LOG.error("HugeGremlinServer stop error: ", e);
        }

        try {
            HugeFactory.shutdown(30L);
            LOG.info("HugeGraph stopped");
        } catch (Throwable e) {
            LOG.error("Failed to stop HugeGraph: ", e);
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            String msg = "Start HugeGraphServer need to pass 2 parameters, " +
                         "they are the config files of GremlinServer and " +
                         "RestServer, for example: conf/gremlin-server.yaml " +
                         "conf/rest-server.properties";
            LOG.error(msg);
            throw new HugeException(msg);
        }

        HugeGraphServer.register();

        HugeGraphServer server = new HugeGraphServer(args[0], args[1]);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("HugeGraphServer stopping");
            server.stop();
            LOG.info("HugeGraphServer stopped");
        }, "hugegraph-server-shutdown"));
    }
}
