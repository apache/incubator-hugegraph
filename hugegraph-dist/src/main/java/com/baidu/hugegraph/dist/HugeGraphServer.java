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

import com.baidu.hugegraph.auth.StandardAuthenticator;
import com.baidu.hugegraph.meta.MetaManager;
import com.baidu.hugegraph.util.E;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.tinkerpop.gremlin.server.GremlinServer;
import org.slf4j.Logger;

import com.baidu.hugegraph.HugeException;
import com.baidu.hugegraph.HugeFactory;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.config.ServerOptions;
import com.baidu.hugegraph.event.EventHub;
import com.baidu.hugegraph.server.RestServer;
import com.baidu.hugegraph.task.TaskManager;
import com.baidu.hugegraph.util.ConfigUtil;
import com.baidu.hugegraph.util.Log;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class HugeGraphServer {

    private static final Logger LOG = Log.logger(HugeGraphServer.class);

    private final GremlinServer gremlinServer;
    private final RestServer restServer;
    private final MetaManager metaManager = MetaManager.instance();

    public static void register() {
        RegisterUtil.registerBackends();
        RegisterUtil.registerPlugins();
        RegisterUtil.registerServer();
    }

    public HugeGraphServer(String gremlinServerConf, String restServerConf,
                           String graphSpace, String serviceId,
                           String nodeId, String nodeRole,
                           List<String> metaEndpoints, String cluster,
                           String pdPeers, Boolean withCa, String caFile,
                           String clientCaFile, String clientKeyFile)
                           throws Exception {
        // Only switch on security manager after HugeGremlinServer started
        SecurityManager securityManager = System.getSecurityManager();
        System.setSecurityManager(null);

        E.checkArgument(metaEndpoints.size() > 0,
                        "The meta endpoints could not be null");
        E.checkArgument(StringUtils.isNotEmpty(cluster),
                        "The cluster could not be null");
        if (StringUtils.isEmpty(graphSpace)) {
            LOG.info("Start service with 'DEFAULT' graph space");
            graphSpace = "DEFAULT";
        }
        if (StringUtils.isEmpty(serviceId)) {
            LOG.info("Start service with 'DEFAULT' graph space");
            serviceId = "DEFAULT";
        }

        // try to fetch rest server config and gremlin config from etcd
        if (!withCa) {
            caFile = null;
            clientCaFile = null;
            clientKeyFile = null;
        }
        this.metaManager.connect(cluster, MetaManager.MetaDriverType.ETCD,
                                 caFile, clientCaFile, clientKeyFile,
                                 metaEndpoints);

        HugeConfig restServerConfig;
        Map<String, Object> restMap = this.metaManager.restProperties(graphSpace, serviceId);
        if (restMap == null || restMap.isEmpty()) {
            restServerConfig = new HugeConfig(restServerConf);
        } else {
            restServerConfig = new HugeConfig(restMap);
        }
        if (StringUtils.isEmpty(restServerConfig.get(ServerOptions.AUTHENTICATOR))) {
            restServerConfig.addProperty(ServerOptions.AUTHENTICATOR.name(),
                            "com.baidu.hugegraph.auth.StandardAuthenticator");
        }
        restServerConfig.addProperty(ServerOptions.SERVICE_ID.name(),
                                     serviceId);
        restServerConfig.addProperty(ServerOptions.SERVICE_GRAPH_SPACE.name(),
                                     graphSpace);
        restServerConfig.addProperty(ServerOptions.NODE_ID.name(), nodeId);
        restServerConfig.addProperty(ServerOptions.NODE_ROLE.name(), nodeRole);
        restServerConfig.addProperty(ServerOptions.META_USE_CA.name(), withCa.toString());
        if (withCa) {
            restServerConfig.addProperty(ServerOptions.META_CA.name(), caFile);
            restServerConfig.addProperty(ServerOptions.META_CLIENT_CA.name(),
                                         clientCaFile);
            restServerConfig.addProperty(ServerOptions.META_CLIENT_KEY.name(),
                                         clientKeyFile);
        }
        String metaPDPeers = this.metaManager.hstorePDPeers();
        if (StringUtils.isNotEmpty(metaPDPeers)) {
            restServerConfig.addProperty(ServerOptions.PD_PEERS.name(),
                                         metaPDPeers);
        } else {
            restServerConfig.addProperty(ServerOptions.PD_PEERS.name(),
                                         pdPeers);
        }
        int threads = restServerConfig.get(
                      ServerOptions.SERVER_EVENT_HUB_THREADS);
        EventHub hub = new EventHub("gremlin=>hub<=rest", threads);
        ConfigUtil.checkGremlinConfig(gremlinServerConf);

        String graphsDir = null;
        if (restServerConfig.get(ServerOptions.GRAPH_LOAD_FROM_LOCAL_CONFIG)) {
            graphsDir = restServerConfig.get(ServerOptions.GRAPHS);
        }

        TaskManager.instance(restServerConfig.get(ServerOptions.TASK_THREADS));
        StandardAuthenticator.initAdminUserIfNeeded(restServerConfig,
                             metaEndpoints, cluster, withCa, caFile,
                             clientCaFile, clientKeyFile);
        try {
            // Start GremlinServer
            String gsText = this.metaManager.gremlinYaml(graphSpace,
                                                         serviceId);
            if (StringUtils.isEmpty(gsText)) {
                this.gremlinServer = HugeGremlinServer.start(gremlinServerConf,
                                                             graphsDir, hub);
            } else {
                InputStream is = IOUtils.toInputStream(gsText,
                                                       StandardCharsets.UTF_8);
                this.gremlinServer = HugeGremlinServer.start(is, graphsDir,
                                                             hub);
            }
        } catch (Throwable e) {
            LOG.error("HugeGremlinServer start error: ", e);
            HugeFactory.shutdown(30L);
            throw e;
        } finally {
            System.setSecurityManager(securityManager);
        }

        try {
            // Start HugeRestServer
            this.restServer = HugeRestServer.start(restServerConfig, hub);
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
        if (args.length < 13) {
            String msg = "Start HugeGraphServer need to pass parameter's " +
                         "length >= 13, they are the config files of " +
                         "GremlinServer and RestServer, for example: " +
                         "conf/gremlin-server.yaml " +
                         "conf/rest-server.properties ......";
            LOG.error(msg);
            throw new HugeException(msg);
        }

        HugeGraphServer.register();

        List<String> metaEndpoints = Arrays.asList(args[6].split(","));
        Boolean withCa = args[9].equals("true") ? true : false;
        HugeGraphServer server = new HugeGraphServer(args[0], args[1],
                                                     args[2], args[3],
                                                     args[4], args[5],
                                                     metaEndpoints, args[7],
                                                     args[8], withCa,
                                                     args[10], args[11],
                                                     args[12]);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("HugeGraphServer stopping");
            server.stop();
            LOG.info("HugeGraphServer stopped");
        }, "hugegraph-server-shutdown"));
    }
}
