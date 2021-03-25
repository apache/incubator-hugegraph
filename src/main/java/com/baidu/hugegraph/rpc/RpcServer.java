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

package com.baidu.hugegraph.rpc;

import java.util.Map;

import org.apache.commons.collections.MapUtils;
import org.slf4j.Logger;

import com.alipay.remoting.RemotingServer;
import com.alipay.sofa.rpc.common.utils.StringUtils;
import com.alipay.sofa.rpc.config.ProviderConfig;
import com.alipay.sofa.rpc.config.ServerConfig;
import com.alipay.sofa.rpc.server.Server;
import com.alipay.sofa.rpc.server.bolt.BoltServer;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.config.RpcOptions;
import com.baidu.hugegraph.testutil.Whitebox;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.Log;

public class RpcServer {

    private static final Logger LOG = Log.logger(RpcServer.class);

    private final HugeConfig conf;
    private final RpcProviderConfig configs;
    private final ServerConfig serverConfig;

    public RpcServer(HugeConfig config) {
        RpcCommonConfig.initRpcConfigs(config);
        this.conf = config;
        this.configs = new RpcProviderConfig();

        String host = config.get(RpcOptions.RPC_SERVER_HOST);
        if (StringUtils.isNotBlank(host)) {
            int port = config.get(RpcOptions.RPC_SERVER_PORT);
            boolean adaptivePort = config.get(RpcOptions.RPC_ADAPTIVE_PORT);
            this.serverConfig = new ServerConfig();
            this.serverConfig.setProtocol(config.get(RpcOptions.RPC_PROTOCOL))
                             .setHost(host).setPort(port)
                             .setAdaptivePort(adaptivePort)
                             .setDaemon(false);
        } else {
            this.serverConfig = null;
        }
    }

    public boolean enabled() {
        return this.serverConfig != null;
    }

    public RpcProviderConfig config() {
        this.checkEnabled();
        return this.configs;
    }

    public String host() {
        this.checkEnabled();
        return this.serverConfig.getBoundHost();
    }

    public int port() {
        this.checkEnabled();
        Server server = this.serverConfig.getServer();
        if (server instanceof BoltServer && server.isStarted()) {
            /*
             * When using random port 0, try to fetch the actual port
             * NOTE: RemotingServer.port() would return the actual port only
             *       if sofa-bolt version >= 1.6.1, please see:
             *       https://github.com/sofastack/sofa-bolt/issues/196
             * TODO: remove this code after adding Server.port() interface:
             *       https://github.com/sofastack/sofa-rpc/issues/1022
             */
            RemotingServer rs = Whitebox.getInternalState(server,
                                                          "remotingServer");
            return rs.port();
        }
        // When using random port 0, the returned port is not the actual port
        return this.serverConfig.getPort();
    }

    public void exportAll() {
        this.checkEnabled();
        LOG.debug("RpcServer starting on port {}", this.port());
        Map<String, ProviderConfig<?>> configs = this.configs.configs();
        if (MapUtils.isEmpty(configs)) {
            LOG.info("RpcServer config is empty, skip starting RpcServer");
            return;
        }
        int timeout = this.conf.get(RpcOptions.RPC_SERVER_TIMEOUT) * 1000;
        for (ProviderConfig<?> providerConfig : configs.values()) {
            providerConfig.setServer(this.serverConfig)
                          .setTimeout(timeout)
                          .export();
        }
        LOG.info("RpcServer started success on port {}", this.port());
    }

    public void unexportAll() {
        this.configs.removeAllService();
    }

    public void unexport(String serviceId) {
        this.configs.removeService(serviceId);
    }

    public void destroy() {
        if (!this.enabled()) {
            return;
        }
        LOG.info("RpcServer stop on port {}", this.port());
        for (ProviderConfig<?> config : this.configs.configs().values()) {
            Object service = config.getRef();
            if (service instanceof AutoCloseable) {
                try {
                    ((AutoCloseable) service).close();
                } catch (Exception e) {
                    LOG.warn("Failed to close service {}", service, e);
                }
            }
        }
        this.configs.removeAllService();
        this.serverConfig.destroy();
    }

    private void checkEnabled() {
        E.checkArgument(this.enabled(),
                        "RpcServer is not enabled, please config option '%s'",
                        RpcOptions.RPC_SERVER_HOST.name());
    }
}
