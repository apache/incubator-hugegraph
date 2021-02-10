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

import com.alipay.sofa.rpc.common.RpcConfigs;
import com.alipay.sofa.rpc.common.RpcOptions;
import com.alipay.sofa.rpc.config.ProviderConfig;
import com.alipay.sofa.rpc.config.ServerConfig;
import com.alipay.sofa.rpc.context.RpcRuntimeContext;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.config.ServerOptions;
import com.baidu.hugegraph.util.Log;

public class SofaRpcServer {

    private static final Logger LOG = Log.logger(SofaRpcServer.class);

    private final HugeConfig conf;
    private final RpcProviderConfig configs;
    private final ServerConfig serverConfig;

    static {
        if (RpcConfigs.getOrDefaultValue(RpcOptions.JVM_SHUTDOWN_HOOK, true)) {
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                RpcRuntimeContext.destroy();
            }, "SOFA-RPC-ShutdownHook"));
        }
    }

    public SofaRpcServer(HugeConfig conf) {
        RpcCommonConfig.initRpcConfigs(conf);
        this.conf = conf;
        this.serverConfig = new ServerConfig()
                            .setProtocol(conf.get(ServerOptions.RPC_PROTOCOL))
                            .setPort(conf.get(ServerOptions.RPC_SERVER_PORT))
                            .setHost(conf.get(ServerOptions.RPC_SERVER_HOST))
                            .setDaemon(false);
        this.configs = new RpcProviderConfig();
    }

    public RpcProviderConfig config() {
        return this.configs;
    }

    public void exportAll() {
        LOG.debug("RpcServer starting on port {}", this.port());
        Map<String, ProviderConfig> configs = this.configs.configs();
        if (MapUtils.isEmpty(configs)) {
            LOG.info("RpcServer config is empty, skip starting RpcServer");
            return;
        }
        int timeout = this.conf.get(ServerOptions.RPC_SERVER_TIMEOUT) * 1000;
        for (ProviderConfig providerConfig : configs.values()) {
            providerConfig.setServer(this.serverConfig);
            providerConfig.setTimeout(timeout);
            providerConfig.export();
        }
        LOG.info("RpcServer started success on port {}", this.port());
    }

    public void unExport(String serviceName) {
        Map<String, ProviderConfig> configs = this.configs.configs();
        if (!configs.containsKey(serviceName)) {
            throw new RpcException("The service name '%s' doesn't exist",
                                   serviceName);
        }
        configs.get(serviceName).unExport();
    }

    public int port() {
        return this.serverConfig.getPort();
    }

    public void destroy() {
        LOG.info("RpcServer stop on port {}", this.port());
        this.serverConfig.destroy();
    }
}
