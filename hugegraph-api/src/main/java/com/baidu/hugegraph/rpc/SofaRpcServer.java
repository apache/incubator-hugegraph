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

import com.alipay.sofa.rpc.common.RpcConfigs;
import com.alipay.sofa.rpc.common.RpcOptions;
import com.alipay.sofa.rpc.config.ProviderConfig;
import com.alipay.sofa.rpc.config.ServerConfig;
import com.alipay.sofa.rpc.context.RpcRuntimeContext;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.config.ServerOptions;

public class SofaRpcServer {

    private final Map<String, ProviderConfig> providerConfigs;
    private final ServerConfig serverConfig;
    private final int rpcServerTimeout;

    static {
        if (RpcConfigs.getOrDefaultValue(RpcOptions.JVM_SHUTDOWN_HOOK, true)) {
            Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
                @Override
                public void run() {
                    RpcRuntimeContext.destroy();
                }
            }, "SOFA-RPC-ShutdownHook"));
        }
    }

    public SofaRpcServer(HugeConfig conf, RpcProviderConfig providerConfig) {
        this.serverConfig = new ServerConfig()
                            .setProtocol(conf.get(ServerOptions.RPC_PROTOCOL))
                            .setPort(conf.get(ServerOptions.RPC_SERVER_PORT))
                            .setHost(conf.get(ServerOptions.RPC_SERVER_HOST))
                            .setDaemon(false);
        this.providerConfigs = providerConfig.providerConfigs();
        this.rpcServerTimeout = conf.get(ServerOptions.RPC_SERVER_TIMEOUT) * 1000;
    }

    public void exportAll() {
        if (MapUtils.isEmpty(this.providerConfigs)) {
            throw new RpcException(
                      "The server provider config map can't be empty");
        }
        for (ProviderConfig providerConfig : this.providerConfigs.values()) {
            providerConfig.setServer(this.serverConfig);
            providerConfig.setTimeout(this.rpcServerTimeout);
            providerConfig.export();
        }
    }

    public void unExport(String serviceName) {
        if (!this.providerConfigs.containsKey(serviceName)) {
            throw new RpcException("The service name '%s' doesn't exist, please " +
                                   "change others", serviceName);
        }
        this.providerConfigs.get(serviceName).unExport();
    }

    public int port() {
        return this.serverConfig.getPort();
    }

    public void destroy() {
        this.serverConfig.destroy();
    }
}
