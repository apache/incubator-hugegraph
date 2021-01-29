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

package com.baidu.hugegraph.sofarpc;

import org.slf4j.Logger;

import com.baidu.hugegraph.auth.UserManager;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.config.ServerOptions;
import com.baidu.hugegraph.util.Log;

public class RpcServerProvider {

    private static final Logger LOG = Log.logger(RpcServerProvider.class);

    public final SofaRpcServer rpcServer;

    public RpcServerProvider(HugeConfig conf, UserManager userManager) {
        LOG.info("rpcServer start {}", conf.get(ServerOptions.RPC_SERVER_PORT));
        RpcProviderConfig rpcProviderConfig =
                new RpcProviderConfig(UserManager.class, userManager);
        this.rpcServer = new SofaRpcServer(conf, rpcProviderConfig);
        this.rpcServer.exportAll();
        LOG.info("rpcServer start success, bind port is {}",
                 this.rpcServer.getBindPort());
    }

    public void unExport(String serviceName) {
        this.rpcServer.unExport(serviceName);
    }

    public void destroy() {
        this.rpcServer.destroy();
    }
}
