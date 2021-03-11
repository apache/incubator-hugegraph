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

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Set;

import com.alipay.sofa.rpc.common.utils.StringUtils;
import com.baidu.hugegraph.auth.AuthManager;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.config.ServerOptions;
import com.baidu.hugegraph.util.E;

public class RpcClientProvider {

    public final RpcConsumerConfig consumerConfig;
    public final RpcConsumerConfig authConsumerConfig;

    public RpcClientProvider(HugeConfig conf) {
        // TODO: fetch from registry server
        String rpcUrl = conf.get(ServerOptions.RPC_REMOTE_URL);
        String selfUrl = conf.get(ServerOptions.RPC_SERVER_HOST) + ":" +
                         conf.get(ServerOptions.RPC_SERVER_PORT);
        rpcUrl = excludeSelfUrl(rpcUrl, selfUrl);
        this.consumerConfig = StringUtils.isNotBlank(rpcUrl) ?
                              new RpcConsumerConfig(conf, rpcUrl) : null;

        String authUrl = conf.get(ServerOptions.AUTH_REMOTE_URL);
        this.authConsumerConfig = StringUtils.isNotBlank(authUrl) ?
                                  new RpcConsumerConfig(conf, authUrl) : null;
    }

    public boolean enabled() {
        return this.consumerConfig != null;
    }

    public RpcConsumerConfig config() {
        E.checkArgument(this.consumerConfig != null,
                        "RpcClient is not enabled, please config option '%s'",
                        ServerOptions.RPC_REMOTE_URL.name());
        return this.consumerConfig;
    }

    public AuthManager authManager() {
        E.checkArgument(this.authConsumerConfig != null,
                        "RpcClient is not enabled, please config option '%s'",
                        ServerOptions.AUTH_REMOTE_URL.name());
        return this.authConsumerConfig.serviceProxy(AuthManager.class);
    }

    private static String excludeSelfUrl(String rpcUrl, String selfUrl) {
        String[] urls = StringUtils.splitWithCommaOrSemicolon(rpcUrl);
        // Keep urls order via LinkedHashSet
        Set<String> urlSet = new LinkedHashSet<>(Arrays.asList(urls));
        urlSet.remove(selfUrl);
        return String.join(",", urlSet);
    }
}
