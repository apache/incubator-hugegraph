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

import com.alipay.sofa.rpc.config.ConsumerConfig;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.config.ServerOptions;
import com.google.common.collect.Maps;

public class RpcConsumerConfig {

    private final Map<String, ConsumerConfig> configs = Maps.newHashMap();

    public <T> void addConsumerConfig(Class<T> clazz, HugeConfig conf) {
        ConsumerConfig<T> consumerConfig = new ConsumerConfig<T>()
                .setInterfaceId(clazz.getName())
                .setProtocol(conf.get(ServerOptions.RPC_PROTOCOL))
                .setDirectUrl(conf.get(ServerOptions.AUTH_REMOTE_URL))
                .setTimeout(conf.get(ServerOptions.RPC_CLIENT_READ_TIMEOUT))
                .setConnectTimeout(conf.get(
                                   ServerOptions.RPC_CLIENT_CONNECTION_TIMEOUT))
                .setRetries(conf.get(ServerOptions.RPC_CLIENT_RETRIES))
                .setLoadBalancer(conf.get(
                                 ServerOptions.RPC_CLIENT_LOAD_BALANCER));
        configs.put(clazz.getName(), consumerConfig);
    }

    public ConsumerConfig consumerConfig(String serverName) {
        if (!configs.containsKey(serverName)) {
            throw new RpcException("Invalid server name '%s'", serverName);
        }
        return configs.get(serverName);
    }
}
