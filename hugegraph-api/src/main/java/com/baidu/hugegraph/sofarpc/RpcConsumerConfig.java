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

import java.util.Map;

import com.alipay.sofa.rpc.config.ConsumerConfig;
import com.alipay.sofa.rpc.core.exception.RpcErrorType;
import com.alipay.sofa.rpc.core.exception.SofaRpcException;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.config.ServerOptions;
import com.google.common.collect.Maps;

public class RpcConsumerConfig {

    private final Map<String, ConsumerConfig> CONSUMER_CONFIG =
            Maps.newHashMap();

    public <T> RpcConsumerConfig(Class<T> clazz, HugeConfig conf) {
        this.buildConsumerConfig(clazz, conf);
    }

    public <T> void buildConsumerConfig(Class<T> clazz, HugeConfig conf) {
        ConsumerConfig<T> consumerConfig = new ConsumerConfig<T>()
                .setInterfaceId(clazz.getName())
                .setDirectUrl(conf.get(ServerOptions.AUTH_REMOTE_URL))
                .setTimeout(conf.get(ServerOptions.RPC_SERVER_TIMEOUT))
                .setRetries(conf.get(ServerOptions.RPC_SERVER_RETRIES));
        CONSUMER_CONFIG.put(clazz.getName(), consumerConfig);
    }

    public ConsumerConfig getConsumerConfig(String serverName) {
        if (!CONSUMER_CONFIG.containsKey(serverName)) {
            throw new SofaRpcException(RpcErrorType.CLIENT_UNDECLARED_ERROR,
                                       String.format("Invalid server name " +
                                                     "%s", serverName));
        }
        return CONSUMER_CONFIG.get(serverName);
    }
}
