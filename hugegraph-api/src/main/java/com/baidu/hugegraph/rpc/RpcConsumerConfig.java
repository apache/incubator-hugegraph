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

public class RpcConsumerConfig implements RpcServiceConfig4Client {

    private final HugeConfig conf;
    private final String remoteUrl;
    private final Map<String, ConsumerConfig<?>> configs;

    public RpcConsumerConfig(HugeConfig conf, String remoteUrl) {
        RpcCommonConfig.initRpcConfigs(conf);
        this.conf = conf;
        this.remoteUrl = remoteUrl;
        this.configs = Maps.newHashMap();
    }

    @Override
    public <T> T serviceProxy(String graph, String interfaceId) {
        ConsumerConfig<T> config = this.consumerConfig(graph, interfaceId);
        return config.refer();
    }

    @Override
    public <T> T serviceProxy(String interfaceId) {
        ConsumerConfig<T> config = this.consumerConfig(null, interfaceId);
        return config.refer();
    }

    private <T> ConsumerConfig<T> consumerConfig(String graph,
                                                 String interfaceId) {
        String serviceId;
        if (graph != null) {
            serviceId = interfaceId + ":" + graph;
        } else {
            serviceId = interfaceId;
        }

        @SuppressWarnings("unchecked")
        ConsumerConfig<T> consumerConfig = (ConsumerConfig<T>)
                                           this.configs.get(serviceId);
        if (consumerConfig != null) {
            return consumerConfig;
        }

        assert consumerConfig == null;
        consumerConfig = new ConsumerConfig<>();

        HugeConfig conf = this.conf;
        String protocol = conf.get(ServerOptions.RPC_PROTOCOL);
        int timeout = conf.get(ServerOptions.RPC_CLIENT_READ_TIMEOUT) * 1000;
        int connectTimeout = conf.get(ServerOptions
                                      .RPC_CLIENT_CONNECT_TIMEOUT) * 1000;
        int reconnectPeriod = conf.get(ServerOptions
                                       .RPC_CLIENT_RECONNECT_PERIOD) * 1000;
        int retries = conf.get(ServerOptions.RPC_CLIENT_RETRIES);
        String loadBalancer = conf.get(ServerOptions.RPC_CLIENT_LOAD_BALANCER);

        if (graph != null) {
            consumerConfig.setId(serviceId).setUniqueId(graph);
        }
        consumerConfig.setInterfaceId(interfaceId)
                      .setProtocol(protocol)
                      .setDirectUrl(this.remoteUrl)
                      .setTimeout(timeout)
                      .setConnectTimeout(connectTimeout)
                      .setReconnectPeriod(reconnectPeriod)
                      .setRetries(retries)
                      .setLoadBalancer(loadBalancer);

        this.configs.put(serviceId, consumerConfig);
        return consumerConfig;
    }
}
