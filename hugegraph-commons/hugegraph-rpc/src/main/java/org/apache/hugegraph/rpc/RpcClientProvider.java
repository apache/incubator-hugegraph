/*
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

package org.apache.hugegraph.rpc;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Set;

import com.alipay.sofa.rpc.common.utils.StringUtils;
import org.apache.hugegraph.config.RpcOptions;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.util.E;

public class RpcClientProvider {

    private final RpcConsumerConfig consumerConfig;

    public RpcClientProvider(HugeConfig config) {
        // TODO: fetch from registry server
        String rpcUrl = config.get(RpcOptions.RPC_REMOTE_URL);
        String selfUrl = config.get(RpcOptions.RPC_SERVER_HOST) + ":" +
                         config.get(RpcOptions.RPC_SERVER_PORT);
        rpcUrl = excludeSelfUrl(rpcUrl, selfUrl);
        this.consumerConfig = StringUtils.isNotBlank(rpcUrl) ?
                              new RpcConsumerConfig(config, rpcUrl) : null;
    }

    public boolean enabled() {
        return this.consumerConfig != null;
    }

    public RpcConsumerConfig config() {
        E.checkArgument(this.consumerConfig != null,
                        "RpcClient is not enabled, please config option '%s' " +
                        "and ensure to add an address other than self service",
                        RpcOptions.RPC_REMOTE_URL.name());
        return this.consumerConfig;
    }

    public void unreferAll() {
        if (this.consumerConfig != null) {
            this.consumerConfig.removeAllServiceProxy();
        }
    }

    public void destroy() {
        if (this.consumerConfig != null) {
            this.consumerConfig.destroy();
        }
    }

    protected static String excludeSelfUrl(String rpcUrl, String selfUrl) {
        String[] urls = StringUtils.splitWithCommaOrSemicolon(rpcUrl);
        // Keep urls order via LinkedHashSet
        Set<String> urlSet = new LinkedHashSet<>(Arrays.asList(urls));
        urlSet.remove(selfUrl);
        return String.join(",", urlSet);
    }
}
