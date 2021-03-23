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

package com.baidu.hugegraph.config;

import static com.baidu.hugegraph.config.OptionChecker.allowValues;
import static com.baidu.hugegraph.config.OptionChecker.disallowEmpty;
import static com.baidu.hugegraph.config.OptionChecker.rangeInt;

public class RpcOptions extends OptionHolder {

    private RpcOptions() {
        super();
    }

    private static volatile RpcOptions instance;

    public static synchronized RpcOptions instance() {
        if (instance == null) {
            instance = new RpcOptions();
            instance.registerOptions();
        }
        return instance;
    }

    public static final ConfigOption<String> RPC_SERVER_HOST =
            new ConfigOption<>(
                    "rpc.server_host",
                    "The hosts/ips bound by rpc server to provide services, " +
                    "empty value means not enabled.",
                    null,
                    ""
            );

    public static final ConfigOption<Integer> RPC_SERVER_PORT =
            new ConfigOption<>(
                    "rpc.server_port",
                    "The port bound by rpc server to provide services.",
                    rangeInt(1, Integer.MAX_VALUE),
                    8090
            );

    public static final ConfigOption<Integer> RPC_SERVER_TIMEOUT =
            new ConfigOption<>(
                    "rpc.server_timeout",
                    "The timeout(in seconds) of rpc server execution.",
                    rangeInt(1, Integer.MAX_VALUE),
                    30
            );

    public static final ConfigOption<String> RPC_REMOTE_URL =
            new ConfigOption<>(
                    "rpc.remote_url",
                    "The remote urls of rpc peers, it can be set to " +
                    "multiple addresses, which are concat by ',', " +
                    "empty value means not enabled.",
                    null,
                    ""
            );

    public static final ConfigOption<Integer> RPC_CLIENT_CONNECT_TIMEOUT =
            new ConfigOption<>(
                    "rpc.client_connect_timeout",
                    "The timeout(in seconds) of rpc client connect to rpc " +
                    "server.",
                    rangeInt(1, Integer.MAX_VALUE),
                    20
            );

    public static final ConfigOption<Integer> RPC_CLIENT_RECONNECT_PERIOD =
            new ConfigOption<>(
                    "rpc.client_reconnect_period",
                    "The period(in seconds) of rpc client reconnect to rpc " +
                    "server.",
                    rangeInt(1, Integer.MAX_VALUE),
                    10
            );

    public static final ConfigOption<Integer> RPC_CLIENT_READ_TIMEOUT =
            new ConfigOption<>(
                    "rpc.client_read_timeout",
                    "The timeout(in seconds) of rpc client read from rpc " +
                    "server.",
                    rangeInt(1, Integer.MAX_VALUE),
                    40
            );

    public static final ConfigOption<Integer> RPC_CLIENT_RETRIES =
            new ConfigOption<>(
                    "rpc.client_retries",
                    "Failed retry number of rpc client calls to rpc server.",
                    rangeInt(0, Integer.MAX_VALUE),
                    3
            );

    public static final ConfigOption<String> RPC_CLIENT_LOAD_BALANCER =
            new ConfigOption<>(
                    "rpc.client_load_balancer",
                    "The rpc client uses a load-balancing algorithm to " +
                    "access multiple rpc servers in one cluster. Default " +
                    "value is 'consistentHash', means forwording by request " +
                    "parameters.",
                    allowValues("random", "localPref", "roundRobin",
                                "consistentHash", "weightRoundRobin"),
                    "consistentHash"
            );

    public static final ConfigOption<String> RPC_PROTOCOL =
            new ConfigOption<>(
                    "rpc.protocol",
                    "Rpc communication protocol, client and server need to " +
                    "be specified the same value.",
                    allowValues("bolt", "rest", "dubbo", "h2c", "http"),
                    "bolt"
            );

    public static final ConfigOption<Integer> RPC_CONFIG_ORDER =
            new ConfigOption<>(
                    "rpc.config_order",
                    "Sofa rpc configuration file loading order, the larger " +
                    "the more later loading.",
                    rangeInt(1, Integer.MAX_VALUE),
                    999
            );

    public static final ConfigOption<String> RPC_LOGGER_IMPL =
            new ConfigOption<>(
                    "rpc.logger_impl",
                    "Sofa rpc log implementation class.",
                    disallowEmpty(),
                    "com.alipay.sofa.rpc.log.SLF4JLoggerImpl"
            );
}
