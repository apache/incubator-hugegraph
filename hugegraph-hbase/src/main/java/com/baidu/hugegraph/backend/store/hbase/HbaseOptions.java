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

package com.baidu.hugegraph.backend.store.hbase;

import static com.baidu.hugegraph.config.OptionChecker.disallowEmpty;
import static com.baidu.hugegraph.config.OptionChecker.rangeInt;

import com.baidu.hugegraph.config.ConfigOption;
import com.baidu.hugegraph.config.OptionHolder;

public class HbaseOptions extends OptionHolder {

    private HbaseOptions() {
        super();
    }

    private static volatile HbaseOptions instance;

    public static synchronized HbaseOptions instance() {
        if (instance == null) {
            instance = new HbaseOptions();
            instance.registerOptions();
        }
        return instance;
    }

    public static final ConfigOption<String> HBASE_HOSTS =
            new ConfigOption<>(
                    "hbase.hosts",
                    "The hostnames or ip addresses of HBase zookeeper, separated with commas.",
                    disallowEmpty(),
                    "localhost"
            );

    public static final ConfigOption<Integer> HBASE_PORT =
            new ConfigOption<>(
                    "hbase.port",
                    "The port address of HBase zookeeper.",
                    rangeInt(1, 65535),
                    2181
            );

    public static final ConfigOption<String> HBASE_ZNODE_PARENT =
            new ConfigOption<>(
                    "hbase.znode_parent",
                    "The znode parent path of HBase zookeeper.",
                    disallowEmpty(),
                    "/hbase"
            );

    public static final ConfigOption<Integer> HBASE_ZK_RETRY =
            new ConfigOption<>(
                    "hbase.zk_retry",
                    "The recovery retry times of HBase zookeeper.",
                    rangeInt(0, 1000),
                    3
            );

    public static final ConfigOption<Integer> HBASE_THREADS_MAX =
            new ConfigOption<>(
                    "hbase.threads_max",
                    "The max threads num of hbase connections.",
                    rangeInt(1, 1000),
                    64
            );
}
