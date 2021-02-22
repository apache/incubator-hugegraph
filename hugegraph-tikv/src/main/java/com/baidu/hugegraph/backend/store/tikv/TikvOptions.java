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

package com.baidu.hugegraph.backend.store.tikv;

import static com.baidu.hugegraph.config.OptionChecker.*;

import com.baidu.hugegraph.config.ConfigOption;
import com.baidu.hugegraph.config.OptionHolder;

public class TikvOptions extends OptionHolder {

    private TikvOptions() {
        super();
    }

    private static volatile TikvOptions instance;

    public static synchronized TikvOptions instance() {
        if (instance == null) {
            instance = new TikvOptions();
            instance.registerOptions();
        }
        return instance;
    }

    public static final ConfigOption<String> TIKV_PDS =
            new ConfigOption<>(
                    "tikv.pds",
                    "The addresses of Tikv pds, separated with commas.",
                    disallowEmpty(),
                    "localhost"
            );

    public static final ConfigOption<Integer> TIKV_BATCH_GET_CONCURRENCY =
            new ConfigOption<>(
                    "tikv.batch_get_concurrency",
                    "The number of thread pool size for batch get of tikv client.",
                    disallowEmpty(),
                    20
            );

    public static final ConfigOption<Integer> TIKV_BATCH_PUT_CONCURRENCY =
            new ConfigOption<>(
                    "tikv.batch_put_concurrency",
                    "The number of thread pool size for batch put of tikv client.",
                    disallowEmpty(),
                    20
            );

    public static final ConfigOption<Integer> TIKV_BATCH_DELETE_CONCURRENCY =
            new ConfigOption<>(
                    "tikv.batch_delete_concurrency",
                    "The number of thread pool size for batch delete of tikv client.",
                    disallowEmpty(),
                    20
            );

    public static final ConfigOption<Integer> TIKV_BATCH_SCAN_CONCURRENCY =
            new ConfigOption<>(
                    "tikv.batch_scan_concurrency",
                    "The number of thread pool size for batch scan of tikv client.",
                    disallowEmpty(),
                    5
            );

    public static final ConfigOption<Integer> TIKV_DELETE_RANGE_CONCURRENCY =
            new ConfigOption<>(
                    "tikv.delete_range_concurrency",
                    "The number of thread pool size for delete range of tikv client.",
                    disallowEmpty(),
                    20
            );
}
