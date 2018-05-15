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

package com.baidu.hugegraph.backend.store.palo;

import static com.baidu.hugegraph.config.OptionChecker.disallowEmpty;
import static com.baidu.hugegraph.config.OptionChecker.nonNegativeInt;
import static com.baidu.hugegraph.config.OptionChecker.positiveInt;
import static com.baidu.hugegraph.config.OptionChecker.rangeInt;

import com.baidu.hugegraph.config.ConfigOption;
import com.baidu.hugegraph.config.OptionHolder;

public class PaloOptions extends OptionHolder {

    private PaloOptions() {
        super();
    }

    private static volatile PaloOptions instance;

    public static synchronized PaloOptions instance() {
        if (instance == null) {
            instance = new PaloOptions();
            instance.registerOptions();
        }
        return instance;
    }

    public static final ConfigOption<String> PALO_HOST =
            new ConfigOption<>(
                    "palo.host",
                    "The host/ip of Palo cluster.",
                    disallowEmpty(),
                    "127.0.0.1"
            );

    public static final ConfigOption<Integer> PALO_HTTP_PORT =
            new ConfigOption<>(
                    "palo.http_port",
                    "The http port of Palo cluster.",
                    positiveInt(),
                    8030
            );

    public static final ConfigOption<Integer> PALO_HTTP_TIMEOUT =
            new ConfigOption<>(
                    "palo.http_timeout",
                    "Timeout(second) for connecting and reading Palo.",
                    nonNegativeInt(),
                    20
            );

    public static final ConfigOption<String> PALO_USERNAME =
            new ConfigOption<>(
                    "palo.username",
                    "The username to login Palo.",
                    disallowEmpty(),
                    "root"
            );

    public static final ConfigOption<String> PALO_PASSWORD =
            new ConfigOption<>(
                    "palo.password",
                    "The password corresponding to palo.username.",
                    null,
                    ""
            );

    public static final ConfigOption<Integer> PALO_POLL_INTERVAL =
            new ConfigOption<>(
                    "palo.poll_interval",
                    "The execution peroid of the background thread that " +
                    "check whether need to load file data into Palo.",
                    rangeInt(5, Integer.MAX_VALUE),
                    5
            );

    public static final ConfigOption<String> PALO_TEMP_DIR =
            new ConfigOption<>(
                    "palo.temp_dir",
                    "The temporary directory to store table files.",
                    null,
                    "palo-data"
            );

    public static final ConfigOption<Integer> PALO_FILE_LIMIT_SIZE =
            new ConfigOption<>(
                    "palo.file_limit_size",
                    "The maximum size(MB) of each file for loading into Palo.",
                    rangeInt(10, 1000),
                    50
            );
}
