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

package org.apache.hugegraph.backend.store.hstore;

import static org.apache.hugegraph.config.OptionChecker.disallowEmpty;

import org.apache.hugegraph.config.ConfigOption;
import org.apache.hugegraph.config.OptionHolder;

public class HstoreOptions extends OptionHolder {

    public static final ConfigOption<Integer> PARTITION_COUNT = new ConfigOption<>(
            "hstore.partition_count",
            "Number of partitions, which PD controls partitions based on.",
            disallowEmpty(),
            0
    );
    public static final ConfigOption<Integer> SHARD_COUNT = new ConfigOption<>(
            "hstore.shard_count",
            "Number of copies, which PD controls partition copies based on.",
            disallowEmpty(),
            0
    );
    private static volatile HstoreOptions instance;

    private HstoreOptions() {
        super();
    }

    public static synchronized HstoreOptions instance() {
        if (instance == null) {
            instance = new HstoreOptions();
            instance.registerOptions();
        }
        return instance;
    }
}
