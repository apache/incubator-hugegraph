/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hugegraph.masterelection;

import static org.apache.hugegraph.config.OptionChecker.disallowEmpty;
import static org.apache.hugegraph.config.OptionChecker.rangeInt;

import org.apache.hugegraph.config.ConfigOption;
import org.apache.hugegraph.config.OptionHolder;

public class RoleElectionOptions  extends OptionHolder {

    private RoleElectionOptions() {
        super();
    }

    private static volatile RoleElectionOptions instance;

    public static synchronized RoleElectionOptions instance() {
        if (instance == null) {
            instance = new RoleElectionOptions();
            // Should initialize all static members first, then register.
            instance.registerOptions();
        }
        return instance;
    }

    public static final ConfigOption<Integer> EXCEEDS_FAIL_COUNT =
            new ConfigOption<>(
                    "server.role.fail_count",
                    "When the node failed count of update or query heartbeat " +
                    "is reaches this threshold, the node will become abdication state to guard" +
                    "safe property.",
                    rangeInt(0, Integer.MAX_VALUE),
                    5
            );

    public static final ConfigOption<String> NODE_EXTERNAL_URL =
            new ConfigOption<>(
                    "server.role.node_external_url",
                    "The url of external accessibility.",
                    disallowEmpty(),
                    "http://127.0.0.1:8080"
            );

    public static final ConfigOption<Integer> RANDOM_TIMEOUT_MILLISECOND =
            new ConfigOption<>(
                    "server.role.random_timeout",
                    "The random timeout in ms that be used when candidate node request " +
                    "to become master state to reduce competitive voting.",
                    rangeInt(0, Integer.MAX_VALUE),
                    1000
            );

    public static final ConfigOption<Integer> HEARTBEAT_INTERVAL_SECOND =
            new ConfigOption<>(
                    "server.role.heartbeat_interval",
                    "The role state machine heartbeat interval second time.",
                    rangeInt(0, Integer.MAX_VALUE),
                    2
            );

    public static final ConfigOption<Integer> MASTER_DEAD_TIMES =
            new ConfigOption<>(
                    "server.role.master_dead_times",
                    "When the worker node detects that the number of times " +
                    "the master node fails to update heartbeat reaches this threshold, " +
                    "the worker node will become to a candidate node.",
                    rangeInt(0, Integer.MAX_VALUE),
                    10
            );

    public static final ConfigOption<Integer> BASE_TIMEOUT_MILLISECOND =
            new ConfigOption<>(
                    "server.role.base_timeout",
                    "The role state machine candidate state base timeout time.",
                    rangeInt(0, Integer.MAX_VALUE),
                    500
            );
}
