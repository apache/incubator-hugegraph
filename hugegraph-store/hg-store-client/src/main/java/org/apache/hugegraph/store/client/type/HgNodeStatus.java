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

package org.apache.hugegraph.store.client.type;

/**
 * created on 2021/10/26
 */
public enum HgNodeStatus {

    UNKNOWN(0, "UNKNOWN"),
    NOT_EXIST(100, "NOT_EXIST"),    // Failed to apply for an instance via node-id from NodeManager.
    NOT_ONLINE(105, "NOT_ONLINE"),  // Failed to connect to Store-Node at the first time.
    NOT_WORK(110, "NOT_WORK"),      // When a Store-Node to be not work anymore.

    PARTITION_COMMON_FAULT(200, "PARTITION_COMMON_FAULT"),   //
    NOT_PARTITION_LEADER(205,
                         "NOT_PARTITION_LEADER") // When a Store-Node is not a specific partition
    // leader.

    ;

    private final int status;
    private final String name;

    HgNodeStatus(int status, String name) {
        this.status = status;
        this.name = name;
    }

}
