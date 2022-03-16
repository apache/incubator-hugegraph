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

package com.baidu.hugegraph.kafka;

/**
 * The role of entire cluster, not just a node or instance
 * @author Scorpiour
 * @since 2022-01-27
 */
public enum HugeGraphClusterRole {

    NONE("NONE"),
    MASTER("MASTER"),
    SLAVE("SLAVE"),
    ;
    private final String value;

    private HugeGraphClusterRole(String value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return this.value;
    }

    public static HugeGraphClusterRole fromName(String name) {
        if ("MASTER".equals(name)) {
            return HugeGraphClusterRole.MASTER;
        }
        if ("SLAVE".equals(name)) {
            return HugeGraphClusterRole.SLAVE; 
        }
        return HugeGraphClusterRole.NONE;
    }
}
