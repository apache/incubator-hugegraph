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

package com.baidu.hugegraph.job.compute;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ComputePool {

    private static final ComputePool INSTANCE = new ComputePool();

    static {
        INSTANCE.register(new PageRankCompute());
        INSTANCE.register(new WeakConnectedComponentCompute());
    }

    private final Map<String, Compute> computes;

    public ComputePool() {
        this.computes = new ConcurrentHashMap<>();
    }

    public Compute register(Compute compute) {
        assert !this.computes.containsKey(compute.name());
        return this.computes.put(compute.name(), compute);
    }

    public Compute find(String name) {
        return this.computes.get(name);
    }

    public static ComputePool instance() {
        return INSTANCE;
    }
}
