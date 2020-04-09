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

package com.baidu.hugegraph.job.algorithm;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.baidu.hugegraph.job.algorithm.cent.BetweenessCentralityAlgorithm;
import com.baidu.hugegraph.job.algorithm.cent.ClosenessCentralityAlgorithm;
import com.baidu.hugegraph.job.algorithm.cent.DegreeCentralityAlgorithm;
import com.baidu.hugegraph.job.algorithm.cent.EigenvectorCentralityAlgorithm;
import com.baidu.hugegraph.job.algorithm.comm.ClusterCoeffcientAlgorithm;
import com.baidu.hugegraph.job.algorithm.comm.LouvainAlgorithm;
import com.baidu.hugegraph.job.algorithm.comm.LpaAlgorithm;
import com.baidu.hugegraph.job.algorithm.comm.TriangleCountAlgorithm;

public class AlgorithmPool {

    private static final AlgorithmPool INSTANCE = new AlgorithmPool();

    static {
        INSTANCE.register(new CountVertexAlgorithm());
        INSTANCE.register(new CountEdgeAlgorithm());

        INSTANCE.register(new DegreeCentralityAlgorithm());
        INSTANCE.register(new BetweenessCentralityAlgorithm());
        INSTANCE.register(new ClosenessCentralityAlgorithm());
        INSTANCE.register(new EigenvectorCentralityAlgorithm());

        INSTANCE.register(new TriangleCountAlgorithm());
        INSTANCE.register(new ClusterCoeffcientAlgorithm());
        INSTANCE.register(new LpaAlgorithm());
        INSTANCE.register(new LouvainAlgorithm());
    }

    private final Map<String, Algorithm> algorithms;

    public AlgorithmPool() {
        this.algorithms = new ConcurrentHashMap<>();
    }

    public Algorithm register(Algorithm algo) {
        assert !this.algorithms.containsKey(algo.name());
        return this.algorithms.put(algo.name(), algo);
    }

    public Algorithm find(String name) {
        return this.algorithms.get(name);
    }

    public static AlgorithmPool instance() {
        return INSTANCE;
    }
}
