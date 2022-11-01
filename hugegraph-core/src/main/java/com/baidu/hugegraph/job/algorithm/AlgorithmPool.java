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

import com.baidu.hugegraph.job.algorithm.cent.BetweennessCentralityAlgorithm;
import com.baidu.hugegraph.job.algorithm.cent.BetweennessCentralityAlgorithmV2;
import com.baidu.hugegraph.job.algorithm.cent.ClosenessCentralityAlgorithm;
import com.baidu.hugegraph.job.algorithm.cent.ClosenessCentralityAlgorithmV2;
import com.baidu.hugegraph.job.algorithm.cent.DegreeCentralityAlgorithm;
import com.baidu.hugegraph.job.algorithm.cent.EigenvectorCentralityAlgorithm;
import com.baidu.hugegraph.job.algorithm.cent.StressCentralityAlgorithm;
import com.baidu.hugegraph.job.algorithm.cent.StressCentralityAlgorithmV2;
import com.baidu.hugegraph.job.algorithm.comm.ClusterCoefficientAlgorithm;
import com.baidu.hugegraph.job.algorithm.comm.KCoreAlgorithm;
import com.baidu.hugegraph.job.algorithm.comm.LouvainAlgorithm;
import com.baidu.hugegraph.job.algorithm.comm.LpaAlgorithm;
import com.baidu.hugegraph.job.algorithm.comm.TriangleCountAlgorithm;
import com.baidu.hugegraph.job.algorithm.comm.WeakConnectedComponent;
import com.baidu.hugegraph.job.algorithm.path.RingsDetectAlgorithm;
import com.baidu.hugegraph.job.algorithm.rank.PageRankAlgorithm;
import com.baidu.hugegraph.job.algorithm.similarity.FusiformSimilarityAlgorithm;
import com.baidu.hugegraph.util.E;

public class AlgorithmPool {

    private static final AlgorithmPool INSTANCE = new AlgorithmPool();

    static {
        INSTANCE.register(new CountVertexAlgorithm());
        INSTANCE.register(new CountEdgeAlgorithm());

        INSTANCE.register(new DegreeCentralityAlgorithm());
        INSTANCE.register(new StressCentralityAlgorithm());
        INSTANCE.register(new BetweennessCentralityAlgorithm());
        INSTANCE.register(new ClosenessCentralityAlgorithm());
        INSTANCE.register(new EigenvectorCentralityAlgorithm());

        INSTANCE.register(new TriangleCountAlgorithm());
        INSTANCE.register(new ClusterCoefficientAlgorithm());
        INSTANCE.register(new LpaAlgorithm());
        INSTANCE.register(new LouvainAlgorithm());
        INSTANCE.register(new WeakConnectedComponent());

        INSTANCE.register(new FusiformSimilarityAlgorithm());
        INSTANCE.register(new RingsDetectAlgorithm());
        INSTANCE.register(new KCoreAlgorithm());

        INSTANCE.register(new PageRankAlgorithm());

        INSTANCE.register(new SubgraphStatAlgorithm());

        INSTANCE.register(new StressCentralityAlgorithmV2());
        INSTANCE.register(new BetweennessCentralityAlgorithmV2());
        INSTANCE.register(new ClosenessCentralityAlgorithmV2());
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

    public Algorithm get(String name) {
        Algorithm algorithm = this.algorithms.get(name);
        E.checkArgument(algorithm != null,
                        "Not found algorithm '%s'", name);
        return algorithm;
    }

    public static AlgorithmPool instance() {
        return INSTANCE;
    }
}
