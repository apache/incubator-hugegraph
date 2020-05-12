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

package com.baidu.hugegraph.job.algorithm.path;

import java.util.Map;

import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.job.Job;
import com.baidu.hugegraph.job.algorithm.AbstractAlgorithm;
import com.baidu.hugegraph.traversal.algorithm.SubGraphTraverser;
import com.baidu.hugegraph.type.define.Directions;
import com.baidu.hugegraph.util.JsonUtil;

public class RingsDetectAlgorithm extends AbstractAlgorithm {

    @Override
    public String name() {
        return "rings_detect";
    }

    @Override
    public String category() {
        return CATEGORY_PATH;
    }

    @Override
    public void checkParameters(Map<String, Object> parameters) {
        depth(parameters);
        degree(parameters);
        capacity(parameters);
        limit(parameters);
        sourceLabel(parameters);
        sourceCLabel(parameters);
        direction(parameters);
        edgeLabel(parameters);
        workers(parameters);
    }

    @Override
    public Object call(Job<Object> job, Map<String, Object> parameters) {
        int workers = workers(parameters);
        try (Traverser traverser = new Traverser(job, workers)) {
            return traverser.rings(sourceLabel(parameters),
                                   sourceCLabel(parameters),
                                   direction(parameters),
                                   edgeLabel(parameters),
                                   depth(parameters),
                                   degree(parameters),
                                   capacity(parameters),
                                   limit(parameters));
        }
    }

    public static class Traverser extends AlgoTraverser {

        public Traverser(Job<Object> job, int workers) {
            super(job, "ring", workers);
        }

        public Object rings(String sourceLabel, String sourceCLabel,
                            Directions dir, String label, int depth,
                            long degree, long capacity, long limit) {
            JsonMap ringsJson = new JsonMap();
            ringsJson.startObject();
            ringsJson.appendKey("rings");
            ringsJson.startList();

            SubGraphTraverser traverser = new SubGraphTraverser(this.graph());

            this.traverse(sourceLabel, sourceCLabel, v -> {
                Id source = (Id) v.id();
                PathSet rings = traverser.rings(source, dir, label, depth,
                                                true, degree, capacity, limit);
                for (Path ring : rings) {
                    Id min = null;
                    for (Id id : ring.vertices()) {
                        if (min == null || id.compareTo(min) < 0) {
                            min = id;
                        }
                    }
                    if (source.equals(min)) {
                        String ringJson = JsonUtil.toJson(ring.vertices());
                        synchronized (ringsJson) {
                            ringsJson.appendRaw(ringJson);
                        }
                    }
                }
            });
            ringsJson.endList();
            ringsJson.endObject();

            return ringsJson.asJson();
        }
    }
}
