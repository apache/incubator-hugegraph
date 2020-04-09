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

import java.util.Iterator;
import java.util.Map;

import org.apache.tinkerpop.gremlin.structure.Vertex;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.query.Query;
import com.baidu.hugegraph.job.Job;
import com.baidu.hugegraph.job.algorithm.AbstractAlgorithm;
import com.baidu.hugegraph.structure.HugeVertex;
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
    }

    @Override
    public Object call(Job<Object> job, Map<String, Object> parameters) {
        Traverser traverser = new Traverser(job);
        return traverser.rings(sourceLabel(parameters),
                               sourceCLabel(parameters),
                               direction(parameters), edgeLabel(parameters),
                               depth(parameters), degree(parameters),
                               capacity(parameters), limit(parameters));
    }

    public static class Traverser extends AlgoTraverser {

        public Traverser(Job<Object> job) {
            super(job);
        }

        public Object rings(String sourceLabel, String sourceCLabel,
                            Directions dir, String label, int depth,
                            long degree, long capacity, long limit) {
            HugeGraph graph = this.graph();
            Iterator<Vertex> vertices = this.vertices(sourceLabel, sourceCLabel,
                                                      Query.NO_LIMIT);
            JsonMap ringsJson = new JsonMap();
            ringsJson.startObject();
            ringsJson.appendKey("rings");
            ringsJson.startList();
            SubGraphTraverser traverser = new SubGraphTraverser(graph);
            while(vertices.hasNext()) {
                this.updateProgress(++this.progress);
                Id source = ((HugeVertex) vertices.next()).id();
                PathSet rings = traverser.rings(source, dir, label, depth,
                                                true, degree,
                                                capacity, limit);
                for (Path ring : rings) {
                    Id min = null;
                    for (Id id : ring.vertices()) {
                        if (min == null || id.compareTo(min) < 0) {
                            min = id;
                        }
                    }
                    if (source.equals(min)) {
                        ringsJson.appendRaw(JsonUtil.toJson(ring.vertices()));
                    }
                }
            }
            ringsJson.endList();
            ringsJson.endObject();

            return ringsJson.asJson();
        }
    }
}
