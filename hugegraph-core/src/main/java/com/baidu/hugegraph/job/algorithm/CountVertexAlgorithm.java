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

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.lang3.mutable.MutableLong;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import com.baidu.hugegraph.job.UserJob;
import com.baidu.hugegraph.util.JsonUtil;

public class CountVertexAlgorithm extends AbstractAlgorithm {

    @Override
    public String name() {
        return "count_vertex";
    }

    @Override
    public String category() {
        return CATEGORY_AGGR;
    }

    @Override
    public Object call(UserJob<Object> job, Map<String, Object> parameters) {
        try (Traverser traverser = new Traverser(job)) {
            return traverser.count();
        }
    }

    private static class Traverser extends AlgoTraverser {

        public Traverser(UserJob<Object> job) {
            super(job);
        }

        public Object count() {
            Iterator<Vertex> vertices = this.vertices();

            Map<String, MutableLong> counts = new HashMap<>();
            long total = 0L;

            while (vertices.hasNext()) {
                Vertex vertex = vertices.next();
                String label = vertex.label();
                MutableLong count = counts.get(label);
                if (count != null) {
                    count.increment();
                } else {
                    counts.put(label, new MutableLong(1L));
                }
                total++;
                this.updateProgress(total);
            }
            counts.put("*", new MutableLong(total));

            return JsonUtil.asJson(counts);
        }
    }
}
