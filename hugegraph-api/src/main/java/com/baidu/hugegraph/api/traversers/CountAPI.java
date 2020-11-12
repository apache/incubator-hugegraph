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

package com.baidu.hugegraph.api.traversers;

import static com.baidu.hugegraph.traversal.algorithm.HugeTraverser.DEFAULT_DEGREE;
import static com.baidu.hugegraph.traversal.algorithm.HugeTraverser.DEFAULT_SKIP_DEGREE;
import static com.baidu.hugegraph.traversal.algorithm.HugeTraverser.NO_LIMIT;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.inject.Singleton;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;

import org.slf4j.Logger;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.api.API;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.core.GraphManager;
import com.baidu.hugegraph.server.RestServer;
import com.baidu.hugegraph.structure.HugeVertex;
import com.baidu.hugegraph.traversal.algorithm.CountTraverser;
import com.baidu.hugegraph.type.define.Directions;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.Log;
import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;

@Path("graphs/{graph}/traversers/count")
@Singleton
public class CountAPI extends API {

    private static final Logger LOG = Log.logger(RestServer.class);

    @POST
    @Timed
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String post(@Context GraphManager manager,
                       @PathParam("graph") String graph,
                       CountRequest request) {
        LOG.debug("Graph [{}] get count from '{}' with request {}",
                  graph, request);

        E.checkArgumentNotNull(request.source,
                               "The source of request can't be null");
        Id sourceId = HugeVertex.getIdValue(request.source);
        E.checkArgumentNotNull(request.steps != null &&
                               !request.steps.isEmpty(),
                               "The steps of request can't be null or empty");
        E.checkArgumentNotNull(request.dedupSize == NO_LIMIT ||
                               request.dedupSize >= 0L,
                               "The dedupSize of request must >= 0, but got '%s'",
                               request.dedupSize);

        HugeGraph g = graph(manager, graph);
        List<CountTraverser.Step> steps = steps(g, request);
        CountTraverser traverser = new CountTraverser(g);
        long count = traverser.count(sourceId, steps, request.containsTraversed,
                                     request.dedupSize);

        return manager.serializer(g).writeMap(ImmutableMap.of("count", count));
    }

    private static List<CountTraverser.Step> steps(HugeGraph graph,
                                                   CountRequest request) {
        int stepSize = request.steps.size();
        List<CountTraverser.Step> steps = new ArrayList<>(stepSize);
        for (Step step : request.steps) {
            steps.add(step.jsonToStep(graph));
        }
        return steps;
    }

    private static class CountRequest {

        @JsonProperty("source")
        public Object source;
        @JsonProperty("steps")
        public List<Step> steps;
        @JsonProperty("contains_traversed")
        public boolean containsTraversed = false;
        @JsonProperty("dedup_size")
        public long dedupSize = 1000000L;

        @Override
        public String toString() {
            return String.format("CountRequest{source=%s,steps=%s," +
                                 "contains_traversed=%s,dedupSize=%s}",
                                 this.source, this.steps,
                                 this.containsTraversed, this.dedupSize);
        }
    }

    private static class Step {

        @JsonProperty("direction")
        public Directions direction = Directions.BOTH;
        @JsonProperty("labels")
        public List<String> labels;
        @JsonProperty("properties")
        public Map<String, Object> properties;
        @JsonProperty("degree")
        public long degree = Long.valueOf(DEFAULT_DEGREE);
        @JsonProperty("skip_degree")
        public long skipDegree = Long.valueOf(DEFAULT_SKIP_DEGREE);

        @Override
        public String toString() {
            return String.format("Step{direction=%s,labels=%s,properties=%s" +
                                 "degree=%s,skipDegree=%s}",
                                 this.direction, this.labels, this.properties,
                                 this.degree, this.skipDegree);
        }

        private CountTraverser.Step jsonToStep(HugeGraph graph) {
            return new CountTraverser.Step(graph, this.direction, this.labels,
                                           this.properties, this.degree,
                                           this.skipDegree);
        }
    }
}
