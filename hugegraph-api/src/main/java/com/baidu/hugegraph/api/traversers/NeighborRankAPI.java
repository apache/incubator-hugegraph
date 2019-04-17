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

import static com.baidu.hugegraph.traversal.algorithm.HugeTraverser.DEFAULT_CAPACITY;
import static com.baidu.hugegraph.traversal.algorithm.HugeTraverser.DEFAULT_DEGREE;
import static com.baidu.hugegraph.traversal.algorithm.HugeTraverser.DEFAULT_MAX_DEPTH;
import static com.baidu.hugegraph.traversal.algorithm.HugeTraverser.DEFAULT_PATHS_LIMIT;
import static com.baidu.hugegraph.traversal.algorithm.HugeTraverser.NO_LIMIT;
import static com.baidu.hugegraph.traversal.algorithm.NeighborRankTraverser.MAX_TOP;

import java.util.ArrayList;
import java.util.HashMap;
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
import com.baidu.hugegraph.api.graph.VertexAPI;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.core.GraphManager;
import com.baidu.hugegraph.schema.EdgeLabel;
import com.baidu.hugegraph.server.RestServer;
import com.baidu.hugegraph.traversal.algorithm.NeighborRankTraverser;
import com.baidu.hugegraph.type.define.Directions;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.Log;
import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.annotation.JsonProperty;

@Path("graphs/{graph}/traversers/neighborrank")
@Singleton
public class NeighborRankAPI extends API {

    private static final Logger LOG = Log.logger(RestServer.class);

    @POST
    @Timed
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String neighborRank(@Context GraphManager manager,
                               @PathParam("graph") String graph,
                               RankRequest request) {
        E.checkArgumentNotNull(request, "The rank request body can't be null");
        E.checkArgumentNotNull(request.source,
                               "The source of rank request can't be null");
        E.checkArgument(request.steps != null && !request.steps.isEmpty(),
                        "The steps of rank request can't be empty");
        E.checkArgument(request.steps.size() <= Long.valueOf(DEFAULT_MAX_DEPTH),
                        "The steps length of rank request can't exceed %s",
                        DEFAULT_MAX_DEPTH);
        E.checkArgument(request.alpha > 0 && request.alpha <= 1.0,
                        "The alpha of rank request must be in range (0, 1], " +
                        "but got '%s'", request.alpha);

        LOG.debug("Graph [{}] get neighbor rank from '{}' with steps '{}', " +
                  "alpha '{}' and capacity '{}'", graph, request.source,
                  request.steps, request.alpha, request.capacity);

        Id sourceId = VertexAPI.checkAndParseVertexId(request.source);
        HugeGraph g = graph(manager, graph);

        List<NeighborRankTraverser.Step> steps = steps(g, request);
        NeighborRankTraverser traverser;
        traverser = new NeighborRankTraverser(g, request.alpha,
                                              request.capacity);
        List<Map<Id, Double>> ranks = traverser.neighborRank(sourceId, steps);
        return manager.serializer(g).writeList("ranks", ranks);
    }

    private static List<NeighborRankTraverser.Step> steps(HugeGraph graph,
                                                          RankRequest req) {
        List<NeighborRankTraverser.Step> steps = new ArrayList<>();
        for (Step step : req.steps) {
            steps.add(step.jsonToStep(graph));
        }
        return steps;
    }

    private static class RankRequest {

        @JsonProperty("source")
        private String source;
        @JsonProperty("steps")
        private List<Step> steps;
        @JsonProperty("alpha")
        private double alpha;
        @JsonProperty("capacity")
        public long capacity = Long.valueOf(DEFAULT_CAPACITY);

        @Override
        public String toString() {
            return String.format("RankRequest{source=%s,steps=%s,alpha=%s," +
                                 "capacity=%s}", this.source, this.steps,
                                 this.alpha, this.capacity);
        }
    }

    private static class Step {

        @JsonProperty("direction")
        public Directions direction;
        @JsonProperty("labels")
        public List<String> labels;
        @JsonProperty("degree")
        public long degree = Long.valueOf(DEFAULT_DEGREE);
        @JsonProperty("top")
        public int top = Integer.valueOf(DEFAULT_PATHS_LIMIT);

        @Override
        public String toString() {
            return String.format("Step{direction=%s,labels=%s,degree=%s," +
                                 "top=%s}", this.direction, this.labels,
                                 this.degree, this.top);
        }

        private NeighborRankTraverser.Step jsonToStep(HugeGraph graph) {
            E.checkArgument(this.degree > 0 || this.degree == NO_LIMIT,
                            "The degree must be > 0, but got: %s",
                            this.degree);
            E.checkArgument(this.top > 0 && this.top <= MAX_TOP,
                            "The top of each layer cannot exceed %s", MAX_TOP);
            Map<Id, String> labelIds = new HashMap<>();
            if (this.labels != null) {
                for (String label : this.labels) {
                    EdgeLabel el = graph.edgeLabel(label);
                    labelIds.put(el.id(), label);
                }
            }
            return new NeighborRankTraverser.Step(this.direction, labelIds,
                                                  this.degree, this.top);
        }
    }
}
