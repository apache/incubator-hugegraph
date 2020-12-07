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
        E.checkArgument(request.steps.size() <= Long.parseLong(DEFAULT_MAX_DEPTH),
                        "The steps length of rank request can't exceed %s",
                        DEFAULT_MAX_DEPTH);
        E.checkArgument(request.alpha > 0 && request.alpha <= 1.0,
                        "The alpha of rank request must be in range (0, 1], " +
                        "but got '%s'", request.alpha);

        LOG.debug("Graph [{}] get neighbor rank from '{}' with steps '{}', " +
                  "alpha '{}' and capacity '{}'", graph, request.source,
                  request.steps, request.alpha, request.capacity);

        Id sourceId = HugeVertex.getIdValue(request.source);
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
        public long capacity = Long.parseLong(DEFAULT_CAPACITY);

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
        public long degree = Long.parseLong(DEFAULT_DEGREE);
        @JsonProperty("skip_degree")
        public long skipDegree = 0L;
        @JsonProperty("top")
        public int top = Integer.parseInt(DEFAULT_PATHS_LIMIT);

        public static final int DEFAULT_CAPACITY_PER_LAYER = 100000;

        @Override
        public String toString() {
            return String.format("Step{direction=%s,labels=%s,degree=%s," +
                                 "top=%s}", this.direction, this.labels,
                                 this.degree, this.top);
        }

        private NeighborRankTraverser.Step jsonToStep(HugeGraph g) {
            return new NeighborRankTraverser.Step(g, this.direction,
                                                  this.labels,
                                                  this.degree,
                                                  this.skipDegree,
                                                  this.top,
                                                  DEFAULT_CAPACITY_PER_LAYER);
        }
    }
}
