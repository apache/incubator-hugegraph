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
import static com.baidu.hugegraph.traversal.algorithm.HugeTraverser.DEFAULT_MAX_DEGREE;
import static com.baidu.hugegraph.traversal.algorithm.HugeTraverser.DEFAULT_LIMIT;

import java.util.Map;

import jakarta.inject.Singleton;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.Context;

import org.slf4j.Logger;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.api.graph.EdgeAPI;
import com.baidu.hugegraph.api.graph.VertexAPI;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.core.GraphManager;
import com.baidu.hugegraph.server.RestServer;
import com.baidu.hugegraph.structure.HugeVertex;
import com.baidu.hugegraph.traversal.algorithm.steps.EdgeStep;
import com.baidu.hugegraph.traversal.algorithm.JaccardSimilarTraverser;
import com.baidu.hugegraph.type.define.Directions;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.JsonUtil;
import com.baidu.hugegraph.util.Log;
import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;

@Path("graphs/{graph}/traversers/jaccardsimilarity")
@Singleton
public class JaccardSimilarityAPI extends TraverserAPI {

    private static final Logger LOG = Log.logger(RestServer.class);

    @GET
    @Timed
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String get(@Context GraphManager manager,
                      @PathParam("graph") String graph,
                      @QueryParam("vertex") String vertex,
                      @QueryParam("other") String other,
                      @QueryParam("direction") String direction,
                      @QueryParam("label") String edgeLabel,
                      @QueryParam("max_degree")
                      @DefaultValue(DEFAULT_MAX_DEGREE) long maxDegree) {
        LOG.debug("Graph [{}] get jaccard similarity between '{}' and '{}' " +
                  "with direction {}, edge label {} and max degree '{}'",
                  graph, vertex, other, direction, edgeLabel, maxDegree);

        Id sourceId = VertexAPI.checkAndParseVertexId(vertex);
        Id targetId = VertexAPI.checkAndParseVertexId(other);
        Directions dir = Directions.convert(EdgeAPI.parseDirection(direction));

        HugeGraph g = graph(manager, graph);
        double similarity;
        try (JaccardSimilarTraverser traverser =
                                     new JaccardSimilarTraverser(g)) {
             similarity = traverser.jaccardSimilarity(sourceId, targetId, dir,
                                                      edgeLabel, maxDegree);
        }
        return JsonUtil.toJson(ImmutableMap.of("jaccard_similarity",
                                               similarity));
    }

    @POST
    @Timed
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String post(@Context GraphManager manager,
                       @PathParam("graph") String graph,
                       Request request) {
        E.checkArgumentNotNull(request, "The request body can't be null");
        E.checkArgumentNotNull(request.vertex,
                               "The source vertex of request can't be null");
        E.checkArgument(request.step != null,
                        "The steps of request can't be null");
        E.checkArgument(request.top >= 0,
                        "The top must be >= 0, but got: %s", request.top);

        LOG.debug("Graph [{}] get jaccard similars from source vertex '{}', " +
                  "with step '{}', top '{}' and capacity '{}'",
                  graph, request.vertex, request.step,
                  request.top, request.capacity);

        HugeGraph g = graph(manager, graph);
        Id sourceId = HugeVertex.getIdValue(request.vertex);

        EdgeStep step = step(g, request.step);

        Map<Id, Double> results;
        try (JaccardSimilarTraverser traverser =
                                     new JaccardSimilarTraverser(g)) {
            results = traverser.jaccardSimilars(sourceId, step, request.top,
                                                request.capacity);
        }
        return manager.serializer(g).writeMap(results);
    }

    private static class Request {

        @JsonProperty("vertex")
        public Object vertex;
        @JsonProperty("step")
        public TraverserAPI.Step step;
        @JsonProperty("top")
        public int top = Integer.parseInt(DEFAULT_LIMIT);
        @JsonProperty("capacity")
        public long capacity = Long.parseLong(DEFAULT_CAPACITY);

        @Override
        public String toString() {
            return String.format("Request{vertex=%s,step=%s,top=%s," +
                                 "capacity=%s}", this.vertex, this.step,
                                 this.top, this.capacity);
        }
    }
}
