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
import static com.baidu.hugegraph.traversal.algorithm.HugeTraverser.DEFAULT_PATHS_LIMIT;
import static com.baidu.hugegraph.traversal.algorithm.HugeTraverser.NO_LIMIT;

import java.util.Iterator;

import javax.inject.Singleton;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;

import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;
import org.slf4j.Logger;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.api.API;
import com.baidu.hugegraph.backend.query.QueryResults;
import com.baidu.hugegraph.core.GraphManager;
import com.baidu.hugegraph.schema.EdgeLabel;
import com.baidu.hugegraph.server.RestServer;
import com.baidu.hugegraph.traversal.algorithm.FusiformSimilarityTraverser;
import com.baidu.hugegraph.traversal.algorithm.FusiformSimilarityTraverser.SimilarsMap;
import com.baidu.hugegraph.type.define.Directions;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.Log;
import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.annotation.JsonProperty;

@Path("graphs/{graph}/traversers/fusiformsimilarity")
@Singleton
public class FusiformSimilarityAPI extends API {

    private static final Logger LOG = Log.logger(RestServer.class);

    @POST
    @Timed
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String post(@Context GraphManager manager,
                       @PathParam("graph") String graph,
                       FusiformSimilarityRequest request) {
        E.checkArgumentNotNull(request, "The fusiform similarity " +
                               "request body can't be null");
        E.checkArgumentNotNull(request.sources,
                               "The sources of fusiform similarity " +
                               "request can't be null");
        if (request.direction == null) {
            request.direction = Directions.BOTH;
        }
        E.checkArgument(request.minNeighbors > 0,
                        "The min neighbor count must be > 0, but got: %s",
                        request.minNeighbors);
        E.checkArgument(request.degree > 0 || request.degree == NO_LIMIT,
                        "The degree of request must be > 0, but got: %s",
                        request.degree);
        E.checkArgument(request.alpha > 0 && request.alpha <= 1.0,
                        "The alpha of request must be in range (0, 1], " +
                        "but got '%s'", request.alpha);
        E.checkArgument(request.minSimilars >= 1,
                        "The min similar count of request must be >= 1, " +
                        "but got: %s", request.minSimilars);
        E.checkArgument(request.top >= 0,
                        "The top must be >= 0, but got: %s", request.top);

        LOG.debug("Graph [{}] get fusiform similars from '{}' with " +
                  "direction '{}', edge label '{}', min neighbor count '{}', " +
                  "alpha '{}', min similar count '{}', group property '{}' " +
                  "and min group count '{}'",
                  graph, request.sources, request.direction, request.label,
                  request.minNeighbors, request.alpha, request.minSimilars,
                  request.groupProperty, request.minGroups);

        HugeGraph g = graph(manager, graph);
        Iterator<Vertex> sources = request.sources.vertices(g);
        E.checkArgument(sources != null && sources.hasNext(),
                        "The source vertices can't be empty");
        EdgeLabel edgeLabel = request.label == null ?
                              null : g.edgeLabel(request.label);

        FusiformSimilarityTraverser traverser =
                                    new FusiformSimilarityTraverser(g);
        SimilarsMap result = traverser.fusiformSimilarity(
                             sources, request.direction, edgeLabel,
                             request.minNeighbors, request.alpha,
                             request.minSimilars, request.top,
                             request.groupProperty, request.minGroups,
                             request.degree, request.capacity, request.limit,
                             request.withIntermediary);

        CloseableIterator.closeIterator(sources);

        Iterator<Vertex> iterator = QueryResults.emptyIterator();
        if (request.withVertex) {
            iterator = g.vertices(result.vertices().toArray());
        }
        return manager.serializer(g).writeSimilars(result, iterator);
    }

    private static class FusiformSimilarityRequest {

        @JsonProperty("sources")
        public Vertices sources;
        @JsonProperty("label")
        public String label;
        @JsonProperty("direction")
        public Directions direction;
        @JsonProperty("min_neighbors")
        public int minNeighbors;
        @JsonProperty("alpha")
        public double alpha;
        @JsonProperty("min_similars")
        public int minSimilars = 1;
        @JsonProperty("top")
        public int top;
        @JsonProperty("group_property")
        public String groupProperty;
        @JsonProperty("min_groups")
        public int minGroups;
        @JsonProperty("max_degree")
        public long degree = Long.valueOf(DEFAULT_DEGREE);
        @JsonProperty("capacity")
        public long capacity = Long.valueOf(DEFAULT_CAPACITY);
        @JsonProperty("limit")
        public long limit = Long.valueOf(DEFAULT_PATHS_LIMIT);
        @JsonProperty("with_intermediary")
        public boolean withIntermediary = false;
        @JsonProperty("with_vertex")
        public boolean withVertex = false;

        @Override
        public String toString() {
            return String.format("FusiformSimilarityRequest{sources=%s," +
                                 "label=%s,direction=%s,minNeighbors=%s," +
                                 "alpha=%s,minSimilars=%s,top=%s," +
                                 "groupProperty=%s,minGroups=%s," +
                                 "degree=%s,capacity=%s,limit=%s," +
                                 "withIntermediary=%s,withVertex=%s}",
                                 this.sources, this.label, this.direction,
                                 this.minNeighbors, this.alpha,
                                 this.minSimilars, this.top,
                                 this.groupProperty, this.minGroups,
                                 this.degree, this.capacity, this.limit,
                                 this.withIntermediary, this.withVertex);
        }
    }
}
