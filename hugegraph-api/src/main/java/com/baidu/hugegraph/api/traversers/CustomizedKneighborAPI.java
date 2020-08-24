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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import javax.inject.Singleton;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;

import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.query.QueryResults;
import com.baidu.hugegraph.core.GraphManager;
import com.baidu.hugegraph.server.RestServer;
import com.baidu.hugegraph.structure.HugeVertex;
import com.baidu.hugegraph.traversal.algorithm.CustomizedKneighborTraverser;
import com.baidu.hugegraph.traversal.algorithm.EdgeStep;
import com.baidu.hugegraph.traversal.algorithm.HugeTraverser;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.Log;
import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.annotation.JsonProperty;

import static com.baidu.hugegraph.traversal.algorithm.HugeTraverser.*;

@Path("graphs/{graph}/traversers/customizedkneighbor")
@Singleton
public class CustomizedKneighborAPI extends TraverserAPI {

    private static final Logger LOG = Log.logger(RestServer.class);

    @POST
    @Timed
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String post(@Context GraphManager manager,
                       @PathParam("graph") String graph,
                       Request request) {
        E.checkArgumentNotNull(request, "The request body can't be null");
        E.checkArgumentNotNull(request.source,
                               "The source of request can't be null");
        E.checkArgument(request.step != null,
                        "The steps of request can't be null");
        if (request.countOnly) {
            E.checkArgument(!request.withVertex && !request.withPath,
                            "Can't return vertex or path when count only");
        }

        LOG.debug("Graph [{}] get customized kneighbor from source vertex " +
                  "'{}', with step '{}', limit '{}', count_only '{}', " +
                  "with_vertex '{}' and with_path '{}'",
                  graph, request.source, request.step, request.limit,
                  request.countOnly, request.withVertex, request.withPath);

        HugeGraph g = graph(manager, graph);
        Id sourceId = HugeVertex.getIdValue(request.source);

        EdgeStep step = step(g, request.step);

        CustomizedKneighborTraverser traverser =
                                     new CustomizedKneighborTraverser(g);
        Set<Node> results = traverser.customizedKneighbor(sourceId, step,
                                                          request.maxDepth,
                                                          request.limit);
        Set<Id> neighbors = new HashSet<>();
        for (Node node : results) {
            neighbors.add(node.id());
        }

        List<HugeTraverser.Path> paths = new ArrayList<>();
        if (request.withPath) {
            for (Node node : results) {
                paths.add(new HugeTraverser.Path(node.path()));
            }
        }
        Iterator<Vertex> iter = QueryResults.emptyIterator();
        if (request.withVertex) {
            Set<Id> ids = new HashSet<>();
            for (HugeTraverser.Path p : paths) {
                ids.addAll(p.vertices());
            }
            if (!ids.isEmpty()) {
                iter = g.vertices(ids.toArray());
            }
        }
        return manager.serializer(g).writeNodes("kneighbor", neighbors,
                                                paths, iter, request.countOnly);
    }

    private static class Request {

        @JsonProperty("source")
        public Object source;
        @JsonProperty("step")
        public TraverserAPI.Step step;
        @JsonProperty("max_depth")
        public int maxDepth;
        @JsonProperty("limit")
        public long limit = Long.valueOf(DEFAULT_PATHS_LIMIT);
        @JsonProperty("count_only")
        public boolean countOnly = false;
        @JsonProperty("with_vertex")
        public boolean withVertex = false;
        @JsonProperty("with_path")
        public boolean withPath = false;

        @Override
        public String toString() {
            return String.format("PathRequest{source=%s,step=%s,maxDepth=%s" +
                                 "limit=%s,countOnly=%s,withVertex=%s," +
                                 "withPath=%s}", this.source, this.step,
                                 this.maxDepth, this.limit, this.countOnly,
                                 this.withVertex, this.withPath);
        }
    }
}
