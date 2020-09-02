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

import java.util.Map;

import javax.inject.Singleton;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;

import org.slf4j.Logger;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.core.GraphManager;
import com.baidu.hugegraph.server.RestServer;
import com.baidu.hugegraph.structure.HugeVertex;
import com.baidu.hugegraph.traversal.algorithm.EdgeStep;
import com.baidu.hugegraph.traversal.algorithm.JaccardSimilarTraverser;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.Log;
import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.annotation.JsonProperty;

import static com.baidu.hugegraph.traversal.algorithm.HugeTraverser
        .DEFAULT_CAPACITY;
import static com.baidu.hugegraph.traversal.algorithm.HugeTraverser.DEFAULT_LIMIT;

@Path("graphs/{graph}/traversers/jaccardsimilar")
@Singleton
public class JaccardSimilarAPI extends TraverserAPI {

    private static final Logger LOG = Log.logger(RestServer.class);

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
                  graph, request.vertex, request.step, request.top);

        HugeGraph g = graph(manager, graph);
        Id sourceId = HugeVertex.getIdValue(request.vertex);

        EdgeStep step = step(g, request.step);

        JaccardSimilarTraverser traverser = new JaccardSimilarTraverser(g);
        Map<Id, Double> results = traverser.jaccardSimilars(sourceId, step,
                                                            request.top,
                                                            request.capacity);
        return manager.serializer(g).writeMap(results);
    }

    private static class Request {

        @JsonProperty("vertex")
        public Object vertex;
        @JsonProperty("step")
        public TraverserAPI.Step step;
        @JsonProperty("top")
        public int top = Integer.valueOf(DEFAULT_LIMIT);
        @JsonProperty("capacity")
        public long capacity = Long.valueOf(DEFAULT_CAPACITY);

        @Override
        public String toString() {
            return String.format("Request{vertex=%s,step=%s,top=%s," +
                                 "capacity=%s}", this.vertex, this.step,
                                 this.top, this.capacity);
        }
    }
}
