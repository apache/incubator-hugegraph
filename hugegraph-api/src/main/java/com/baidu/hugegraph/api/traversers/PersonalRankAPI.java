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
import static com.baidu.hugegraph.traversal.algorithm.HugeTraverser.DEFAULT_LIMIT;
import static com.baidu.hugegraph.traversal.algorithm.HugeTraverser.NO_LIMIT;

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
import com.baidu.hugegraph.server.RestServer;
import com.baidu.hugegraph.traversal.algorithm.PersonalRankTraverser;
import com.baidu.hugegraph.util.CollectionUtil;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.InsertionOrderUtil;
import com.baidu.hugegraph.util.Log;
import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.annotation.JsonProperty;

@Path("graphs/{graph}/traversers/personalrank")
@Singleton
public class PersonalRankAPI extends API {

    private static final Logger LOG = Log.logger(RestServer.class);

    @POST
    @Timed
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String personalRank(@Context GraphManager manager,
                               @PathParam("graph") String graph,
                               RankRequest request) {
        E.checkArgumentNotNull(request, "The rank request body can't be null");
        E.checkArgument(request.source != null,
                        "The source vertex id of rank request can't be null");
        E.checkArgument(request.label != null,
                        "The edge label of rank request can't be null");
        E.checkArgument(request.alpha > 0 && request.alpha <= 1.0,
                        "The alpha of rank request must belong (0, 1], " +
                        "but got '%s'", request.alpha);
        E.checkArgument(request.degree > 0 || request.degree == NO_LIMIT,
                        "The degree of rank request must be > 0, but got: %s",
                        request.degree);
        E.checkArgument(request.limit > 0 || request.limit == NO_LIMIT,
                        "The limit of rank request must be > 0, but got: %s",
                        request.limit);
        E.checkArgument(request.maxDepth >= 1,
                        "The max depth of rank request must >= 1, but got '%s'",
                        request.maxDepth);

        LOG.debug("Graph [{}] get personal rank from '{}' with " +
                  "edge label '{}', alpha '{}', degree '{}', " +
                  "max depth '{}' and sorted '{}'",
                  graph, request.source, request.label, request.alpha,
                  request.degree, request.maxDepth, request.sorted);

        Id sourceId = VertexAPI.checkAndParseVertexId(request.source);
        HugeGraph g = graph(manager, graph);

        PersonalRankTraverser traverser;
        traverser = new PersonalRankTraverser(g, request.alpha, request.degree,
                                              request.maxDepth);
        Map<Id, Double> ranks = traverser.personalRank(sourceId, request.label,
                                                       request.withLabel);
        ranks = topN(ranks, request.sorted, request.limit);
        return manager.serializer(g).writeMap(ranks);
    }

    private static Map<Id, Double> topN(Map<Id, Double> ranks,
                                        boolean sorted, long limit) {
        if (sorted) {
            ranks = CollectionUtil.sortByValue(ranks, false);
        }
        Map<Id, Double> results = InsertionOrderUtil.newMap();
        long count = 0;
        for (Map.Entry<Id, Double> entry : ranks.entrySet()) {
            results.put(entry.getKey(), entry.getValue());
            if (++count >= limit) {
                break;
            }
        }
        return results;
    }

    private static class RankRequest {

        @JsonProperty("source")
        private String source;
        @JsonProperty("label")
        private String label;
        @JsonProperty("alpha")
        private double alpha;
        @JsonProperty("degree")
        private long degree = Long.valueOf(DEFAULT_DEGREE);
        @JsonProperty("limit")
        private long limit = Long.valueOf(DEFAULT_LIMIT);
        @JsonProperty("max_depth")
        private int maxDepth;
        @JsonProperty("with_label")
        private PersonalRankTraverser.WithLabel withLabel =
                PersonalRankTraverser.WithLabel.BOTH_LABEL;
        @JsonProperty("sorted")
        private boolean sorted = true;

        @Override
        public String toString() {
            return String.format("RankRequest{source=%s,label=%s," +
                                 "alpha=%s,degree=%s,limit=%s, maxDepth=%s," +
                                 "withLabel=%s,sorted=%s}",
                                 this.source, this.label, this.alpha,
                                 this.degree, this.limit, this.maxDepth,
                                 this.withLabel, this.sorted);
        }
    }
}
