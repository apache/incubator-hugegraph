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

import static com.baidu.hugegraph.traversal.algorithm.HugeTraverser.DEFAULT_ELEMENTS_LIMIT;
import static com.baidu.hugegraph.traversal.algorithm.HugeTraverser.DEFAULT_MAX_DEGREE;

import java.util.Set;
import java.util.List;
import java.util.ArrayList;

import javax.inject.Singleton;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;

import org.slf4j.Logger;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.api.API;
import com.baidu.hugegraph.api.graph.EdgeAPI;
import com.baidu.hugegraph.api.graph.VertexAPI;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.id.IdGenerator;
import com.baidu.hugegraph.core.GraphManager;
import com.baidu.hugegraph.server.RestServer;
import com.baidu.hugegraph.traversal.algorithm.SameNeighborTraverser;
import com.baidu.hugegraph.type.define.Directions;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.Log;
import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

@Path("graphs/{graph}/traversers/sameneighborsbatch")
@Singleton
public class SameNeighborsBatchAPI extends API {

    private static final Logger LOG = Log.logger(RestServer.class);

    @POST
    @Timed
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String sameNeighborsBatch(@Context GraphManager manager,
                      @PathParam("graph") String graph,
                      Request req) {
        LOG.debug("Graph [{}] get same neighbors batch, '{}'", graph, req.toString());

        Directions dir = Directions.convert(EdgeAPI.parseDirection(req.direction));

        HugeGraph g = graph(manager, graph);
        SameNeighborTraverser traverser = new SameNeighborTraverser(g);

        List<Set<Id>> result = new ArrayList<Set<Id>>();
        for(List<String> vertexPair : req.vertexList) {
            E.checkArgument(vertexPair.size() == 2, "vertex pair length error");
            Id sourceId = VertexAPI.checkAndParseVertexId(vertexPair.get(0));
            Id targetId = VertexAPI.checkAndParseVertexId(vertexPair.get(1));
            Set<Id> neighbors = traverser.sameNeighbors(sourceId, targetId, dir, 
                req.label, req.maxDegree, req.limit);
            result.add(neighbors);
        }
        
        return manager.serializer(g).writeList("same_neighbors", result);
    }

    private static class Request {
        @JsonProperty("vertex_list")
        private List<List<String>> vertexList;
        @JsonProperty("direction")
        private String direction;
        @JsonProperty("label")
        private String label;
        @JsonProperty("max_degree")
        public long maxDegree = Long.parseLong(DEFAULT_MAX_DEGREE);
        @JsonProperty("limit")
        public long limit = Long.parseLong(DEFAULT_ELEMENTS_LIMIT);

        @Override
        public String toString() {
            ObjectMapper om = new ObjectMapper();
            String vertexStr = "";
            try {
                vertexStr = om.writeValueAsString(this.vertexList);
            } catch (Exception e) {
            }
            return String.format("SameNeighborsBatchRequest{vertex=%s,direction=%s,label=%s,"+
                "max_degree=%d,limit=%d", vertexStr, this.direction,
                this.label, this.maxDegree, this.limit);
        }
    }
}
