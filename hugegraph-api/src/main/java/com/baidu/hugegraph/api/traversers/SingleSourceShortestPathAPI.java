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
import static com.baidu.hugegraph.traversal.algorithm.HugeTraverser.DEFAULT_PATHS_LIMIT;

import java.util.Iterator;

import jakarta.inject.Singleton;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.Context;

import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.api.API;
import com.baidu.hugegraph.api.graph.EdgeAPI;
import com.baidu.hugegraph.api.graph.VertexAPI;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.query.QueryResults;
import com.baidu.hugegraph.core.GraphManager;
import com.baidu.hugegraph.server.RestServer;
import com.baidu.hugegraph.traversal.algorithm.SingleSourceShortestPathTraverser;
import com.baidu.hugegraph.traversal.algorithm.SingleSourceShortestPathTraverser.WeightedPaths;
import com.baidu.hugegraph.type.define.Directions;
import com.baidu.hugegraph.util.Log;
import com.codahale.metrics.annotation.Timed;

@Path("graphs/{graph}/traversers/singlesourceshortestpath")
@Singleton
public class SingleSourceShortestPathAPI extends API {

    private static final Logger LOG = Log.logger(RestServer.class);

    @GET
    @Timed
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String get(@Context GraphManager manager,
                      @PathParam("graph") String graph,
                      @QueryParam("source") String source,
                      @QueryParam("direction") String direction,
                      @QueryParam("label") String edgeLabel,
                      @QueryParam("weight") String weight,
                      @QueryParam("max_degree")
                      @DefaultValue(DEFAULT_MAX_DEGREE) long maxDegree,
                      @QueryParam("skip_degree")
                      @DefaultValue("0") long skipDegree,
                      @QueryParam("capacity")
                      @DefaultValue(DEFAULT_CAPACITY) long capacity,
                      @QueryParam("limit")
                      @DefaultValue(DEFAULT_PATHS_LIMIT) long limit,
                      @QueryParam("with_vertex") boolean withVertex) {
        LOG.debug("Graph [{}] get single source shortest path from '{}' " +
                  "with direction {}, edge label {}, weight property {}, " +
                  "max degree '{}', limit '{}' and with vertex '{}'",
                  graph, source, direction, edgeLabel,
                  weight, maxDegree, withVertex);

        Id sourceId = VertexAPI.checkAndParseVertexId(source);
        Directions dir = Directions.convert(EdgeAPI.parseDirection(direction));

        HugeGraph g = graph(manager, graph);
        SingleSourceShortestPathTraverser traverser =
                new SingleSourceShortestPathTraverser(g);
        WeightedPaths paths = traverser.singleSourceShortestPaths(
                              sourceId, dir, edgeLabel, weight,
                              maxDegree, skipDegree, capacity, limit);
        Iterator<Vertex> iterator = QueryResults.emptyIterator();
        assert paths != null;
        if (!paths.isEmpty() && withVertex) {
            iterator = g.vertices(paths.vertices().toArray());
        }
        return manager.serializer(g).writeWeightedPaths(paths, iterator);
    }
}
