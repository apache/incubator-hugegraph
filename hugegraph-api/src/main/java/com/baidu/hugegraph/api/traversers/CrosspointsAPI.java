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

import javax.inject.Singleton;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;

import org.slf4j.Logger;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.api.API;
import com.baidu.hugegraph.api.graph.EdgeAPI;
import com.baidu.hugegraph.api.graph.VertexAPI;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.core.GraphManager;
import com.baidu.hugegraph.server.RestServer;
import com.baidu.hugegraph.traversal.algorithm.HugeTraverser;
import com.baidu.hugegraph.traversal.algorithm.PathsTraverser;
import com.baidu.hugegraph.type.define.Directions;
import com.baidu.hugegraph.util.Log;
import com.codahale.metrics.annotation.Timed;

@Path("graphs/{graph}/traversers/crosspoints")
@Singleton
public class CrosspointsAPI extends API {

    private static final Logger LOG = Log.logger(RestServer.class);

    @GET
    @Timed
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String get(@Context GraphManager manager,
                      @PathParam("graph") String graph,
                      @QueryParam("source") String source,
                      @QueryParam("target") String target,
                      @QueryParam("direction") String direction,
                      @QueryParam("label") String edgeLabel,
                      @QueryParam("max_depth") int depth,
                      @QueryParam("max_degree")
                      @DefaultValue(DEFAULT_MAX_DEGREE) long maxDegree,
                      @QueryParam("capacity")
                      @DefaultValue(DEFAULT_CAPACITY) long capacity,
                      @QueryParam("limit")
                      @DefaultValue(DEFAULT_PATHS_LIMIT) long limit) {
        LOG.debug("Graph [{}] get crosspoints with paths from '{}', to '{}' " +
                  "with direction '{}', edge label '{}', max depth '{}', " +
                  "max degree '{}', capacity '{}' and limit '{}'",
                  graph, source, target, direction, edgeLabel,
                  depth, maxDegree, capacity, limit);

        Id sourceId = VertexAPI.checkAndParseVertexId(source);
        Id targetId = VertexAPI.checkAndParseVertexId(target);
        Directions dir = Directions.convert(EdgeAPI.parseDirection(direction));

        HugeGraph g = graph(manager, graph);
        PathsTraverser traverser = new PathsTraverser(g);
        HugeTraverser.PathSet paths = traverser.paths(sourceId, dir, targetId,
                                                      dir, edgeLabel, depth,
                                                      maxDegree, capacity, limit);
        return manager.serializer(g).writePaths("crosspoints",
                                                paths.paths(),true);
    }
}
