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

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import static com.baidu.hugegraph.traversal.algorithm.HugeTraverser.DEFAULT_CAPACITY;
import static com.baidu.hugegraph.traversal.algorithm.HugeTraverser.DEFAULT_DEGREE;
import static com.baidu.hugegraph.traversal.algorithm.HugeTraverser.DEFAULT_PATHS_LIMIT;

import javax.inject.Singleton;
import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;

import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.api.graph.EdgeAPI;
import com.baidu.hugegraph.api.graph.VertexAPI;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.query.QueryResults;
import com.baidu.hugegraph.core.GraphManager;
import com.baidu.hugegraph.server.RestServer;
import com.baidu.hugegraph.traversal.algorithm.CollectionPathsTraverser;
import com.baidu.hugegraph.traversal.algorithm.EdgeStep;
import com.baidu.hugegraph.traversal.algorithm.HugeTraverser;
import com.baidu.hugegraph.traversal.algorithm.PathsTraverser;
import com.baidu.hugegraph.type.define.Directions;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.Log;
import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.annotation.JsonProperty;

@Path("graphs/{graph}/traversers/paths")
@Singleton
public class PathsAPI extends TraverserAPI {

    private static final Logger LOG = Log.logger(RestServer.class);

    @POST
    @Timed
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String post(@Context GraphManager manager,
                       @PathParam("graph") String graph,
                       Request request) {
        E.checkArgumentNotNull(request, "The request body can't be null");
        E.checkArgumentNotNull(request.sources,
                               "The sources of request can't be null");
        E.checkArgumentNotNull(request.targets,
                               "The targets of request can't be null");
        E.checkArgumentNotNull(request.step,
                               "The step of request can't be null");
        E.checkArgument(request.depth > 0,
                        "The depth of request must be > 0, but got: %s",
                        request.depth);

        LOG.debug("Graph [{}] get paths from source vertices '{}', target " +
                  "vertices '{}', with step '{}', max depth '{}', " +
                  "capacity '{}', limit '{}' and with_vertex '{}'",
                  graph, request.sources, request.targets, request.step,
                  request.depth, request.capacity, request.limit,
                  request.withVertex);

        HugeGraph g = graph(manager, graph);
        Iterator<Vertex> sources = request.sources.vertices(g);
        Iterator<Vertex> targets = request.targets.vertices(g);
        EdgeStep step = step(g, request.step);

        CollectionPathsTraverser traverser = new CollectionPathsTraverser(g);
        Collection<HugeTraverser.Path> paths;
        paths = traverser.paths(sources, targets, step, request.depth,
                                request.capacity, request.limit);

        if (!request.withVertex) {
            return manager.serializer(g).writePaths("paths", paths, false);
        }

        Set<Id> ids = new HashSet<>();
        for (HugeTraverser.Path p : paths) {
            ids.addAll(p.vertices());
        }
        Iterator<Vertex> iter = QueryResults.emptyIterator();
        if (!ids.isEmpty()) {
            iter = g.vertices(ids.toArray());
        }
        return manager.serializer(g).writePaths("paths", paths, false, iter);
    }

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
                      @DefaultValue(DEFAULT_DEGREE) long degree,
                      @QueryParam("capacity")
                      @DefaultValue(DEFAULT_CAPACITY) long capacity,
                      @QueryParam("limit")
                      @DefaultValue(DEFAULT_PATHS_LIMIT) long limit) {
        LOG.debug("Graph [{}] get paths from '{}', to '{}' with " +
                  "direction {}, edge label {}, max depth '{}', " +
                  "max degree '{}', capacity '{}' and limit '{}'",
                  graph, source, target, direction, edgeLabel, depth,
                  degree, capacity, limit);

        Id sourceId = VertexAPI.checkAndParseVertexId(source);
        Id targetId = VertexAPI.checkAndParseVertexId(target);
        Directions dir = Directions.convert(EdgeAPI.parseDirection(direction));

        HugeGraph g = graph(manager, graph);
        PathsTraverser traverser = new PathsTraverser(g);
        HugeTraverser.PathSet paths = traverser.paths(sourceId, dir, targetId,
                                                      dir.opposite(), edgeLabel,
                                                      depth, degree, capacity,
                                                      limit);
        return manager.serializer(g).writePaths("paths", paths, false);
    }

    private static class Request {

        @JsonProperty("sources")
        public Vertices sources;
        @JsonProperty("targets")
        public Vertices targets;
        @JsonProperty("step")
        public TraverserAPI.Step step;
        @JsonProperty("max_depth")
        public int depth;
        @JsonProperty("capacity")
        public long capacity = Long.valueOf(DEFAULT_CAPACITY);
        @JsonProperty("limit")
        public long limit = Long.valueOf(DEFAULT_PATHS_LIMIT);
        @JsonProperty("with_vertex")
        public boolean withVertex = false;

        @Override
        public String toString() {
            return String.format("PathRequest{sources=%s,targets=%s,step=%s," +
                                 "maxDepth=%s,capacity=%s,limit=%s," +
                                 "withVertex=%s}", this.sources, this.targets,
                                 this.step, this.depth, this.capacity,
                                 this.limit, this.withVertex);
        }
    }
}
