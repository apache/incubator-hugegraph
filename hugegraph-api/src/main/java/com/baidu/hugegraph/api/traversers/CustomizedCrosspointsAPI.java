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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
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
import com.baidu.hugegraph.api.API;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.query.QueryResults;
import com.baidu.hugegraph.core.GraphManager;
import com.baidu.hugegraph.server.RestServer;
import com.baidu.hugegraph.traversal.algorithm.CustomizedCrosspointsTraverser;
import com.baidu.hugegraph.traversal.algorithm.HugeTraverser;
import com.baidu.hugegraph.type.define.Directions;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.Log;
import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.annotation.JsonProperty;

@Path("graphs/{graph}/traversers/customizedcrosspoints")
@Singleton
public class CustomizedCrosspointsAPI extends API {

    private static final Logger LOG = Log.logger(RestServer.class);

    @POST
    @Timed
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String post(@Context GraphManager manager,
                       @PathParam("graph") String graph,
                       CrosspointsRequest request) {
        E.checkArgumentNotNull(request,
                               "The crosspoints request body can't be null");
        E.checkArgumentNotNull(request.sources,
                               "The sources of crosspoints request " +
                               "can't be null");
        E.checkArgument(request.pathPatterns != null &&
                        !request.pathPatterns.isEmpty(),
                        "The steps of crosspoints request can't be empty");

        LOG.debug("Graph [{}] get customized crosspoints from source vertex " +
                  "'{}', with path_pattern '{}', with_path '{}', with_vertex " +
                  "'{}', capacity '{}' and limit '{}'", graph, request.sources,
                  request.pathPatterns, request.withPath, request.withVertex,
                  request.capacity, request.limit);

        HugeGraph g = graph(manager, graph);
        Iterator<Vertex> sources = request.sources.vertices(g);
        List<CustomizedCrosspointsTraverser.PathPattern> patterns;
        patterns = pathPatterns(g, request);

        CustomizedCrosspointsTraverser traverser =
                                       new CustomizedCrosspointsTraverser(g);
        CustomizedCrosspointsTraverser.CrosspointsPaths paths;
        paths = traverser.crosspointsPaths(sources, patterns, request.capacity,
                                           request.limit);
        Iterator<Vertex> iter = QueryResults.emptyIterator();
        if (!request.withVertex) {
            return manager.serializer(g).writeCrosspoints(paths, iter,
                                                          request.withPath);
        }
        Set<Id> ids = new HashSet<>();
        if (request.withPath) {
            for (HugeTraverser.Path p : paths.paths()) {
                ids.addAll(p.vertices());
            }
        } else {
            ids = paths.crosspoints();
        }
        if (!ids.isEmpty()) {
            iter = g.vertices(ids.toArray());
        }
        return manager.serializer(g).writeCrosspoints(paths, iter,
                                                      request.withPath);
    }

    private static List<CustomizedCrosspointsTraverser.PathPattern>
                   pathPatterns(HugeGraph graph, CrosspointsRequest request) {
        int stepSize = request.pathPatterns.size();
        List<CustomizedCrosspointsTraverser.PathPattern> pathPatterns;
        pathPatterns = new ArrayList<>(stepSize);
        for (PathPattern pattern : request.pathPatterns) {
            CustomizedCrosspointsTraverser.PathPattern pathPattern;
            pathPattern = new CustomizedCrosspointsTraverser.PathPattern();
            for (Step step : pattern.steps) {
                pathPattern.add(step.jsonToStep(graph));
            }
            pathPatterns.add(pathPattern);
        }
        return pathPatterns;
    }

    private static class CrosspointsRequest {

        @JsonProperty("sources")
        public Vertices sources;
        @JsonProperty("path_patterns")
        public List<PathPattern> pathPatterns;
        @JsonProperty("capacity")
        public long capacity = Long.valueOf(DEFAULT_CAPACITY);
        @JsonProperty("limit")
        public long limit = Long.valueOf(DEFAULT_PATHS_LIMIT);
        @JsonProperty("with_path")
        public boolean withPath = false;
        @JsonProperty("with_vertex")
        public boolean withVertex = false;

        @Override
        public String toString() {
            return String.format("CrosspointsRequest{sourceVertex=%s," +
                                 "pathPatterns=%s,withPath=%s,withVertex=%s," +
                                 "capacity=%s,limit=%s}", this.sources,
                                 this.pathPatterns, this.withPath,
                                 this.withVertex, this.capacity, this.limit);
        }
    }

    private static class PathPattern {

        @JsonProperty("steps")
        public List<Step> steps;

        @Override
        public String toString() {
            return String.format("PathPattern{steps=%s", this.steps);
        }
    }

    private static class Step {

        @JsonProperty("direction")
        public Directions direction;
        @JsonProperty("labels")
        public List<String> labels;
        @JsonProperty("properties")
        public Map<String, Object> properties;
        @JsonProperty("degree")
        public long degree = Long.valueOf(DEFAULT_DEGREE);
        @JsonProperty("skip_degree")
        public long skipDegree = 0L;

        @Override
        public String toString() {
            return String.format("Step{direction=%s,labels=%s,properties=%s," +
                                 "degree=%s,skipDegree=%s}",
                                 this.direction, this.labels, this.properties,
                                 this.degree, this.skipDegree);
        }

        private CustomizedCrosspointsTraverser.Step jsonToStep(HugeGraph g) {
            return new CustomizedCrosspointsTraverser.Step(g, this.direction,
                                                           this.labels,
                                                           this.properties,
                                                           this.degree,
                                                           this.skipDegree);
        }
    }
}
