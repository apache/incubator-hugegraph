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
import java.util.Collections;
import java.util.HashMap;
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
import com.baidu.hugegraph.api.filter.StatusFilter.Status;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.core.GraphManager;
import com.baidu.hugegraph.schema.EdgeLabel;
import com.baidu.hugegraph.schema.PropertyKey;
import com.baidu.hugegraph.server.RestServer;
import com.baidu.hugegraph.structure.HugeVertex;
import com.baidu.hugegraph.traversal.algorithm.CustomizePathsTraverser;
import com.baidu.hugegraph.traversal.algorithm.HugeTraverser;
import com.baidu.hugegraph.type.define.Directions;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.Log;
import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.annotation.JsonProperty;

import static com.baidu.hugegraph.traversal.algorithm.HugeTraverser.*;

@Path("graphs/{graph}/traversers/customizedpaths")
@Singleton
public class CustomizedPathsAPI extends API {

    private static final Logger LOG = Log.logger(RestServer.class);

    @POST
    @Timed
    @Status(Status.CREATED)
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON_WITH_CHARSET)
    public String post(@Context GraphManager manager,
                       @PathParam("graph") String graph,
                       PathRequest request) {
        E.checkArgumentNotNull(request, "The path request body can't be null");
        E.checkArgumentNotNull(request.sources,
                               "The sources of path request can't be null");
        E.checkArgument(request.steps != null && !request.steps.isEmpty(),
                        "The steps of path request can't be empty");
        if (request.sortBy == null) {
            request.sortBy = SortBy.NONE;
        }

        LOG.debug("Graph [{}] get customized paths from source vertex '{}', " +
                  "with steps '{}', sort by '{}', capacity '{}', limit '{}' " +
                  "and with_vertex '{}'", graph, request.sources, request.steps,
                  request.sortBy, request.capacity, request.limit,
                  request.withVertex);

        HugeGraph g = graph(manager, graph);
        List<HugeVertex> sources = request.sources.sourcesVertices(g);
        List<CustomizePathsTraverser.Step> steps = step(g, request);
        boolean sorted = request.sortBy != SortBy.NONE;

        CustomizePathsTraverser traverser = new CustomizePathsTraverser(g);
        List<HugeTraverser.Path> paths;
        paths = traverser.customizedPaths(sources, steps, sorted,
                                          request.capacity, request.limit);

        if (sorted) {
            boolean incr = request.sortBy == SortBy.INCR;
            paths = CustomizePathsTraverser.topNPath(paths, incr,
                                                     request.limit);
        }

        if (!request.withVertex) {
            return manager.serializer(g).writePaths("paths", paths, false);
        }

        Set<Id> ids = new HashSet<>();
        for (HugeTraverser.Path p : paths) {
            ids.addAll(p.vertices());
        }
        Iterator<Vertex> iter = Collections.emptyIterator();
        if (!ids.isEmpty()) {
            iter = g.vertices(ids.toArray());
        }
        return manager.serializer(g).writePaths("paths", paths, false, iter);
    }

    private static List<CustomizePathsTraverser.Step> step(HugeGraph graph,
                                                           PathRequest req) {
        int stepSize = req.steps.size();
        List<CustomizePathsTraverser.Step> steps = new ArrayList<>(stepSize);
        for (Step step : req.steps) {
            steps.add(step.jsonToStep(graph));
        }
        return steps;
    }

    private static class PathRequest {

        @JsonProperty("sources")
        public SourceVertices sources;
        @JsonProperty("steps")
        public List<Step> steps;
        @JsonProperty("sort_by")
        public SortBy sortBy;
        @JsonProperty("capacity")
        public long capacity = Long.valueOf(DEFAULT_CAPACITY);
        @JsonProperty("limit")
        public long limit = Long.valueOf(DEFAULT_PATHS_LIMIT);
        @JsonProperty("with_vertex")
        public boolean withVertex = false;

        @Override
        public String toString() {
            return String.format("PathRequest{sourceVertex=%s,steps=%s," +
                                 "sortBy=%s,capacity=%s,limit=%s," +
                                 "withVertex=%s}", this.sources, this.steps,
                                 this.sortBy, this.capacity, this.limit,
                                 this.withVertex);
        }
    }

    private static class Step {

        @JsonProperty("direction")
        public Directions direction;
        @JsonProperty("labels")
        public List<String> labels;
        @JsonProperty("properties")
        public Map<String, Object> properties;
        @JsonProperty("weight_by")
        public String weightBy;
        @JsonProperty("default_weight")
        public double defaultWeight = Double.valueOf(DEFAULT_WEIGHT);
        @JsonProperty("degree")
        public long degree = Long.valueOf(DEFAULT_DEGREE);
        @JsonProperty("sample")
        public long sample = Long.valueOf(DEFAULT_SAMPLE);

        @Override
        public String toString() {
            return String.format("Step{direction=%s,labels=%s,properties=%s," +
                                 "weightBy=%s,defaultWeight=%s,degree=%s," +
                                 "sample=%s}", this.direction, this.labels,
                                 this.properties, this.weightBy,
                                 this.defaultWeight, this.degree, this.sample);
        }

        private CustomizePathsTraverser.Step jsonToStep(HugeGraph graph) {
            E.checkArgument(this.degree > 0 || this.degree == NO_LIMIT,
                            "The degree must be > 0, but got: %s",
                            this.degree);
            E.checkArgument(this.sample > 0 || this.sample == NO_LIMIT,
                            "The sample must be > 0, but got: %s",
                            this.sample);
            E.checkArgument(this.degree == NO_LIMIT || this.degree >= sample,
                            "Degree must be greater than or equal to sample," +
                            " but got degree %s and sample %s", degree, sample);
            Map<Id, String> labelIds = new HashMap<>();
            if (this.labels != null) {
                for (String label : this.labels) {
                    EdgeLabel el = graph.edgeLabel(label);
                    labelIds.put(el.id(), label);
                }
            }
            PropertyKey weightBy = null;
            if (this.weightBy != null) {
                weightBy = graph.propertyKey(this.weightBy);
            }
            return new CustomizePathsTraverser.Step(this.direction, labelIds,
                                                    this.properties, weightBy,
                                                    this.defaultWeight,
                                                    this.degree, this.sample);
        }
    }

    private enum SortBy {
        INCR,
        DECR,
        NONE
    }
}
