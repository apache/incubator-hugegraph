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

package com.baidu.hugegraph.traversal.algorithm;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.ws.rs.core.MultivaluedMap;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.structure.HugeEdge;
import com.baidu.hugegraph.structure.HugeVertex;
import com.baidu.hugegraph.type.define.Directions;
import com.baidu.hugegraph.util.CollectionUtil;
import com.baidu.hugegraph.util.E;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

public class CustomizedCrosspointsTraverser extends HugeTraverser {

    public CustomizedCrosspointsTraverser(HugeGraph graph) {
        super(graph);
    }

    public CrosspointsPaths crosspointsPaths(Iterator<Vertex> vertices,
                                             List<PathPattern> pathPatterns,
                                             long capacity, long limit) {
        E.checkArgument(vertices.hasNext(),
                        "The source vertices can't be empty");
        E.checkArgument(!pathPatterns.isEmpty(),
                        "The steps pattern can't be empty");
        checkCapacity(capacity);
        checkLimit(limit);
        MultivaluedMap<Id, Node> initialSources = newMultivalueMap();
        List<HugeVertex> verticesList = new ArrayList<>();
        while (vertices.hasNext()) {
            HugeVertex vertex = (HugeVertex) vertices.next();
            verticesList.add(vertex);
            Node node = new Node(vertex.id(), null);
            initialSources.add(vertex.id(), node);
        }
        List<Path> paths = new ArrayList<>();

        for (PathPattern pathPattern : pathPatterns) {
            MultivaluedMap<Id, Node> sources = initialSources;
            int stepNum = pathPattern.size();
            long access = 0;
            MultivaluedMap<Id, Node> newVertices = null;
            for (Step step : pathPattern.steps()) {
                stepNum--;
                newVertices = newMultivalueMap();
                Iterator<Edge> edges;

                // Traversal vertices of previous level
                for (Map.Entry<Id, List<Node>> entry : sources.entrySet()) {
                    List<Node> adjacency = new ArrayList<>();
                    edges = edgesOfVertex(entry.getKey(), step.direction,
                                          step.labels, step.properties,
                                          step.degree);
                    while (edges.hasNext()) {
                        HugeEdge edge = (HugeEdge) edges.next();
                        Id target = edge.id().otherVertexId();
                        for (Node n : entry.getValue()) {
                            // If have loop, skip target
                            if (n.contains(target)) {
                                continue;
                            }
                            Node newNode = new Node(target, n);
                            adjacency.add(newNode);

                            checkCapacity(capacity, ++access,
                                          "customized crosspoints");
                        }
                    }

                    // Add current node's adjacent nodes
                    for (Node node : adjacency) {
                        newVertices.add(node.id(), node);
                    }
                }
                // Re-init sources
                sources = newVertices;
            }
            assert stepNum == 0;
            for (List<Node> nodes : newVertices.values()) {
                for (Node n : nodes) {
                    paths.add(new Path(n.path()));
                }
            }
        }
        return intersectionPaths(verticesList, paths, limit);
    }

    private static CrosspointsPaths intersectionPaths(List<HugeVertex> sources,
                                                      List<Path> paths,
                                                      long limit) {
        // Split paths by end vertices
        MultivaluedMap<Id, Id> endVertices = newMultivalueMap();
        for (Path path : paths) {
            List<Id> vertices = path.vertices();
            int length = vertices.size();
            endVertices.add(vertices.get(0), vertices.get(length - 1));
        }

        Set<Id> sourceIds = sources.stream().map(HugeVertex::id)
                                   .collect(Collectors.toSet());
        Set<Id> ids = endVertices.keySet();
        if (sourceIds.size() != ids.size() || !sourceIds.containsAll(ids)) {
            return CrosspointsPaths.EMPTY;
        }

        // Get intersection of end vertices
        Collection<Id> intersection = null;
        for (List<Id> ends : endVertices.values()) {
            if (intersection == null) {
                intersection = ends;
            } else {
                intersection = CollectionUtil.intersect(intersection, ends);
            }
            if (intersection == null || intersection.isEmpty()) {
                return CrosspointsPaths.EMPTY;
            }
        }
        assert intersection != null;
        // Limit intersection number to limit crosspoints vertices in result
        int size = intersection.size();
        if (limit != NO_LIMIT && size > limit) {
            intersection = new ArrayList<>(intersection).subList(0, size - 1);
        }

        // Filter intersection paths
        List<Path> results = new ArrayList<>();
        for (Path path : paths) {
            List<Id> vertices = path.vertices();
            int length = vertices.size();
            if (intersection.contains(vertices.get(length - 1))) {
                results.add(path);
            }
        }
        return new CrosspointsPaths(new HashSet<>(intersection), results);
    }

    public static class PathPattern {

        private List<Step> steps;

        public PathPattern() {
            this.steps = new ArrayList<>();
        }

        public List<Step> steps() {
            return this.steps;
        }

        public int size() {
            return this.steps.size();
        }

        public void add(Step step) {
            this.steps.add(step);
        }
    }

    public static class Step {

        private Directions direction;
        private Map<Id, String> labels;
        private Map<String, Object> properties;
        private long degree;

        public Step(Directions direction, Map<Id, String> labels,
                    Map<String, Object> properties, long degree) {
            this.direction = direction;
            this.labels = labels;
            this.properties = properties;
            this.degree = degree;
        }
    }

    public static class CrosspointsPaths {

        private static final CrosspointsPaths EMPTY = new CrosspointsPaths(
                ImmutableSet.of(), ImmutableList.of()
        );

        private Set<Id> crosspoints;
        private List<Path> paths;

        public CrosspointsPaths(Set<Id> crosspoints, List<Path> paths) {
            this.crosspoints = crosspoints;
            this.paths = paths;
        }

        public Set<Id> crosspoints() {
            return this.crosspoints;
        }

        public List<Path> paths() {
            return this.paths;
        }
    }
}
