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

package com.baidu.hugegraph.serializer;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import com.baidu.hugegraph.schema.EdgeLabel;
import com.baidu.hugegraph.schema.IndexLabel;
import com.baidu.hugegraph.schema.PropertyKey;
import com.baidu.hugegraph.schema.VertexLabel;
import com.baidu.hugegraph.traversal.algorithm.CustomizedCrosspointsTraverser.CrosspointsPaths;
import com.baidu.hugegraph.traversal.algorithm.FusiformSimilarityTraverser.SimilarsMap;
import com.baidu.hugegraph.traversal.algorithm.HugeTraverser;
import com.baidu.hugegraph.traversal.algorithm.SingleSourceShortestPathTraverser.NodeWithWeight;
import com.baidu.hugegraph.traversal.algorithm.SingleSourceShortestPathTraverser.ShortestPaths;

public interface Serializer {

    public String writeMap(Map<?, ?> map);

    public String writeList(String label, Collection<?> list);

    public String writePropertyKey(PropertyKey propertyKey);

    public String writePropertyKeys(List<PropertyKey> propertyKeys);

    public String writeVertexLabel(VertexLabel vertexLabel);

    public String writeVertexLabels(List<VertexLabel> vertexLabels);

    public String writeEdgeLabel(EdgeLabel edgeLabel);

    public String writeEdgeLabels(List<EdgeLabel> edgeLabels);

    public String writeIndexlabel(IndexLabel indexLabel);

    public String writeIndexlabels(List<IndexLabel> indexLabels);

    public String writeCreatedIndexLabel(IndexLabel.CreatedIndexLabel cil);

    public String writeVertex(Vertex v);

    public String writeVertices(Iterator<Vertex> vertices, boolean paging);

    public String writeEdge(Edge e);

    public String writeEdges(Iterator<Edge> edges, boolean paging);

    public String writePaths(String name, Collection<HugeTraverser.Path> paths,
                             boolean withCrossPoint, Iterator<Vertex> vertices);

    public default String writePaths(String name,
                                     Collection<HugeTraverser.Path> paths,
                                     boolean withCrossPoint) {
        return this.writePaths(name, paths, withCrossPoint, null);
    }

    public String writeCrosspoints(CrosspointsPaths paths,
                                   Iterator<Vertex> iterator, boolean withPath);

    public String writeSimilars(SimilarsMap similars,
                                Iterator<Vertex> vertices);

    public String writeShortestPath(NodeWithWeight path,
                                    Iterator<Vertex> vertices);

    public String writeShortestPaths(ShortestPaths paths,
                                     Iterator<Vertex> vertices);
}
