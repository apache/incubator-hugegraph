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

import com.baidu.hugegraph.auth.SchemaDefine.AuthElement;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.schema.EdgeLabel;
import com.baidu.hugegraph.schema.IndexLabel;
import com.baidu.hugegraph.schema.PropertyKey;
import com.baidu.hugegraph.schema.SchemaElement;
import com.baidu.hugegraph.schema.VertexLabel;
import com.baidu.hugegraph.traversal.algorithm.CustomizedCrosspointsTraverser.CrosspointsPaths;
import com.baidu.hugegraph.traversal.algorithm.FusiformSimilarityTraverser.SimilarsMap;
import com.baidu.hugegraph.traversal.algorithm.HugeTraverser;
import com.baidu.hugegraph.traversal.algorithm.SingleSourceShortestPathTraverser.NodeWithWeight;
import com.baidu.hugegraph.traversal.algorithm.SingleSourceShortestPathTraverser.WeightedPaths;

public interface Serializer {

    String writeMap(Map<?, ?> map);

    String writeList(String label, Collection<?> list);

    String writePropertyKey(PropertyKey propertyKey);

    String writePropertyKeys(List<PropertyKey> propertyKeys);

    String writeVertexLabel(VertexLabel vertexLabel);

    String writeVertexLabels(List<VertexLabel> vertexLabels);

    String writeEdgeLabel(EdgeLabel edgeLabel);

    String writeEdgeLabels(List<EdgeLabel> edgeLabels);

    String writeIndexlabel(IndexLabel indexLabel);

    String writeIndexlabels(List<IndexLabel> indexLabels);

    String writeTaskWithSchema(SchemaElement.TaskWithSchema tws);

    String writeVertex(Vertex v);

    String writeVertices(Iterator<Vertex> vertices, boolean paging);

    String writeEdge(Edge e);

    String writeEdges(Iterator<Edge> edges, boolean paging);

    String writeIds(List<Id> ids);

    String writeAuthElement(AuthElement elem);

    <V extends AuthElement> String writeAuthElements(String label, List<V> users);

    String writePaths(String name, Collection<HugeTraverser.Path> paths,
                      boolean withCrossPoint, Iterator<Vertex> vertices);

    default String writePaths(String name, Collection<HugeTraverser.Path> paths,
                              boolean withCrossPoint) {
        return this.writePaths(name, paths, withCrossPoint, null);
    }

    String writeCrosspoints(CrosspointsPaths paths, Iterator<Vertex> iterator,
                            boolean withPath);

    String writeSimilars(SimilarsMap similars, Iterator<Vertex> vertices);

    String writeWeightedPath(NodeWithWeight path, Iterator<Vertex> vertices);

    String writeWeightedPaths(WeightedPaths paths, Iterator<Vertex> vertices);

    String writeNodesWithPath(String name, List<Id> nodes, long size,
                              Collection<HugeTraverser.Path> paths,
                              Iterator<Vertex> vertices);
}
