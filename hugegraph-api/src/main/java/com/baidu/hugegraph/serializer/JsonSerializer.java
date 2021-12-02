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

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;

import com.baidu.hugegraph.HugeException;
import com.baidu.hugegraph.api.API;
import com.baidu.hugegraph.auth.SchemaDefine.AuthElement;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.page.PageInfo;
import com.baidu.hugegraph.iterator.Metadatable;
import com.baidu.hugegraph.schema.EdgeLabel;
import com.baidu.hugegraph.schema.IndexLabel;
import com.baidu.hugegraph.schema.PropertyKey;
import com.baidu.hugegraph.schema.SchemaElement;
import com.baidu.hugegraph.schema.VertexLabel;
import com.baidu.hugegraph.space.GraphSpace;
import com.baidu.hugegraph.space.Service;
import com.baidu.hugegraph.traversal.algorithm.CustomizedCrosspointsTraverser.CrosspointsPaths;
import com.baidu.hugegraph.traversal.algorithm.FusiformSimilarityTraverser.SimilarsMap;
import com.baidu.hugegraph.traversal.algorithm.HugeTraverser;
import com.baidu.hugegraph.traversal.algorithm.SingleSourceShortestPathTraverser.NodeWithWeight;
import com.baidu.hugegraph.traversal.algorithm.SingleSourceShortestPathTraverser.WeightedPaths;
import com.baidu.hugegraph.traversal.optimize.TraversalUtil;
import com.baidu.hugegraph.util.JsonUtil;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class JsonSerializer implements Serializer {

    private static final int LBUF_SIZE = 1024;

    private static final JsonSerializer INSTANCE = new JsonSerializer();

    private Map<String, Object> debugMeasure = null;
    private static final String MEASURE_KEY = "measure";

    private JsonSerializer() {
    }

    public static JsonSerializer instance() {
        return INSTANCE;
    }

    private JsonSerializer(Map<String, Object> debugMeasure) {
        this.debugMeasure = debugMeasure;
    }

    public static JsonSerializer instance(Map<String, Object> debugMatrix) {
        return new JsonSerializer(debugMatrix);
    }

    @Override
    public String writeMap(Map<?, ?> map) {
        return JsonUtil.toJson(map);
    }

    @Override
    public String writeList(String label, Collection<?> list) {
        Map<String, Object> results;
        if (this.debugMeasure != null) {
            results = ImmutableMap.of(label, list,
                                      MEASURE_KEY, this.debugMeasure);
        } else {
            results = ImmutableMap.of(label, list);
        }
        return JsonUtil.toJson(results);
    }

    private String writeIterator(String label, Iterator<?> iter,
                                 boolean paging) {
        // Early throw if needed
        iter.hasNext();

        // Serialize Iterator
        try (ByteArrayOutputStream out = new ByteArrayOutputStream(LBUF_SIZE)) {
            out.write("{".getBytes(API.CHARSET));

            out.write(String.format("\"%s\":[", label).getBytes(API.CHARSET));

            // Write data
            boolean first = true;
            while (iter.hasNext()) {
                if (!first) {
                    out.write(",".getBytes(API.CHARSET));
                } else {
                    first = false;
                }
                out.write(JsonUtil.toJson(iter.next()).getBytes(API.CHARSET));
            }
            out.write("]".getBytes(API.CHARSET));

            // Write page
            if (paging) {
                String page;
                if (iter instanceof GraphTraversal<?, ?>) {
                    page = TraversalUtil.page((GraphTraversal<?, ?>) iter);
                } else if (iter instanceof Metadatable) {
                    page = PageInfo.pageInfo(iter);
                } else {
                    throw new HugeException("Invalid paging iterator: %s",
                                            iter.getClass());
                }
                if (page != null) {
                    page = String.format(",\"page\": \"%s\"", page);
                } else {
                    page = ",\"page\": null";
                }
                out.write(page.getBytes(API.CHARSET));
            }

            out.write("}".getBytes(API.CHARSET));
            return out.toString(API.CHARSET);
        } catch (HugeException e) {
            throw e;
        } catch (Exception e) {
            throw new HugeException("Failed to serialize %s", e, label);
        } finally {
            try {
                CloseableIterator.closeIterator(iter);
            } catch (Exception e) {
                throw new HugeException("Failed to close for %s", e, label);
            }
        }
    }

    @Override
    public String writePropertyKey(PropertyKey propertyKey) {
        return JsonUtil.toJson(propertyKey);
    }

    @Override
    public String writeTaskWithSchema(
                  SchemaElement.TaskWithSchema taskWithSchema) {
        StringBuilder builder = new StringBuilder();
        long id = taskWithSchema.task() == null ?
                  0L : taskWithSchema.task().asLong();
        SchemaElement schemaElement = taskWithSchema.schemaElement();
        String type;
        String schema;
        if (schemaElement instanceof PropertyKey) {
            type = "property_key";
            schema = this.writePropertyKey((PropertyKey) schemaElement);
        } else if (schemaElement instanceof IndexLabel) {
            type = "index_label";
            schema = this.writeIndexlabel((IndexLabel) schemaElement);
        } else {
            throw new HugeException("Invalid schema element '%s' in " +
                                    "TaskWithSchema, only support " +
                                    "[PropertyKey, IndexLabel]",
                                    schemaElement);
        }
        return builder.append("{\"").append(type).append("\": ")
                      .append(schema).append(", \"task_id\": ")
                      .append(id).append("}").toString();
    }

    @Override
    public String writePropertyKeys(List<PropertyKey> propertyKeys) {
        return writeList("propertykeys", propertyKeys);
    }

    @Override
    public String writeVertexLabel(VertexLabel vertexLabel) {
        return JsonUtil.toJson(vertexLabel);
    }

    @Override
    public String writeVertexLabels(List<VertexLabel> vertexLabels) {
        return writeList("vertexlabels", vertexLabels);
    }

    @Override
    public String writeEdgeLabel(EdgeLabel edgeLabel) {
        return JsonUtil.toJson(edgeLabel);
    }

    @Override
    public String writeEdgeLabels(List<EdgeLabel> edgeLabels) {
        return writeList("edgelabels", edgeLabels);
    }

    @Override
    public String writeIndexlabel(IndexLabel indexLabel) {
        return JsonUtil.toJson(indexLabel);
    }

    @Override
    public String writeIndexlabels(List<IndexLabel> indexLabels) {
        return writeList("indexlabels", indexLabels);
    }

    @Override
    public String writeVertex(Vertex vertex) {
        return JsonUtil.toJson(vertex);
    }

    @Override
    public String writeVertices(Iterator<Vertex> vertices, boolean paging) {
        return this.writeIterator("vertices", vertices, paging);
    }

    @Override
    public String writeEdge(Edge edge) {
        return JsonUtil.toJson(edge);
    }

    @Override
    public String writeEdges(Iterator<Edge> edges, boolean paging) {
        return this.writeIterator("edges", edges, paging);
    }

    @Override
    public String writeIds(List<Id> ids) {
        return JsonUtil.toJson(ids);
    }

    @Override
    public String writeAuthElement(AuthElement elem) {
        return this.writeMap(elem.asMap());
    }

    @Override
    public <V extends AuthElement> String writeAuthElements(String label,
                                                            List<V> elems) {
        List<Object> list = new ArrayList<>(elems.size());
        for (V elem : elems) {
            list.add(elem.asMap());
        }
        return this.writeList(label, list);
    }

    @Override
    public String writePaths(String name, Collection<HugeTraverser.Path> paths,
                             boolean withCrossPoint,
                             Iterator<Vertex> vertices) {
        List<Map<String, Object>> pathList = new ArrayList<>(paths.size());
        for (HugeTraverser.Path path : paths) {
            pathList.add(path.toMap(withCrossPoint));
        }

        Map<String, Object> results;
        if (vertices == null) {
            results = ImmutableMap.of(name, pathList);
        } else {
            results = ImmutableMap.of(name, pathList, "vertices", vertices);
        }
        return JsonUtil.toJson(results);
    }

    @Override
    public String writeCrosspoints(CrosspointsPaths paths,
                                   Iterator<Vertex> iterator,
                                   boolean withPath) {
        Map<String, Object> results;
        List<Map<String, Object>> pathList;
        if (withPath) {
            pathList = new ArrayList<>();
            for (HugeTraverser.Path path : paths.paths()) {
                pathList.add(path.toMap(false));
            }
        } else {
            pathList = ImmutableList.of();
        }
        results = ImmutableMap.of("crosspoints", paths.crosspoints(),
                                  "paths", pathList,
                                  "vertices", iterator);
        return JsonUtil.toJson(results);
    }

    @Override
    public String writeSimilars(SimilarsMap similars,
                                Iterator<Vertex> vertices) {
        return JsonUtil.toJson(ImmutableMap.of("similars", similars.toMap(),
                                               "vertices", vertices));
    }

    @Override
    public String writeWeightedPath(NodeWithWeight path,
                                    Iterator<Vertex> vertices) {
        Map<String, Object> pathMap = path == null ?
                                      ImmutableMap.of() : path.toMap();
        return JsonUtil.toJson(ImmutableMap.of("path", pathMap,
                                               "vertices", vertices));
    }

    @Override
    public String writeWeightedPaths(WeightedPaths paths,
                                     Iterator<Vertex> vertices) {
        Map<Id, Map<String, Object>> pathMap = paths == null ?
                                               ImmutableMap.of() :
                                               paths.toMap();
        return JsonUtil.toJson(ImmutableMap.of("paths", pathMap,
                                               "vertices", vertices));
    }

    @Override
    public String writeNodesWithPath(String name, List<Id> nodes, long size,
                                     Collection<HugeTraverser.Path> paths,
                                     Iterator<Vertex> vertices,
                                     Iterator<Edge> edges, List<Integer> sizes) {
        List<Map<String, Object>> pathList = new ArrayList<>();
        for (HugeTraverser.Path path : paths) {
            pathList.add(path.toMap(false));
        }

        ImmutableMap.Builder<Object, Object> build = ImmutableMap.builder()
                                                     .put(name, nodes)
                                                     .put("size", size)
                                                     .put("paths", pathList)
                                                     .put("vertices", vertices)
                                                     .put("edges", edges);

        if (this.debugMeasure != null) {
            build.put(MEASURE_KEY, this.debugMeasure);
        }
        if (sizes != null ) {
            build.put("sizes", sizes);
        }
        return JsonUtil.toJson(build.build());
    }

    @Override
    public String writeGraphSpace(GraphSpace graphSpace) {
        return JsonUtil.toJson(graphSpace);
    }

    @Override
    public String writeService(Service service) {
        return JsonUtil.toJson(service);
    }
}
