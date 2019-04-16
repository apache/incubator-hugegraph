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
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.page.PageState;
import com.baidu.hugegraph.backend.store.Shard;
import com.baidu.hugegraph.iterator.Metadatable;
import com.baidu.hugegraph.schema.EdgeLabel;
import com.baidu.hugegraph.schema.IndexLabel;
import com.baidu.hugegraph.schema.PropertyKey;
import com.baidu.hugegraph.schema.VertexLabel;
import com.baidu.hugegraph.traversal.algorithm.CustomizedCrosspointsTraverser.CrosspointsPaths;
import com.baidu.hugegraph.traversal.algorithm.HugeTraverser;
import com.baidu.hugegraph.traversal.optimize.TraversalUtil;
import com.baidu.hugegraph.util.JsonUtil;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class JsonSerializer implements Serializer {

    private static final int LBUF_SIZE = 1024;

    private static JsonSerializer INSTANCE = new JsonSerializer();

    private JsonSerializer() {
    }

    public static JsonSerializer instance() {
        return INSTANCE;
    }

    @Override
    public String writeMap(Map<?, ?> map) {
        return JsonUtil.toJson(map);
    }

    @Override
    public String writeList(String label, Collection<?> list) {
        try (ByteArrayOutputStream out = new ByteArrayOutputStream(LBUF_SIZE)) {
            out.write(String.format("{\"%s\": ", label).getBytes(API.CHARSET));
            out.write(JsonUtil.toJson(list).getBytes(API.CHARSET));
            out.write("}".getBytes(API.CHARSET));
            return out.toString(API.CHARSET);
        } catch (Exception e) {
            throw new HugeException("Failed to serialize %s", e, label);
        }
    }

    private String writeIterator(String label, Iterator<?> iter,
                                 boolean paging) {
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
                    page = PageState.page(iter);
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
    public String writeCreatedIndexLabel(IndexLabel.CreatedIndexLabel cil) {
        StringBuilder builder = new StringBuilder();
        long id = cil.task() == null ? 0L : cil.task().asLong();
        return builder.append("{\"index_label\": ")
                      .append(this.writeIndexlabel(cil.indexLabel()))
                      .append(", \"task_id\": ")
                      .append(id)
                      .append("}")
                      .toString();
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
}
