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
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.ws.rs.core.MultivaluedMap;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONWriter;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;

import com.baidu.hugegraph.HugeException;
import com.baidu.hugegraph.api.API;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.store.Shard;
import com.baidu.hugegraph.schema.EdgeLabel;
import com.baidu.hugegraph.schema.IndexLabel;
import com.baidu.hugegraph.schema.PropertyKey;
import com.baidu.hugegraph.schema.VertexLabel;
import com.baidu.hugegraph.traversal.optimize.HugeTraverser;
import com.baidu.hugegraph.traversal.optimize.TraversalUtil;

public class JsonSerializer implements Serializer {

    private GraphSONWriter writer;

    private static final int BUF_SIZE = 128;
    private static final int LBUF_SIZE = 1024;

    public JsonSerializer(GraphSONWriter writer) {
        this.writer = writer;
    }

    private String writeObject(Object object) {
        try (ByteArrayOutputStream out = new ByteArrayOutputStream(BUF_SIZE)) {
            this.writer.writeObject(out, object);
            return out.toString(API.CHARSET);
        } catch (Exception e) {
            throw new HugeException("Failed to serialize %s", e,
                                    object.getClass().getSimpleName());
        }
    }

    private String writeList(String label, List<?> list) {
        try (ByteArrayOutputStream out = new ByteArrayOutputStream(LBUF_SIZE)) {
            out.write(String.format("{\"%s\": ", label).getBytes(API.CHARSET));
            this.writer.writeObject(out, list);
            out.write("}".getBytes(API.CHARSET));
            return out.toString(API.CHARSET);
        } catch (Exception e) {
            throw new HugeException("Failed to serialize %s", e, label);
        }
    }

    private String writeList(String label, Iterator<?> itor, boolean paging) {
        try (ByteArrayOutputStream out = new ByteArrayOutputStream(LBUF_SIZE)) {
            out.write("{".getBytes(API.CHARSET));

            out.write(String.format("\"%s\":[", label).getBytes(API.CHARSET));

            // Write data
            boolean first = true;
            while (itor.hasNext()) {
                if (!first) {
                    out.write(",".getBytes(API.CHARSET));
                } else {
                    first = false;
                }
                this.writer.writeObject(out, itor.next());
            }
            out.write("]".getBytes(API.CHARSET));

            // Write page
            if (paging) {
                String page = TraversalUtil.page((GraphTraversal<?, ?>) itor);
                page = String.format(",\"page\": \"%s\"", page);
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
                CloseableIterator.closeIterator(itor);
            } catch (Exception e) {
                throw new HugeException("Failed to close for %s", e, label);
            }
        }
    }

    @Override
    public String writePropertyKey(PropertyKey propertyKey) {
        return writeObject(propertyKey);
    }

    @Override
    public String writePropertyKeys(List<PropertyKey> propertyKeys) {
        return writeList("propertykeys", propertyKeys);
    }

    @Override
    public String writeVertexLabel(VertexLabel vertexLabel) {
        return writeObject(vertexLabel);
    }

    @Override
    public String writeVertexLabels(List<VertexLabel> vertexLabels) {
        return writeList("vertexlabels", vertexLabels);
    }

    @Override
    public String writeEdgeLabel(EdgeLabel edgeLabel) {
        return writeObject(edgeLabel);
    }

    @Override
    public String writeEdgeLabels(List<EdgeLabel> edgeLabels) {
        return writeList("edgelabels", edgeLabels);
    }

    @Override
    public String writeIndexlabel(IndexLabel indexLabel) {
        return writeObject(indexLabel);
    }

    @Override
    public String writeIndexlabels(List<IndexLabel> indexLabels) {
        return writeList("indexlabels", indexLabels);
    }

    @Override
    public String writeVertex(Vertex vertex) {
        return writeObject(vertex);
    }

    @Override
    public String writeVertices(Iterator<Vertex> vertices, boolean paging) {
        return writeList("vertices", vertices, paging);
    }

    @Override
    public String writeEdge(Edge edge) {
        return writeObject(edge);
    }

    @Override
    public String writeEdges(Iterator<Edge> edges, boolean paging) {
        return writeList("edges", edges, paging);
    }

    @Override
    public String writeIds(String name, Collection<Id> ids) {
        if (ids instanceof List) {
            return writeList(name, (List<?>) ids);
        } else {
            return writeList(name, new ArrayList<>(ids));
        }
    }

    @Override
    public String writePaths(String name, Collection<HugeTraverser.Path> paths,
                             boolean withCrossPoint) {
        List<Map<String, Object>> pathList = new ArrayList<>(paths.size());
        for (HugeTraverser.Path path : paths) {
            pathList.add(path.toMap(withCrossPoint));
        }
        return writeList(name, pathList);
    }

    @Override
    public String writeSubGraphPaths(
                  MultivaluedMap<Boolean, HugeTraverser.Path> paths,
                  boolean withCrossPoint) {
        try (ByteArrayOutputStream out = new ByteArrayOutputStream(LBUF_SIZE)) {
            out.write("{\"loop path\": ".getBytes(API.CHARSET));
            this.writePathsList(out, paths.get(true));
            out.write(",\"leaf path\": ".getBytes(API.CHARSET));
            this.writePathsList(out, paths.get(false));
            out.write("}".getBytes(API.CHARSET));
            return out.toString(API.CHARSET);
        } catch (Exception e) {
            throw new HugeException("Failed to serialize sub-graph-paths", e);
        }
    }

    private void writePathsList(ByteArrayOutputStream out,
                                List<HugeTraverser.Path> paths)
                                throws IOException {
        if (paths == null) {
            out.write("[]".getBytes(API.CHARSET));
        } else {
            this.writer.writeObject(out, paths.stream().map(p -> p.toMap(false))
                       .collect(Collectors.toList()));
        }
    }

    @Override
    public String writeShards(List<Shard> shards) {
        return this.writeList("shards", shards);
    }
}
