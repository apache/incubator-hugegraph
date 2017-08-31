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
import java.util.List;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONWriter;

import com.baidu.hugegraph.HugeException;
import com.baidu.hugegraph.api.API;
import com.baidu.hugegraph.schema.EdgeLabel;
import com.baidu.hugegraph.schema.IndexLabel;
import com.baidu.hugegraph.schema.PropertyKey;
import com.baidu.hugegraph.schema.VertexLabel;

public class JsonSerializer implements Serializer {

    private GraphSONWriter writer;

    public JsonSerializer(GraphSONWriter writer) {
        this.writer = writer;
    }

    private String writeObject(Object object) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
            this.writer.writeObject(out, object);
            return out.toString(API.CHARSET);
        } catch (Exception e) {
            throw new HugeException("Failed to serialize %s", e,
                                    object.getClass().getSimpleName());
        }
    }

    private String writeList(String label, Object object) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
            out.write(String.format("{\"%s\": ", label).getBytes(API.CHARSET));
            this.writer.writeObject(out, object);
            out.write("}".getBytes(API.CHARSET));
            return out.toString(API.CHARSET);
        } catch (Exception e) {
            throw new HugeException("Failed to serialize %s", e, label);
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
    public String writeVertices(List<Vertex> vertices) {
        return writeList("vertices", vertices);
    }

    @Override
    public String writeEdge(Edge edge) {
        return writeObject(edge);
    }

    @Override
    public String writeEdges(List<Edge> edges) {
        return writeList("edges", edges);
    }
}
