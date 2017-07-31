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
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.baidu.hugegraph.backend.serializer;

import java.nio.ByteBuffer;

import org.apache.tinkerpop.gremlin.structure.Edge;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.query.Query;
import com.baidu.hugegraph.backend.store.BackendEntry;
import com.baidu.hugegraph.backend.store.BackendEntry.BackendColumn;
import com.baidu.hugegraph.schema.EdgeLabel;
import com.baidu.hugegraph.schema.IndexLabel;
import com.baidu.hugegraph.schema.PropertyKey;
import com.baidu.hugegraph.schema.VertexLabel;
import com.baidu.hugegraph.structure.HugeEdge;
import com.baidu.hugegraph.structure.HugeIndex;
import com.baidu.hugegraph.structure.HugeProperty;
import com.baidu.hugegraph.structure.HugeVertex;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.define.HugeKeys;
import com.baidu.hugegraph.util.StringEncoding;

public class BinarySerializer extends AbstractSerializer {

    public BinarySerializer(final HugeGraph graph) {
        super(graph);
    }

    @Override
    public BackendEntry newBackendEntry(Id id) {
        return new BinaryBackendEntry(id);
    }

    @Override
    public BackendEntry writeId(HugeType type, Id id) {
        return null;
    }

    @Override
    public Query writeQuery(Query query) {
        return null;
    }

    protected byte[] formatSystemPropertyName(HugeKeys col) {
        return new byte[] {HugeType.SYS_PROPERTY.code(), col.code()};
    }

    private BackendColumn formatLabel(VertexLabel vertexLabel) {
        BackendColumn col = new BackendColumn();
        col.name = this.formatSystemPropertyName(HugeKeys.LABEL);
        // TODO: save label name or id?
        col.value = StringEncoding.encodeString(vertexLabel.name());
        return col;
    }

    private VertexLabel parseLabel(BackendColumn col) {
        String label = StringEncoding.decodeString(col.value);
        return this.graph.schema().getVertexLabel(label);
    }

    private byte[] formatPropertyName(HugeProperty<?> prop) {
        // With encoded bytes
        byte[] name = StringEncoding.encodeString(prop.key());
        ByteBuffer buffer = ByteBuffer.allocate(3 + name.length);
        buffer.put(prop.type().code());
        // WriteString(name, buffer);
        buffer.put(name);
        return buffer.array();
    }

    private byte[] formatPropertyValue(HugeProperty<?> prop) {
        // With encoded bytes
        Object value = prop.value();
        // TODO: serialize any object, not only string
        return StringEncoding.encodeString(value.toString());
    }

    private BackendColumn formatProperty(HugeProperty<?> prop) {
        BackendColumn col = new BackendColumn();
        col.name = this.formatPropertyName(prop);
        col.value = this.formatPropertyValue(prop);
        return col;
    }

    private Object parsePropertyValue(byte[] bytes) {
        // TODO: deserialize any object, not only string
        return StringEncoding.decodeString(bytes);
    }

    private void parseColumn(BackendColumn col, HugeVertex vertex) {
        ByteBuffer buffer = ByteBuffer.wrap(col.name);
        byte type = buffer.get();
        // Property
        if (type == HugeType.PROPERTY.code()) {
            String name = readStringFromRemaining(buffer);
            Object value = parsePropertyValue(col.value);
            vertex.addProperty(name, value);
        } else if (type == HugeType.EDGE_IN.code() ||
                   type == HugeType.EDGE_OUT.code()) {
            // TODO: parse edge
            ;
        }
    }

    @Override
    public BackendEntry writeVertex(HugeVertex vertex) {
        BinaryBackendEntry entry = new BinaryBackendEntry(vertex.id());

        // Write label column
        entry.column(this.formatLabel(vertex.vertexLabel()));

        // Add all properties of a Vertex
        for (HugeProperty<?> prop : vertex.getProperties().values()) {
            entry.column(this.formatProperty(prop));
        }

        // Add all edges of a Vertex
        for (@SuppressWarnings("unused") Edge edge : vertex.getEdges()) {
            // TODO: format edge
        }

        return entry;
    }

    @Override
    public HugeVertex readVertex(BackendEntry bytesEntry) {
        assert bytesEntry instanceof BinaryBackendEntry;
        BinaryBackendEntry entry = (BinaryBackendEntry) bytesEntry;

        // Parse label
        byte[] labelCol = this.formatSystemPropertyName(HugeKeys.LABEL);
        VertexLabel label = this.parseLabel(entry.column(labelCol));

        HugeVertex vertex = new HugeVertex(this.graph, entry.id(), label);

        // Parse all properties and edges of a Vertex
        for (BackendColumn col : entry.columns()) {
            this.parseColumn(col, vertex);
        }

        return vertex;
    }

    @Override
    public BackendEntry writeVertexLabel(VertexLabel vertexLabel) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public BackendEntry writeEdgeLabel(EdgeLabel edgeLabel) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public BackendEntry writePropertyKey(PropertyKey propertyKey) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public VertexLabel readVertexLabel(BackendEntry entry) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public EdgeLabel readEdgeLabel(BackendEntry entry) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public PropertyKey readPropertyKey(BackendEntry entry) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public BackendEntry writeIndexLabel(IndexLabel indexLabel) {
        return null;
    }

    @Override
    public IndexLabel readIndexLabel(BackendEntry entry) {
        return null;
    }

    protected static void writeString(byte[] bytes, ByteBuffer buffer) {
        assert bytes.length < Short.MAX_VALUE;
        buffer.putShort((short) bytes.length);
        buffer.put(bytes);
    }

    protected static void writeString(String value, ByteBuffer buffer) {
        byte[] bytes = StringEncoding.encodeString(value);
        writeString(bytes, buffer);
    }

    protected static String readString(ByteBuffer buffer) {
        short length = buffer.getShort();
        byte[] bytes = new byte[length];
        buffer.get(bytes);
        return StringEncoding.decodeString(bytes);
    }

    protected static String readStringFromRemaining(ByteBuffer buffer) {
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return StringEncoding.decodeString(bytes);
    }

    @Override
    public BackendEntry writeIndex(HugeIndex index) {
        return null;
    }

    @Override
    public HugeIndex readIndex(BackendEntry entry) {
        return null;
    }

    @Override
    public BackendEntry writeEdge(HugeEdge edge) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public HugeEdge readEdge(BackendEntry entry) {
        // TODO Auto-generated method stub
        return null;
    }
}
