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

package com.baidu.hugegraph.backend.serializer;

import org.apache.commons.lang.NotImplementedException;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.id.EdgeId;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.store.BackendEntry;
import com.baidu.hugegraph.backend.store.BackendEntry.BackendColumn;
import com.baidu.hugegraph.schema.VertexLabel;
import com.baidu.hugegraph.structure.HugeVertex;
import com.baidu.hugegraph.structure.HugeVertexProperty;

public class BinaryInlineSerializer extends BinarySerializer {

    public BinaryInlineSerializer() {
        super(true, true);
    }

    @Override
    public BackendEntry writeVertex(HugeVertex vertex) {
        BinaryBackendEntry entry = newBackendEntry(vertex);

        if (vertex.removed()) {
            return entry;
        }

        int propsCount = vertex.getProperties().size();
        BytesBuffer buffer = BytesBuffer.allocate(8 + 16 * propsCount);

        // Write vertex label
        buffer.writeId(vertex.schemaLabel().id());

        // Write all properties of the vertex
        this.formatProperties(vertex.getProperties().values(), buffer);

        entry.column(entry.id().asBytes(), buffer.bytes());

        return entry;
    }

    @Override
    public HugeVertex readVertex(HugeGraph graph, BackendEntry bytesEntry) {
        if (bytesEntry == null) {
            return null;
        }
        BinaryBackendEntry entry = this.convertEntry(bytesEntry);

        // Parse id
        Id id = entry.id().origin();
        Id vid = id.edge() ? ((EdgeId) id).ownerVertexId() : id;
        HugeVertex vertex = new HugeVertex(graph, vid, VertexLabel.NONE);

        // Parse all properties and edges of a Vertex
        for (BackendColumn col : entry.columns()) {
            if (id.edge()) {
                // Parse vertex edges
                this.parseColumn(col, vertex);
            } else {
                // Parse vertex properties
                assert entry.columnsSize() == 1 : entry.columnsSize();
                this.parseVertex(col.value, vertex);
            }
        }

        return vertex;
    }

    protected void parseVertex(byte[] value, HugeVertex vertex) {
        BytesBuffer buffer = BytesBuffer.wrap(value);

        // Parse vertex label
        VertexLabel label = vertex.graph().vertexLabel(buffer.readId());
        vertex.vertexLabel(label);

        // Parse properties
        this.parseProperties(buffer, vertex);
    }

    @Override
    public BackendEntry writeVertexProperty(HugeVertexProperty<?> prop) {
        throw new NotImplementedException("Unsupported writeVertexProperty()");
    }
}
