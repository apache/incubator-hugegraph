/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hugegraph.backend.serializer;

import java.util.List;

import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.backend.query.ConditionQuery;
import org.apache.hugegraph.backend.query.Query;
import org.apache.hugegraph.backend.store.BackendEntry;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.schema.EdgeLabel;
import org.apache.hugegraph.schema.IndexLabel;
import org.apache.hugegraph.schema.PropertyKey;
import org.apache.hugegraph.schema.VertexLabel;
import org.apache.hugegraph.structure.HugeEdge;
import org.apache.hugegraph.structure.HugeEdgeProperty;
import org.apache.hugegraph.structure.HugeIndex;
import org.apache.hugegraph.structure.HugeVertex;
import org.apache.hugegraph.structure.HugeVertexProperty;
import org.apache.hugegraph.type.HugeType;
import org.apache.hugegraph.util.Log;
import org.slf4j.Logger;

/**
 * Vector serializer for handling vector index serialization
 * This serializer only handles vector index operations, not general graph data
 */
public class VectorSerializer extends AbstractSerializer {

    private static final Logger LOG = Log.logger(VectorSerializer.class);

    public VectorSerializer() {
        super();
    }

    public VectorSerializer(HugeConfig config) {
        super(config);
    }

    @Override
    protected BackendEntry newBackendEntry(HugeType type, Id id) {
        if (!type.isVectorIndex()) {
            throw new UnsupportedOperationException("VectorSerializer only supports vector index " +
                                                    "types");
        }
        // For vector index, return empty VectorBackendEntry
        return new VectorBackendEntry(type, id, id, id.asString(), new float[0], "L2", 0);
    }

    @Override
    protected BackendEntry convertEntry(BackendEntry entry) {
        if (entry instanceof VectorBackendEntry) {
            return entry;
        }
        throw new IllegalArgumentException("Expected VectorBackendEntry, got: " + entry.getClass());
    }

    @Override
    public BackendEntry writeVertex(HugeVertex vertex) {
        // Vector serializer doesn't handle vertex data
        throw new UnsupportedOperationException("VectorSerializer doesn't handle vertex data");
    }

    @Override
    public BackendEntry writeOlapVertex(HugeVertex vertex) {
        throw new UnsupportedOperationException("VectorSerializer doesn't handle OlapVertex data");
    }

    @Override
    public BackendEntry writeVertexProperty(HugeVertexProperty<?> prop) {
        throw new UnsupportedOperationException("VectorSerializer doesn't handle VertexProperty");
    }

    @Override
    public HugeVertex readVertex(HugeGraph graph, BackendEntry bytesEntry) {
        // Vector serializer doesn't handle vertex data
        throw new UnsupportedOperationException("VectorSerializer doesn't handle vertex data");
    }

    @Override
    public BackendEntry writeEdge(HugeEdge edge) {
        throw new UnsupportedOperationException("VectorSerializer doesn't handle edge data");
    }

    @Override
    public BackendEntry writeEdgeProperty(HugeEdgeProperty<?> prop) {
        throw new UnsupportedOperationException("VectorSerializer doesn't handle edge data");
    }

    @Override
    public HugeEdge readEdge(HugeGraph graph, BackendEntry entry) {
        return null;
    }

    @Override
    public BackendEntry writeIndex(HugeIndex index) {
        if (!index.type().isVectorIndex()) {
            throw new UnsupportedOperationException("VectorSerializer only handles vector index");
        }

        if (index.fieldValues() == null && index.elementIds().isEmpty()) {
            // Deletion case - return empty VectorBackendEntry
            return new VectorBackendEntry(
                index.type(),
                index.id(),
                index.elementId(),
                index.elementId().asString(),
                new float[0],
                "L2",  // default metric type
                0
            );
        } else {
            // Normal vector index entry
            Id vertexId = index.elementId();
            HugeType type = index.type();
            
            // Extract vector data
            float[] vector = new float[0];
            if (index.fieldValues() instanceof List) {
                @SuppressWarnings("unchecked")
                List<Float> vectorList = (List<Float>) index.fieldValues();
                vector = new float[vectorList.size()];
                for (int i = 0; i < vectorList.size(); i++) {
                    vector[i] = vectorList.get(i);
                }
            }
            
            // Create VectorBackendEntry
            return new VectorBackendEntry(
                type,
                index.id(),
                vertexId,
                vertexId.asString(),  // use vertexId as vectorId
                vector,
                "L2",  // default metric type, can be configured later
                vector.length
            );
        }
    }

    @Override
    public HugeIndex readIndex(HugeGraph graph, ConditionQuery query, BackendEntry entry) {
        return null;
    }

    public HugeIndex readIndex(HugeGraph graph, Query query, BackendEntry bytesEntry) {
        if (bytesEntry == null) {
            return null;
        }

        BackendEntry entry = this.convertEntry(bytesEntry);
        if (!entry.type().isVectorIndex()) {
            throw new UnsupportedOperationException("VectorSerializer only handles vector index");
        }

        // Parse vector index from entry
        // TODO: Implement vector index parsing logic
        return null;
    }

    @Override
    public BackendEntry writeId(HugeType type, Id id) {
        if (type.isVectorIndex()) {
            return newBackendEntry(type, id);
        }
        throw new UnsupportedOperationException("VectorSerializer only supports vector index types");
    }

    @Override
    protected Id writeQueryId(HugeType type, Id id) {
        if (type.isVectorIndex()) {
            // For vector index, return the original ID
            return id;
        }
        throw new UnsupportedOperationException("VectorSerializer only supports vector index types");
    }

    @Override
    protected Query writeQueryEdgeCondition(Query query) {
        return null;
    }

    @Override
    protected Query writeQueryCondition(Query query) {
        HugeType type = query.resultType();
        if (!type.isVectorIndex()) {
            throw new UnsupportedOperationException("VectorSerializer only handles vector index queries");
        }
        
        // TODO: Implement vector index query condition writing
        return query;
    }

    /**
     * Format vector index deletion entry
     */
    private VectorBackendEntry formatVectorIndexDeletion(HugeIndex index) {
        // Return empty VectorBackendEntry for deletion
        return new VectorBackendEntry(
            index.type(),
            index.id(),
            index.elementId(),
            index.elementId().asString(),
            new float[0],
            "L2",
            0
        );
    }

    /**
     * Format vector index name for column storage
     */
    private byte[] formatVectorIndexName(HugeIndex index) {
        // Use vertex ID as the column name for vector index
        return index.elementId().asBytes();
    }

    /**
     * Serialize vector data to bytes
     */
    private byte[] serializeVector(List<Float> vector) {
        // Convert List<Float> to byte array
        byte[] bytes = new byte[vector.size() * 4]; // 4 bytes per float
        for (int i = 0; i < vector.size(); i++) {
            int bits = Float.floatToIntBits(vector.get(i));
            bytes[i * 4] = (byte) (bits >> 24);
            bytes[i * 4 + 1] = (byte) (bits >> 16);
            bytes[i * 4 + 2] = (byte) (bits >> 8);
            bytes[i * 4 + 3] = (byte) bits;
        }
        return bytes;
    }

    /**
     * Deserialize vector data from bytes
     */
    private List<Float> deserializeVector(byte[] bytes) {
        // Convert byte array to List<Float>
        List<Float> vector = new java.util.ArrayList<>();
        for (int i = 0; i < bytes.length; i += 4) {
            int bits = ((bytes[i] & 0xFF) << 24) |
                      ((bytes[i + 1] & 0xFF) << 16) |
                      ((bytes[i + 2] & 0xFF) << 8) |
                      (bytes[i + 3] & 0xFF);
            vector.add(Float.intBitsToFloat(bits));
        }
        return vector;
    }

    @Override
    public BackendEntry writeVertexLabel(VertexLabel vertexLabel) {
        return null;
    }

    @Override
    public VertexLabel readVertexLabel(HugeGraph graph, BackendEntry entry) {
        return null;
    }

    @Override
    public BackendEntry writeEdgeLabel(EdgeLabel edgeLabel) {
        return null;
    }

    @Override
    public EdgeLabel readEdgeLabel(HugeGraph graph, BackendEntry entry) {
        return null;
    }

    @Override
    public BackendEntry writePropertyKey(PropertyKey propertyKey) {
        return null;
    }

    @Override
    public PropertyKey readPropertyKey(HugeGraph graph, BackendEntry entry) {
        return null;
    }

    @Override
    public BackendEntry writeIndexLabel(IndexLabel indexLabel) {
        return null;
    }

    @Override
    public IndexLabel readIndexLabel(HugeGraph graph, BackendEntry entry) {
        return null;
    }
}
