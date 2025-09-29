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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.hugegraph.backend.id.Id;
import org.apache.hugegraph.backend.store.BackendEntry;
import org.apache.hugegraph.type.HugeType;

/**
 * Vector backend entry for handling vector index data
 * This entry is specifically designed for vector operations and jvector integration
 */
public class VectorBackendEntry implements BackendEntry {

    // Basic fields (implementing BackendEntry interface)
    private final HugeType type;           // VECTOR_INDEX
    private final Id id;                   // index ID
    private final Id subId;                // vertex ID
    
    // Vector core fields
    private final String vectorId;         // vector id
    private final float[] vector;          // vector data
    private final String metricType;       // metric type (L2, COSINE, DOT)
    private final Integer dimension;       // vector dimension
    
    public VectorBackendEntry(HugeType type, Id id, Id subId, 
                             String vectorId, float[] vector, 
                             String metricType, Integer dimension) {
        this.type = type;
        this.id = id;
        this.subId = subId;
        this.vectorId = vectorId;
        this.vector = vector;
        this.metricType = metricType;
        this.dimension = dimension;
    }
    
    // BackendEntry interface implementation
    @Override
    public HugeType type() { 
        return this.type; 
    }
    
    @Override
    public Id id() { 
        return this.id; 
    }
    
    @Override
    public Id originId() {
        return this.id;
    }
    
    @Override
    public Id subId() { 
        return this.subId; 
    }
    
    @Override
    public long ttl() { 
        return 0L;  // Vector index doesn't expire
    }
    
    @Override
    public boolean olap() { 
        return false;  // Vector index is not OLAP data
    }
    
    // Vector-specific methods
    public String vectorId() { 
        return this.vectorId; 
    }
    
    public float[] vector() { 
        return this.vector; 
    }
    
    public String metricType() { 
        return this.metricType; 
    }
    
    public Integer dimension() { 
        return this.dimension; 
    }
    
    // For BackendEntry interface compatibility, provide columns method
    @Override
    public Collection<BackendColumn> columns() {
        List<BackendColumn> cols = new ArrayList<>();
        if (this.vector != null) {
            cols.add(BackendColumn.of("vector".getBytes(), this.serializeVector()));
        }
        if (this.metricType != null) {
            cols.add(BackendColumn.of("metric".getBytes(), this.metricType.getBytes()));
        }
        if (this.dimension != null) {
            cols.add(BackendColumn.of("dimension".getBytes(), 
                                    this.dimension.toString().getBytes()));
        }
        return Collections.unmodifiableList(cols);
    }
    
    @Override
    public int columnsSize() {
        return this.columns().size();
    }
    
    @Override
    public void columns(Collection<BackendColumn> columns) {
        // Vector index doesn't support dynamic column addition
        throw new UnsupportedOperationException("VectorBackendEntry doesn't support dynamic columns");
    }
    
    @Override
    public void columns(BackendColumn column) {
        throw new UnsupportedOperationException("VectorBackendEntry doesn't support dynamic columns");
    }
    
    @Override
    public void merge(BackendEntry other) {
        throw new UnsupportedOperationException("VectorBackendEntry doesn't support merge");
    }
    
    @Override
    public boolean mergeable(BackendEntry other) {
        return false;  // Vector index doesn't support merging
    }
    
    @Override
    public void clear() {
        throw new UnsupportedOperationException("VectorBackendEntry doesn't support clear");
    }
    
    @Override
    public boolean belongToMe(BackendColumn column) {
        // All columns of vector index belong to itself
        return true;
    }
    
    private byte[] serializeVector() {
        if (this.vector == null || this.vector.length == 0) {
            return new byte[0];
        }
        ByteBuffer buffer = ByteBuffer.allocate(this.vector.length * 4);
        for (float f : this.vector) {
            buffer.putFloat(f);
        }
        return buffer.array();
    }
    
    @Override
    public String toString() {
        return String.format("VectorBackendEntry{type=%s, id=%s, subId=%s, vectorId=%s, dimension=%d}", 
                           this.type, this.id, this.subId, this.vectorId, this.dimension);
    }
    
    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof VectorBackendEntry)) {
            return false;
        }
        VectorBackendEntry other = (VectorBackendEntry) obj;
        return this.type.equals(other.type) &&
               this.id.equals(other.id) &&
               this.subId.equals(other.subId) &&
               this.vectorId.equals(other.vectorId);
    }
    
    @Override
    public int hashCode() {
        return this.type.hashCode() ^ 
               this.id.hashCode() ^ 
               this.subId.hashCode() ^ 
               this.vectorId.hashCode();
    }
}
