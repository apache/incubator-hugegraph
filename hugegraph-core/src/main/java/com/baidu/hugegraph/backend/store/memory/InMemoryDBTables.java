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

package com.baidu.hugegraph.backend.store.memory;

import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.id.IdGenerator;
import com.baidu.hugegraph.backend.id.SplicingIdGenerator;
import com.baidu.hugegraph.backend.serializer.TextBackendEntry;
import com.baidu.hugegraph.backend.store.BackendEntry;
import com.baidu.hugegraph.type.HugeType;

public class InMemoryDBTables {

    public static class Vertex extends InMemoryDBTable {

        public Vertex() {
            super(HugeType.VERTEX);
        }
    }

    public static class Edge extends InMemoryDBTable {

        public Edge(Vertex vertex) {
            // Edges are stored as columns of corresponding vertices now
            super(HugeType.EDGE, vertex.store());
        }

        @Override
        public void insert(TextBackendEntry entry) {
            Id id = vertexIdOfEdge(entry);

            if (!this.store().containsKey(id)) {
                BackendEntry vertex = new TextBackendEntry(HugeType.VERTEX, id);
                vertex.merge(entry);
                this.store().put(id, vertex);
            } else {
                // Merge columns if the entry exists
                BackendEntry vertex = this.store().get(id);
                vertex.merge(entry);
            }
        }

        @Override
        public void delete(TextBackendEntry entry) {
            Id id = vertexIdOfEdge(entry);

            BackendEntry vertex = this.store().get(id);
            if (vertex != null) {
                ((TextBackendEntry) vertex).eliminate(entry);
            }
        }

        @Override
        public void append(TextBackendEntry entry) {
            throw new UnsupportedOperationException("Edge append");
        }

        @Override
        public void eliminate(TextBackendEntry entry) {
            throw new UnsupportedOperationException("Edge eliminate");
        }

        private static Id vertexIdOfEdge(TextBackendEntry entry) {
            assert entry.type() == HugeType.EDGE;
            String vertexId = SplicingIdGenerator.split(entry.id())[0];
            return IdGenerator.of(vertexId);
        }
    }
}
