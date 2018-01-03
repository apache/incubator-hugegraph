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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;

import com.baidu.hugegraph.backend.id.EdgeId;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.id.IdGenerator;
import com.baidu.hugegraph.backend.id.SplicingIdGenerator;
import com.baidu.hugegraph.backend.query.Condition;
import com.baidu.hugegraph.backend.query.ConditionQuery;
import com.baidu.hugegraph.backend.query.IdQuery;
import com.baidu.hugegraph.backend.query.Query;
import com.baidu.hugegraph.backend.serializer.TextBackendEntry;
import com.baidu.hugegraph.backend.store.BackendEntry;
import com.baidu.hugegraph.structure.HugeIndex;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.define.HugeKeys;
import com.baidu.hugegraph.util.E;

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
            // Assume the first part is owner vertex id
            String vertexId = EdgeId.split(entry.id())[0];
            return IdGenerator.of(vertexId);
        }
    }

    public static class SecondaryIndex extends InMemoryDBTable {

        public SecondaryIndex() {
            super(HugeType.SECONDARY_INDEX);
        }
        @Override
        public Iterator<BackendEntry> query(final Query query) {
            Set<Condition> conditions = query.conditions();
            E.checkState(query instanceof ConditionQuery &&
                         conditions.size() == 2,
                         "Secondary index query must be condition query " +
                         "and have two conditions, but got: %s", query);
            String fieldValue = null;
            String indexLabelId = null;
            for (Condition c : conditions) {
                assert c instanceof Condition.Relation;
                Condition.Relation r = (Condition.Relation) c;
                if (r.key() == HugeKeys.FIELD_VALUES) {
                    fieldValue = r.value().toString();
                } else if (r.key() == HugeKeys.INDEX_LABEL_ID) {
                    indexLabelId = r.value().toString();
                } else {
                    E.checkState(false,
                                 "Secondary index query conditions must be" +
                                 "field_values or index_label_id, but got: %s",
                                 r.key());
                }
            }
            assert fieldValue != null && indexLabelId != null;
            Id id = SplicingIdGenerator.splicing(fieldValue, indexLabelId);
            IdQuery q = new IdQuery(query, id);
            return super.query(q);
        }
    }

    public static class RangeIndex extends InMemoryDBTable {

        public RangeIndex() {
            super(HugeType.RANGE_INDEX, new ConcurrentSkipListMap<>());
        }

        @Override
        protected NavigableMap<Id, BackendEntry> store() {
            return (NavigableMap<Id, BackendEntry>) super.store();
        }

        @Override
        public Iterator<BackendEntry> query(final Query query) {
            Set<Condition> conditions = query.conditions();
            E.checkState(query instanceof ConditionQuery &&
                         (conditions.size() == 3 || conditions.size() == 2),
                         "Range index query must be condition query" +
                         " and have 2 or 3 conditions, but got: %s", query);

            List<Condition.Relation> relations = new ArrayList<>();
            Id indexLabelId = null;
            for (Condition.Relation r : ((ConditionQuery) query).relations()) {
                if (r.key().equals(HugeKeys.INDEX_LABEL_ID)) {
                    indexLabelId = (Id) r.value();
                    continue;
                }
                relations.add(r);
            }
            assert indexLabelId != null;

            Object keyEq = null;
            Object keyMin = null;
            boolean keyMinEq = false;
            Object keyMax = null;
            boolean keyMaxEq = false;

            for (Condition.Relation r : relations) {
                E.checkArgument(r.key() == HugeKeys.FIELD_VALUES,
                                "Expect FIELD_VALUES in AND condition");
                switch (r.relation()) {
                    case EQ:
                        keyEq = r.value();
                        break;
                    case GTE:
                        keyMinEq = true;
                    case GT:
                        keyMin = r.value();
                        break;
                    case LTE:
                        keyMaxEq = true;
                    case LT:
                        keyMax = r.value();
                        break;
                    default:
                        E.checkArgument(false, "Unsupported relation '%s'",
                                        r.relation());
                        break;
                }
            }

            if (keyEq != null) {
                Id id = HugeIndex.formatIndexId(HugeType.RANGE_INDEX,
                                                indexLabelId, keyEq);
                IdQuery q = new IdQuery(query, id);
                return super.query(q);
            } else {
                if (keyMin == null) {
                    // Field value < keyMax
                    assert keyMax != null;
                    return this.ltQuery(indexLabelId, keyMax, keyMaxEq);
                } else {
                    if (keyMax == null) {
                        // Field value > keyMin
                        return this.gtQuery(indexLabelId, keyMin, keyMinEq);
                    } else {
                        // keyMin <(=) field value <(=) keyMax
                        return this.betweenQuery(indexLabelId,
                                                 keyMax, keyMaxEq,
                                                 keyMin, keyMinEq);
                    }
                }
            }
        }

        private Iterator<BackendEntry> ltQuery(Id indexLabelId,
                                               Object keyMax,
                                               boolean keyMaxEq) {
            NavigableMap<Id, BackendEntry> rs = this.store();
            Map<Id, BackendEntry> results = new HashMap<>();

            Id min = HugeIndex.formatIndexId(HugeType.RANGE_INDEX,
                                             indexLabelId, 0L);
            Id max = HugeIndex.formatIndexId(HugeType.RANGE_INDEX,
                                             indexLabelId, keyMax);
            Map.Entry<Id, BackendEntry> entry = keyMaxEq ?
                                                rs.floorEntry(max) :
                                                rs.lowerEntry(max);
            while (entry != null) {
                if (entry.getKey().compareTo(min) < 0) {
                    break;
                }
                results.put(entry.getKey(), entry.getValue());
                entry = rs.lowerEntry(entry.getKey());
            }
            return results.values().iterator();
        }

        private Iterator<BackendEntry> gtQuery(Id indexLabelId,
                                               Object keyMin,
                                               boolean keyMinEq) {
            NavigableMap<Id, BackendEntry> rs = this.store();
            Map<Id, BackendEntry> results = new HashMap<>();

            Id min = HugeIndex.formatIndexId(HugeType.RANGE_INDEX,
                                             indexLabelId, keyMin);
            indexLabelId = IdGenerator.of(indexLabelId.asLong() + 1L);
            Id max = HugeIndex.formatIndexId(HugeType.RANGE_INDEX,
                                             indexLabelId, 0L);
            Map.Entry<Id, BackendEntry> entry = keyMinEq ?
                                                rs.ceilingEntry(min) :
                                                rs.higherEntry(min);
            while (entry != null) {
                if (entry.getKey().compareTo(max) >= 0) {
                    break;
                }
                results.put(entry.getKey(), entry.getValue());
                entry = rs.higherEntry(entry.getKey());
            }
            return results.values().iterator();
        }

        private Iterator<BackendEntry> betweenQuery(Id indexLabelId,
                                                    Object keyMax,
                                                    boolean keyMaxEq,
                                                    Object keyMin,
                                                    boolean keyMinEq) {
            NavigableMap<Id, BackendEntry> rs = this.store();
            Map<Id, BackendEntry> results = new HashMap<>();

            Id min = HugeIndex.formatIndexId(HugeType.RANGE_INDEX,
                                             indexLabelId, keyMin);
            Id max = HugeIndex.formatIndexId(HugeType.RANGE_INDEX,
                                             indexLabelId, keyMax);
            Map.Entry<Id, BackendEntry> entry = keyMinEq ?
                                                rs.ceilingEntry(min) :
                                                rs.higherEntry(min);
            max = keyMaxEq ? rs.floorKey(max) : rs.lowerKey(max);
            while (entry != null) {
                if (entry.getKey().compareTo(max) > 0) {
                    break;
                }
                results.put(entry.getKey(), entry.getValue());
                entry = rs.higherEntry(entry.getKey());
            }
            return results.values().iterator();
        }
    }
}
