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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Set;
import java.util.SortedMap;
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
import com.baidu.hugegraph.backend.store.BackendEntry.BackendColumn;
import com.baidu.hugegraph.backend.store.BackendSession;
import com.baidu.hugegraph.iterator.ExtendableIterator;
import com.baidu.hugegraph.structure.HugeIndex;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.define.HugeKeys;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.InsertionOrderUtil;
import com.baidu.hugegraph.util.NumericUtil;
import com.google.common.collect.ImmutableList;

public class InMemoryDBTables {

    public static class Vertex extends InMemoryDBTable {

        public Vertex() {
            super(HugeType.VERTEX);
        }
    }

    public static class Edge extends InMemoryDBTable {

        public Edge(HugeType type) {
            super(type);
        }

        @Override
        public void insert(BackendSession session, TextBackendEntry entry) {
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
        public void delete(BackendSession session, TextBackendEntry entry) {
            Id id = vertexIdOfEdge(entry);

            BackendEntry vertex = this.store().get(id);
            if (vertex != null) {
                ((TextBackendEntry) vertex).eliminate(entry);
            }
        }

        @Override
        public void append(BackendSession session, TextBackendEntry entry) {
            throw new UnsupportedOperationException("Edge append");
        }

        @Override
        public void eliminate(BackendSession session, TextBackendEntry entry) {
            throw new UnsupportedOperationException("Edge eliminate");
        }

        @Override
        protected Map<Id, BackendEntry> queryById(
                                        Set<Id> ids,
                                        Map<Id, BackendEntry> entries) {
            // Query edge(in a vertex) by id
            return this.queryEdgeById(ids, false, entries);
        }

        @Override
        protected Map<Id, BackendEntry> queryByIdPrefix(
                                        Id start,
                                        boolean inclusiveStart,
                                        Id prefix,
                                        Map<Id, BackendEntry> entries) {
            // Query edge(in a vertex) by v-id + column-name-prefix
            BackendEntry value = this.getEntryById(start, entries);
            if (value == null) {
                return Collections.emptyMap();
            }

            Map<Id, BackendEntry> rs = InsertionOrderUtil.newMap();

            // TODO: Compatible with BackendEntry
            TextBackendEntry entry = (TextBackendEntry) value;
            // Prefix edges in the vertex
            String startColumn = columnOfEdge(start);
            String prefixColumn = columnOfEdge(prefix);
            BackendEntry edges = new TextBackendEntry(HugeType.VERTEX,
                                                      entry.id());
            edges.columns(entry.columnsWithPrefix(startColumn, inclusiveStart,
                                                  prefixColumn));

            BackendEntry result = rs.get(entry.id());
            if (result == null) {
                rs.put(entry.id(), edges);
            } else {
                result.merge(edges);
            }

            return rs;
        }

        @Override
        protected Map<Id, BackendEntry> queryByIdRange(
                                        Id start,
                                        boolean inclusiveStart,
                                        Id end,
                                        boolean inclusiveEnd,
                                        Map<Id, BackendEntry> entries) {
            BackendEntry value = this.getEntryById(start, entries);
            if (value == null) {
                return Collections.emptyMap();
            }

            Map<Id, BackendEntry> rs = InsertionOrderUtil.newMap();

            // TODO: Compatible with BackendEntry
            TextBackendEntry entry = (TextBackendEntry) value;
            // Range edges in the vertex
            String startColumn = columnOfEdge(start);
            String endColumn = columnOfEdge(end);
            BackendEntry edges = new TextBackendEntry(HugeType.VERTEX,
                                                      entry.id());
            edges.columns(entry.columnsWithRange(startColumn, inclusiveStart,
                                                 endColumn, inclusiveEnd));

            BackendEntry result = rs.get(entry.id());
            if (result == null) {
                rs.put(entry.id(), edges);
            } else {
                result.merge(edges);
            }

            return rs;
        }

        private Map<Id, BackendEntry> queryEdgeById(
                                      Set<Id> ids, boolean prefix,
                                      Map<Id, BackendEntry> entries) {
            assert ids.size() > 0;
            Map<Id, BackendEntry> rs = InsertionOrderUtil.newMap();

            for (Id id : ids) {
                BackendEntry value = this.getEntryById(id, entries);
                if (value != null) {
                    // TODO: Compatible with BackendEntry
                    TextBackendEntry entry = (TextBackendEntry) value;
                    String column = columnOfEdge(id);
                    if (column == null) {
                        // All edges in the vertex
                        rs.put(entry.id(), entry);
                    } else if ((!prefix && entry.contains(column)) ||
                               (prefix && entry.containsPrefix(column))) {
                        BackendEntry edges = new TextBackendEntry(
                                                 HugeType.VERTEX, entry.id());
                        if (prefix) {
                            // Some edges with specified prefix in the vertex
                            edges.columns(entry.columnsWithPrefix(column));
                        } else {
                            // An edge with specified id in the vertex
                            BackendColumn col = entry.columns(column);
                            if (col != null) {
                                edges.columns(col);
                            }
                        }

                        BackendEntry result = rs.get(entry.id());
                        if (result == null) {
                            rs.put(entry.id(), edges);
                        } else {
                            result.merge(edges);
                        }
                    }
                }
            }

            return rs;
        }

        private BackendEntry getEntryById(Id id,
                                          Map<Id, BackendEntry> entries) {
            // TODO: improve id split
            Id entryId = IdGenerator.of(EdgeId.split(id)[0]);
            return entries.get(entryId);
        }

        @Override
        protected Map<Id, BackendEntry> queryByFilter(
                                        Set<Condition> conditions,
                                        Map<Id, BackendEntry> entries) {
            if (conditions.isEmpty()) {
                return entries;
            }

            // Only support querying edge by label
            E.checkState(conditions.size() == 1,
                         "Not support querying edge by %s", conditions);
            Condition cond = conditions.iterator().next();
            E.checkState(cond.isRelation(),
                         "Not support querying edge by %s", conditions);
            Condition.Relation relation = (Condition.Relation) cond;
            E.checkState(relation.key().equals(HugeKeys.LABEL),
                         "Not support querying edge by %s", conditions);
            String label = (String) relation.serialValue();

            Map<Id, BackendEntry> rs = InsertionOrderUtil.newMap();

            for (BackendEntry value : entries.values()) {
                // TODO: Compatible with BackendEntry
                TextBackendEntry entry = (TextBackendEntry) value;
                String out = EdgeId.concat(HugeType.EDGE_OUT.string(), label);
                String in = EdgeId.concat(HugeType.EDGE_IN.string(), label);
                if (entry.containsPrefix(out)) {
                    BackendEntry edges = new TextBackendEntry(HugeType.VERTEX,
                                                              entry.id());
                    edges.columns(entry.columnsWithPrefix(out));
                    rs.put(edges.id(), edges);
                }
                if (entry.containsPrefix(in)) {
                    BackendEntry edges = new TextBackendEntry(HugeType.VERTEX,
                                                              entry.id());
                    edges.columns(entry.columnsWithPrefix(in));
                    BackendEntry result = rs.get(edges.id());
                    if (result == null) {
                        rs.put(edges.id(), edges);
                    } else {
                        result.merge(edges);
                    }
                }
            }

            return rs;
        }

        @Override
        protected Iterator<BackendEntry> skipOffset(Iterator<BackendEntry> iter,
                                                    long offset) {
            long count = 0;
            BackendEntry last = null;
            while (count < offset && iter.hasNext()) {
                last = iter.next();
                count += last.columnsSize();
            }
            if (count == offset) {
                return iter;
            } else if (count < offset) {
                return Collections.emptyIterator();
            }

            // Collect edges that are over-skipped
            assert count > offset;
            assert last != null;
            int remaining = (int) (count - offset);
            last = ((TextBackendEntry) last).copyLast(remaining);

            ExtendableIterator<BackendEntry> all = new ExtendableIterator<>();
            all.extend(ImmutableList.of(last).iterator());
            all.extend(iter);
            return all;
        }

        @Override
        protected Iterator<BackendEntry> dropTails(Iterator<BackendEntry> iter,
                                                   long limit) {
            long count = 0;
            BackendEntry last = null;
            List<BackendEntry> entries = new ArrayList<>();
            while (count < limit && iter.hasNext()) {
                last = iter.next();
                count += last.columnsSize();
                entries.add(last);
            }
            if (count <= limit) {
                return entries.iterator();
            }

            // Drop edges that are over-fetched
            assert count > limit;
            assert last != null;
            int head = (int) (limit + last.columnsSize() - count);
            last = ((TextBackendEntry) last).copyHead(head);
            entries.remove(entries.size() - 1);
            entries.add(last);
            return entries.iterator();
        }

        private static Id vertexIdOfEdge(TextBackendEntry entry) {
            assert entry.type().isEdge();
            // Assume the first part is owner vertex id
            String vertexId = EdgeId.split(entry.id())[0];
            return IdGenerator.of(vertexId);
        }

        private static String columnOfEdge(Id id) {
            // TODO: improve id split
            String[] parts = EdgeId.split(id);
            if (parts.length > 1) {
                parts = Arrays.copyOfRange(parts, 1, parts.length);
                return EdgeId.concat(parts);
            } else {
                // All edges
                assert parts.length == 1;
            }
            return null;
        }
    }

    public static class SecondaryIndex extends InMemoryDBTable {

        public SecondaryIndex() {
            super(HugeType.SECONDARY_INDEX);
        }

        protected SecondaryIndex(HugeType type) {
            super(type);
        }

        @Override
        public Iterator<BackendEntry> query(BackendSession session,
                                            Query query) {
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
                                 "FIELD_VALUES or INDEX_LABEL_ID, but got: %s",
                                 r.key());
                }
            }
            assert fieldValue != null && indexLabelId != null;
            Id id = SplicingIdGenerator.splicing(indexLabelId, fieldValue);
            IdQuery q = new IdQuery(query, id);
            q.offset(query.offset());
            q.limit(query.limit());
            return super.query(session, q);
        }

        @Override
        public void delete(BackendSession session, TextBackendEntry entry) {
            // Delete by index label
            assert entry.columnsSize() == 1;
            String indexLabel = entry.column(HugeKeys.INDEX_LABEL_ID);
            E.checkState(indexLabel != null, "Expect index label");

            Iterator<Entry<Id, BackendEntry>> iter;
            for (iter = this.store().entrySet().iterator(); iter.hasNext();) {
                Entry<Id, BackendEntry> e = iter.next();
                // Delete if prefix with index label
                if (e.getKey().asString().startsWith(indexLabel)) {
                    iter.remove();
                }
            }
        }
    }

    public static class SearchIndex extends SecondaryIndex {

        public SearchIndex() {
            super(HugeType.SEARCH_INDEX);
        }
    }

    public static class RangeIndex extends InMemoryDBTable {

        public RangeIndex() {
            this(HugeType.RANGE_INDEX);
        }

        protected RangeIndex(HugeType type) {
            super(type, new ConcurrentSkipListMap<>());
        }

        @Override
        protected NavigableMap<Id, BackendEntry> store() {
            return (NavigableMap<Id, BackendEntry>) super.store();
        }

        @Override
        public Iterator<BackendEntry> query(BackendSession session,
                                            Query query) {
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
                Id id = HugeIndex.formatIndexId(query.resultType(),
                                                indexLabelId, keyEq);
                IdQuery q = new IdQuery(query, id);
                q.offset(query.offset());
                q.limit(query.limit());
                return super.query(session, q);
            }
            // keyMin <(=) field value <(=) keyMax
            return this.betweenQuery(indexLabelId, keyMax, keyMaxEq,
                                     keyMin, keyMinEq, query);
        }

        private Iterator<BackendEntry> betweenQuery(Id indexLabelId,
                                                    Object keyMax,
                                                    boolean keyMaxEq,
                                                    Object keyMin,
                                                    boolean keyMinEq,
                                                    Query query) {
            NavigableMap<Id, BackendEntry> rs = this.store();

            E.checkArgument(keyMin != null || keyMax != null,
                            "Please specify at least one condition");
            if (keyMin == null) {
                // Field value < keyMax
                keyMin = NumericUtil.minValueOf(keyMax.getClass());
            }
            Id min = HugeIndex.formatIndexId(query.resultType(),
                                             indexLabelId, keyMin);

            if (keyMax == null) {
                // Field value > keyMin
                keyMaxEq = false;
                indexLabelId = IdGenerator.of(indexLabelId.asLong() + 1L);
                keyMax = NumericUtil.minValueOf(keyMin.getClass());
            }
            Id max = HugeIndex.formatIndexId(query.resultType(),
                                             indexLabelId, keyMax);

            max = keyMaxEq ? rs.floorKey(max) : rs.lowerKey(max);
            if (max == null) {
                return Collections.emptyIterator();
            }

            Map<Id, BackendEntry> results = new HashMap<>();
            Map.Entry<Id, BackendEntry> entry = keyMinEq ?
                                                rs.ceilingEntry(min) :
                                                rs.higherEntry(min);
            while (entry != null) {
                if (entry.getKey().compareTo(max) > 0) {
                    break;
                }
                results.put(entry.getKey(), entry.getValue());
                entry = rs.higherEntry(entry.getKey());
            }
            return results.values().iterator();
        }

        @Override
        public void delete(BackendSession session, TextBackendEntry entry) {
            // Delete by index label
            assert entry.columnsSize() == 1;
            String indexLabel = entry.column(HugeKeys.INDEX_LABEL_ID);
            E.checkState(indexLabel != null, "Expect index label");

            Id indexLabelId = IdGenerator.of(indexLabel);
            Id min = HugeIndex.formatIndexId(entry.type(), indexLabelId, 0L);
            indexLabelId = IdGenerator.of(indexLabelId.asLong() + 1L);
            Id max = HugeIndex.formatIndexId(entry.type(), indexLabelId, 0L);
            SortedMap<Id, BackendEntry> subStore;
            subStore = this.store().subMap(min, max);
            Iterator<Entry<Id, BackendEntry>> iter;
            for (iter = subStore.entrySet().iterator(); iter.hasNext();) {
                iter.next();
                // Delete if prefix with index label
                iter.remove();
            }
        }
    }

    public static class ShardIndex extends RangeIndex {

        public ShardIndex() {
            super(HugeType.SHARD_INDEX);
        }
    }
}
