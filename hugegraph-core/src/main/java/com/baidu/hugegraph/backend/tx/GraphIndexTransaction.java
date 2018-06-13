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

package com.baidu.hugegraph.backend.tx;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import com.baidu.hugegraph.HugeException;
import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.id.SplicingIdGenerator;
import com.baidu.hugegraph.backend.query.Condition;
import com.baidu.hugegraph.backend.query.ConditionQuery;
import com.baidu.hugegraph.backend.query.IdQuery;
import com.baidu.hugegraph.backend.query.Query;
import com.baidu.hugegraph.backend.store.BackendEntry;
import com.baidu.hugegraph.backend.store.BackendStore;
import com.baidu.hugegraph.perf.PerfUtil.Watched;
import com.baidu.hugegraph.schema.EdgeLabel;
import com.baidu.hugegraph.schema.IndexLabel;
import com.baidu.hugegraph.schema.PropertyKey;
import com.baidu.hugegraph.schema.SchemaElement;
import com.baidu.hugegraph.schema.SchemaLabel;
import com.baidu.hugegraph.schema.VertexLabel;
import com.baidu.hugegraph.structure.HugeEdge;
import com.baidu.hugegraph.structure.HugeElement;
import com.baidu.hugegraph.structure.HugeIndex;
import com.baidu.hugegraph.structure.HugeProperty;
import com.baidu.hugegraph.structure.HugeVertex;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.define.HugeKeys;
import com.baidu.hugegraph.type.define.IndexType;
import com.baidu.hugegraph.util.CollectionUtil;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.InsertionOrderUtil;
import com.baidu.hugegraph.util.LockUtil;
import com.baidu.hugegraph.util.NumericUtil;
import com.google.common.collect.ImmutableSet;

public class GraphIndexTransaction extends AbstractTransaction {

    private static final String INDEX_EMPTY_SYM = "\u0000";
    private static final Query EMPTY_QUERY = new ConditionQuery(null);

    public GraphIndexTransaction(HugeGraph graph, BackendStore store) {
        super(graph, store);
    }

    protected void removeIndexLeft(ConditionQuery query, HugeElement element) {
        if (element.type() != HugeType.VERTEX &&
            element.type() != HugeType.EDGE_OUT &&
            element.type() != HugeType.EDGE_IN) {
            throw new HugeException("Only accept element of type VERTEX and " +
                                    "EDGE to remove left index, but got: '%s'",
                                    element.type());
        }

        // TODO: remove left index in async thread
        // Process range index
        this.processRangeIndexLeft(query, element);
        // Process secondary index
        this.processSecondaryIndexLeft(query, element);

        this.commit();
    }

    private void processRangeIndexLeft(ConditionQuery query,
                                       HugeElement element) {
        // Construct index ConditionQuery
        Set<MatchedLabel> labels = collectMatchedLabels(query);
        IndexQueries queries = null;
        for (MatchedLabel label : labels) {
            if (label.schemaLabel().id().equals(element.schemaLabel().id())) {
                queries = label.constructQueries(query);
                break;
            }
        }
        if (queries == null) {
            throw new HugeException(
                      "Can't construct index query for '%s'", query);
        }

        for (ConditionQuery q : queries.values()) {
            if (q.resultType() != HugeType.RANGE_INDEX) {
                continue;
            }
            // Search and delete index equals element id
            for (Iterator<BackendEntry> it = super.query(q); it.hasNext();) {
                BackendEntry entry = it.next();
                HugeIndex index = this.serializer.readIndex(graph(), entry);
                if (index.elementIds().contains(element.id())) {
                    index.resetElementIds();
                    index.elementIds(element.id());
                    this.doEliminate(this.serializer.writeIndex(index));
                }
            }
        }
    }

    private void processSecondaryIndexLeft(ConditionQuery query,
                                           HugeElement element) {
        HugeElement elem = element.copyAsFresh();
        Set<Id> propKeys = query.userpropKeys();
        Set<Id> incorrectPKs = InsertionOrderUtil.newSet();
        for (Id key : propKeys) {
            Set<Object> conditionValues = query.userpropValue(key);
            E.checkState(!conditionValues.isEmpty(),
                         "Expect user property values for key '%s', " +
                         "but got none", key);
            if (conditionValues.size() > 1) {
                // It's inside/between Query (processed in range index)
                return;
            }
            Object propValue = elem.getProperty(key).value();
            Object conditionValue = conditionValues.iterator().next();
            if (!propValue.equals(conditionValue)) {
                PropertyKey pkey = this.graph().propertyKey(key);
                elem.addProperty(pkey, conditionValue);
                incorrectPKs.add(key);
            }
        }
        this.removeSecondaryIndexLeft(element, elem, incorrectPKs);
    }

    private void removeSecondaryIndexLeft(HugeElement correctElem,
                                          HugeElement incorrectElem,
                                          Set<Id> propKeys) {
        for (IndexLabel il : relatedIndexLabels(incorrectElem)) {
            if (CollectionUtils.containsAny(il.indexFields(), propKeys)) {
                this.updateIndex(il.id(), incorrectElem, true);
                this.updateIndex(il.id(), correctElem, false);
            }
        }
    }

    @Watched(prefix = "index")
    public void updateLabelIndex(HugeElement element, boolean removed) {
        if (!this.needIndexForLabel()) {
            return;
        }

        // Don't update label index if it's not enabled
        if (!element.schemaLabel().enableLabelIndex()) {
            return;
        }

        // Update label index if backend store not supports label-query
        HugeIndex index = new HugeIndex(IndexLabel.label(element.type()));
        index.fieldValues(element.schemaLabel().id().asLong());
        index.elementIds(element.id());

        if (removed) {
            this.doEliminate(this.serializer.writeIndex(index));
        } else {
            this.doAppend(this.serializer.writeIndex(index));
        }
    }

    @Watched(prefix = "index")
    public void updateVertexIndex(HugeVertex vertex, boolean removed) {
        // Update index(only property, no edge) of a vertex
        for (Id id : vertex.schemaLabel().indexLabels()) {
            this.updateIndex(id, vertex, removed);
        }
    }

    @Watched(prefix = "index")
    public void updateEdgeIndex(HugeEdge edge, boolean removed) {
        // Update index of an edge
        for (Id id : edge.schemaLabel().indexLabels()) {
            this.updateIndex(id, edge, removed);
        }
    }

    /**
     * Update index(user properties) of vertex or edge
     */
    private void updateIndex(Id ilId, HugeElement element, boolean removed) {
        SchemaTransaction schema = graph().schemaTransaction();
        IndexLabel indexLabel = schema.getIndexLabel(ilId);
        E.checkArgument(indexLabel != null,
                        "Not exist index label id: '%s'", ilId);

        List<Object> propValues = new ArrayList<>();
        for (Id fieldId : indexLabel.indexFields()) {
            HugeProperty<Object> property = element.getProperty(fieldId);
            if (property == null) {
                E.checkState(hasNullableProp(element, fieldId),
                             "Non-null property '%s' is null for '%s'",
                             this.graph().propertyKey(fieldId) , element);
                // Not build index for record with nullable field
                break;
            }
            propValues.add(property.value());
        }

        for (int i = 0, n = propValues.size(); i < n; i++) {
            List<Object> subPropValues = propValues.subList(0, i + 1);

            Object propValue;
            if (indexLabel.indexType() == IndexType.SECONDARY) {
                propValue = SplicingIdGenerator.concatValues(subPropValues);
                // Use `\u0000` as escape for empty String and treat it as
                // illegal value for text property
                E.checkArgument(!propValue.equals(INDEX_EMPTY_SYM),
                                "Illegal value of text property: '%s'",
                                INDEX_EMPTY_SYM);
                if (((String) propValue).isEmpty()) {
                    propValue = INDEX_EMPTY_SYM;
                }
            } else {
                assert indexLabel.indexType() == IndexType.RANGE;
                E.checkState(subPropValues.size() == 1,
                             "Expect range query by only one property");
                propValue = NumericUtil.convertToNumber(subPropValues.get(0));
            }

            HugeIndex index = new HugeIndex(indexLabel);
            index.fieldValues(propValue);
            index.elementIds(element.id());

            if (removed) {
                this.doEliminate(this.serializer.writeIndex(index));
            } else {
                this.doAppend(this.serializer.writeIndex(index));
            }
        }
    }

    /**
     * Composite index, an index involving multiple columns.
     * Single index, an index involving only one column.
     * Joint indexes, join of single indexes, composite indexes or mixed
     * of single indexes and composite indexes.
     */
    @Watched(prefix = "index")
    public Query query(ConditionQuery query) {
        // Index query must have been flattened in Graph tx
        query.checkFlattened();

        // NOTE: Currently we can't support filter changes in memory
        if (this.hasUpdates()) {
            throw new BackendException("Can't do index query when " +
                                       "there are changes in transaction");
        }

        // Can't query by index and by non-label sysprop at the same time
        List<Condition> conds = query.syspropConditions();
        if (conds.size() > 1 ||
            (conds.size() == 1 && !query.containsCondition(HugeKeys.LABEL))) {
            throw new BackendException("Can't do index query with %s", conds);
        }

        // Query by index
        query.optimized(OptimizedType.INDEX.ordinal());
        Set<Id> ids;
        if (query.allSysprop() && conds.size() == 1 &&
            query.containsCondition(HugeKeys.LABEL)) {
            // Query only by label
            ids = this.queryByLabel(query);
        } else {
            // Query by userprops (or userprops + label)
            ids = this.queryByUserprop(query);
        }

        if (ids.isEmpty()) {
            return EMPTY_QUERY;
        }

        // Wrap id(s) by IdQuery
        return new IdQuery(query, ids);
    }

    @Watched(prefix = "index")
    private Set<Id> queryByLabel(ConditionQuery query) {
        IndexLabel il = IndexLabel.label(query.resultType());
        Id label = (Id) query.condition(HugeKeys.LABEL);
        assert label != null;

        ConditionQuery indexQuery;
        indexQuery = new ConditionQuery(HugeType.SECONDARY_INDEX, query);
        indexQuery.eq(HugeKeys.INDEX_LABEL_ID, il.id());
        indexQuery.eq(HugeKeys.FIELD_VALUES, label);
        /*
         * Set offset and limit for single index or composite index
         * to avoid redundant element ids
         */
        indexQuery.limit(query.limit());
        indexQuery.offset(query.offset());

        return this.doIndexQuery(il, indexQuery);
    }

    @Watched(prefix = "index")
    private Set<Id> queryByUserprop(ConditionQuery query) {
        // Get user applied label or collect all qualified labels with
        // related index labels
        Set<MatchedLabel> labels = collectMatchedLabels(query);
        if (labels.isEmpty()) {
            throw noIndexException(this.graph(), query, "<any label>");
        }

        // Value type of Condition not matched
        if (!validQueryConditionValues(this.graph(), query)) {
            return ImmutableSet.of();
        }
        // Do index query
        Set<Id> ids = InsertionOrderUtil.newSet();
        for (MatchedLabel label : labels) {
            IndexQueries queries = label.constructQueries(query);
            ids.addAll(this.doMultiIndexQuery(queries));
            if (query.reachLimit(ids.size())) {
                break;
            }
        }
        return limit(ids, query);
    }

    private boolean needIndexForLabel() {
        return !this.store().features().supportsQueryByLabel();
    }

    private Set<Id> doIndexQuery(IndexLabel indexLabel, ConditionQuery query) {
        Set<Id> ids = InsertionOrderUtil.newSet();
        LockUtil.Locks locks = new LockUtil.Locks();
        try {
            locks.lockReads(LockUtil.INDEX_LABEL, indexLabel.id());
            locks.lockReads(LockUtil.INDEX_REBUILD, indexLabel.id());

            Iterator<BackendEntry> entries = super.query(query);
            while(entries.hasNext()) {
                BackendEntry entry = entries.next();
                HugeIndex index = this.serializer.readIndex(graph(), entry);
                ids.addAll(index.elementIds());
            }
        } finally {
            locks.unlock();
        }
        return ids;
    }

    @SuppressWarnings("unchecked") // intersection()
    private Collection<Id> doMultiIndexQuery(IndexQueries queries) {
        Collection<Id> interIds = null;

        for (Map.Entry<IndexLabel, ConditionQuery> entry: queries.entrySet()) {
            Set<Id> ids = this.doIndexQuery(entry.getKey(), entry.getValue());
            if (interIds == null) {
                interIds = ids;
            } else {
                interIds = CollectionUtils.intersection(interIds, ids);
            }
            if (ids.isEmpty() || interIds.isEmpty()) {
                return ImmutableSet.of();
            }
        }
        return interIds;
    }

    private Set<MatchedLabel> collectMatchedLabels(ConditionQuery query) {
        SchemaTransaction schema = this.graph().schemaTransaction();
        Id label = (Id) query.condition(HugeKeys.LABEL);
        // Query-condition has label
        if (label != null) {
            SchemaLabel schemaLabel;
            if (query.resultType().isVertex()) {
                schemaLabel = schema.getVertexLabel(label);
            } else if (query.resultType().isEdge()) {
                schemaLabel = schema.getEdgeLabel(label);
            } else {
                throw new AssertionError(String.format(
                          "Unsupported index query type: %s",
                          query.resultType()));
            }
            MatchedLabel info = this.collectMatchedLabel(schemaLabel, query);
            return info == null ? ImmutableSet.of() : ImmutableSet.of(info);
        }

        // Query-condition doesn't have label
        List<? extends SchemaLabel> schemaLabels;
        if (query.resultType().isVertex()) {
            schemaLabels = schema.getVertexLabels();
        } else if (query.resultType().isEdge()) {
            schemaLabels = schema.getEdgeLabels();
        } else {
            throw new AssertionError(String.format(
                      "Unsupported index query type: %s",
                      query.resultType()));
        }

        Set<MatchedLabel> labels = InsertionOrderUtil.newSet();
        for (SchemaLabel schemaLabel : schemaLabels) {
            MatchedLabel l = this.collectMatchedLabel(schemaLabel, query);
            if (l != null) {
                labels.add(l);
            }
        }
        return labels;
    }

    /**
     * Collect matched IndexLabel(s) in a SchemaLabel for a query
     * @param schemaLabel find indexLabels of this schemaLabel
     * @param query conditions container
     * @return MatchedLabel object contains schemaLabel and matched indexLabels
     */
    private MatchedLabel collectMatchedLabel(SchemaLabel schemaLabel,
                                             ConditionQuery query) {
        SchemaTransaction schema = this.graph().schemaTransaction();
        Set<IndexLabel> indexLabels = schemaLabel.indexLabels().stream()
                                                 .map(schema::getIndexLabel)
                                                 .collect(Collectors.toSet());
        if (indexLabels.isEmpty()) {
            return null;
        }
        // Single or composite index
        Set<IndexLabel> matchedLabels =
                        matchSingleOrCompositeIndex(query, indexLabels);
        if (matchedLabels.isEmpty()) {
            // Joint indexes
            matchedLabels = matchPrefixJointIndexes(query, indexLabels);
        }

        if (!matchedLabels.isEmpty()) {
            return new MatchedLabel(schemaLabel, matchedLabels);
        }
        return null;
    }

    private static Set<IndexLabel> matchSingleOrCompositeIndex(
                                   ConditionQuery query,
                                   Set<IndexLabel> indexLabels) {
        Set<Id> propKeys = query.userpropKeys();
        for (IndexLabel indexLabel : indexLabels) {
            List<Id> indexFields = indexLabel.indexFields();
            if (matchIndexFields(propKeys, indexFields)) {
                if (query.hasRangeCondition() &&
                    indexLabel.indexType() != IndexType.RANGE) {
                    // Range-query can't match secondary index
                    continue;
                }
                return ImmutableSet.of(indexLabel);
            }
        }
        return ImmutableSet.of();
    }

    /**
     * Collect index label(s) whose prefix index fields are contained in
     * property-keys in query
     */
    private static Set<IndexLabel> matchPrefixJointIndexes(
                                   ConditionQuery query,
                                   Set<IndexLabel> indexLabels) {
        Set<Id> propKeys = query.userpropKeys();
        Set<IndexLabel> ils = InsertionOrderUtil.newSet();
        ils.addAll(indexLabels);
        // Handle range index first
        Set<IndexLabel> rangeILs = InsertionOrderUtil.newSet();
        if (query.hasRangeCondition()) {
            rangeILs = matchRangeIndexLabels(query, ils);
            if (rangeILs.isEmpty()) {
                return ImmutableSet.of();
            }
            for (IndexLabel il : rangeILs) {
                propKeys.remove(il.indexField());
            }
            ils.removeAll(rangeILs);
        }
        if (propKeys.isEmpty()) {
            return rangeILs;
        }
        // Handle secondary indexes
        Set<IndexLabel> matchedIndexLabels = InsertionOrderUtil.newSet();
        matchedIndexLabels.addAll(rangeILs);
        Set<Id> indexFields = InsertionOrderUtil.newSet();
        for (IndexLabel indexLabel : ils) {
            List<Id> fields = indexLabel.indexFields();
            for (Id field : fields) {
                if (!propKeys.contains(field)) {
                    break;
                }
                matchedIndexLabels.add(indexLabel);
                indexFields.add(field);
            }
        }
        if(indexFields.equals(propKeys)) {
            return matchedIndexLabels;
        } else {
            return ImmutableSet.of();
        }
    }

    private static Set<IndexLabel> matchRangeIndexLabels(
                                   ConditionQuery query,
                                   Set<IndexLabel> indexLabels) {
        Set<IndexLabel> rangeIL = InsertionOrderUtil.newSet();
        for (Condition.Relation relation : query.userpropRelations()) {
            if (!relation.relation().isRangeType()) {
                continue;
            }
            boolean matched = false;
            Id key = (Id) relation.key();
            for (IndexLabel indexLabel : indexLabels) {
                if (indexLabel.indexType() == IndexType.RANGE &&
                    indexLabel.indexField().equals(key)) {
                    matched = true;
                    rangeIL.add(indexLabel);
                    break;
                }
            }
            if (!matched) {
                return ImmutableSet.of();
            }
        }
        return rangeIL;
    }

    private static IndexQueries buildJointIndexesQueries(ConditionQuery query,
                                                         MatchedLabel info) {
        IndexQueries queries = new IndexQueries();
        List<IndexLabel> allILs = new ArrayList<>(info.indexLabels());

        // Handle range indexes
        if (query.hasRangeCondition()) {
            Set<IndexLabel> rangeILs =
                            matchRangeIndexLabels(query, info.indexLabels());
            assert !rangeILs.isEmpty();

            Set<Id> propKeys = InsertionOrderUtil.newSet();
            for (IndexLabel il : rangeILs) {
                propKeys.add(il.indexField());
            }

            queries.putAll(constructQueries(query, rangeILs, propKeys));

            query = query.copy();
            for (Id field : propKeys) {
                query.unsetCondition(field);
            }
            allILs.removeAll(rangeILs);
        }

        // Range indexes joint satisfies query-conditions already
        if (query.userpropKeys().isEmpty()) {
            return queries;
        }

        // Handle secondary joint indexes
        final ConditionQuery q = query;
        for (int i = 1, size = allILs.size(); i <= size; i++) {
            boolean found = cmn(allILs, size, i, 0, null, r -> {
                // All n indexLabels are selected, test current combination
                IndexQueries qs = constructJointSecondaryQueries(q, r);
                if (qs.isEmpty()) {
                    return false;
                }
                queries.putAll(qs);
                return true;
            });

            if (found) {
                return queries;
            }
        }
        return IndexQueries.EMPTY;
    }

    /**
     * Traverse C(m, n) combinations of a list to find first matched
     * result combination and call back with the result.
     * TODO: move this method to common module.
     * @param all list to contain all items for combination
     * @param m m of C(m, n)
     * @param n n of C(m, n)
     * @param current current position in list
     * @param result list to contains selected items
     * @return true if matched items combination else false
     */
    private static <T> boolean cmn(List<T> all, int m, int n,
                                   int current, List<T> result,
                                   Function<List<T>, Boolean> callback) {
        assert m <= all.size();
        assert n <= m;
        assert current <= all.size();
        if (result == null) {
            result = new ArrayList<>(n);
        }

        if (m == n) {
            result.addAll(all.subList(current, all.size()));
            n = 0;
        }
        if (n == 0) {
            // All n items are selected
            return callback.apply(result);
        }
        if (current >= all.size()) {
            // Reach the end of items
            return false;
        }

        // Select current item, continue to select C(m-1, n-1)
        int index = result.size();
        result.add(all.get(current));
        if (cmn(all, m - 1, n - 1, ++current, result, callback)) {
            return true;
        }
        // Not select current item, continue to select C(m-1, n)
        result.remove(index);
        if (cmn(all, m - 1, n, current, result, callback)) {
            return true;
        }
        return false;
    }

    private static IndexQueries constructJointSecondaryQueries(
                                ConditionQuery query,
                                List<IndexLabel> ils) {
        Set<IndexLabel> indexLabels = InsertionOrderUtil.newSet();
        indexLabels.addAll(ils);
        indexLabels = matchPrefixJointIndexes(query, indexLabels);
        if (indexLabels.isEmpty()) {
            return IndexQueries.EMPTY;
        }

        return constructQueries(query, indexLabels, query.userpropKeys());
    }

    private static IndexQueries constructQueries(ConditionQuery query,
                                                 Set<IndexLabel> ils,
                                                 Set<Id> propKeys) {
        IndexQueries queries = new IndexQueries();

        for (IndexLabel il : ils) {
            List<Id> fields = il.indexFields();
            ConditionQuery newQuery = query.copy();
            newQuery.resetUserpropConditions();
            for (Id field : fields) {
                if (!propKeys.contains(field)) {
                    break;
                }
                for (Condition c : query.userpropConditions(field)) {
                    newQuery.query(c);
                }
            }
            ConditionQuery q = matchIndexLabel(newQuery, il);
            assert q != null;
            queries.put(il, q);
        }
        return queries;
    }

    private static Set<ConditionQuery> query2IndexQuery(ConditionQuery query,
                                                        HugeElement element) {
        Set<ConditionQuery> indexQueries = InsertionOrderUtil.newSet();
        for (IndexLabel indexLabel : relatedIndexLabels(element)) {
            ConditionQuery indexQuery = matchIndexLabel(query, indexLabel);
            if (indexQuery != null) {
                indexQueries.add(indexQuery);
            }
        }
        return indexQueries;
    }

    private static ConditionQuery matchIndexLabel(ConditionQuery query,
                                                  IndexLabel indexLabel) {
        boolean requireRange = query.hasRangeCondition();
        boolean isRange = indexLabel.indexType() == IndexType.RANGE;
        if (requireRange && !isRange) {
            LOG.debug("There is range query condition in '{}'," +
                      "but the index label '{}' is unable to match",
                      query, indexLabel.name());
            return null;
        }

        Set<Id> queryKeys = query.userpropKeys();
        List<Id> indexFields = indexLabel.indexFields();
        if (!matchIndexFields(queryKeys, indexFields)) {
            return null;
        }
        LOG.debug("Matched index fields: {} of index '{}'",
                  indexFields, indexLabel);

        ConditionQuery indexQuery;
        if (indexLabel.indexType() == IndexType.SECONDARY) {
            List<Id> joinedKeys = indexFields.subList(0, queryKeys.size());
            String joinedValues = query.userpropValuesString(joinedKeys);

            // Escape empty String to "\u0000"
            if (joinedValues.isEmpty()) {
                joinedValues = INDEX_EMPTY_SYM;
            }
            indexQuery = new ConditionQuery(HugeType.SECONDARY_INDEX, query);
            indexQuery.eq(HugeKeys.INDEX_LABEL_ID, indexLabel.id());
            indexQuery.eq(HugeKeys.FIELD_VALUES, joinedValues);
        } else {
            assert indexLabel.indexType() == IndexType.RANGE;
            if (query.userpropConditions().size() > 2) {
                throw new BackendException(
                          "Range query has two conditions at most, but got: %s",
                          query.userpropConditions());
            }
            // Replace the query key with PROPERTY_VALUES, and set number value
            indexQuery = new ConditionQuery(HugeType.RANGE_INDEX, query);
            indexQuery.eq(HugeKeys.INDEX_LABEL_ID, indexLabel.id());
            for (Condition condition : query.userpropConditions()) {
                assert condition instanceof Condition.Relation;
                Condition.Relation r = (Condition.Relation) condition;
                Condition.Relation sys = new Condition.SyspropRelation(
                        HugeKeys.FIELD_VALUES,
                        r.relation(),
                        NumericUtil.convertToNumber(r.value()));
                condition = condition.replace(r, sys);
                indexQuery.query(condition);
            }
        }
        return indexQuery;
    }

    private static boolean matchIndexFields(Set<Id> queryKeys,
                                            List<Id> indexFields) {
        if (queryKeys.size() > indexFields.size()) {
            return false;
        }

        // Is queryKeys the prefix of indexFields?
        List<Id> subFields = indexFields.subList(0, queryKeys.size());
        return subFields.containsAll(queryKeys);
    }

    private static boolean validQueryConditionValues(HugeGraph graph,
                                                     ConditionQuery query) {
        Set<Id> keys = query.userpropKeys();
        for (Id key : keys) {
            PropertyKey pk = graph.propertyKey(key);
            Set<Object> values = query.userpropValue(key);
            E.checkState(!values.isEmpty(),
                         "Expect user property values for key '%s', " +
                         "but got none", pk);
            for (Object value : values) {
                if (!pk.checkValue(value)) {
                    return false;
                }
            }
        }
        return true;
    }

    private static BackendException noIndexException(HugeGraph graph,
                                                     ConditionQuery query,
                                                     String label) {
        return new BackendException("Don't accept query based on properties " +
                                    "%s that are not indexed in label '%s'",
                                    graph.mapPkId2Name(query.userpropKeys()),
                                    label);
    }

    private static boolean hasNullableProp(HugeElement element, Id key) {
        return element.schemaLabel().nullableKeys().contains(key);
    }

    private static Set<IndexLabel> relatedIndexLabels(HugeElement element) {
        Set<IndexLabel> indexLabels = InsertionOrderUtil.newSet();
        Set<Id> indexLabelIds = element.schemaLabel().indexLabels();

        for (Id id : indexLabelIds) {
            SchemaTransaction schema = element.graph().schemaTransaction();
            IndexLabel indexLabel = schema.getIndexLabel(id);
            indexLabels.add(indexLabel);
        }
        return indexLabels;
    }

    private static Set<Id> limit(Set<Id> ids, Query query) {
        long fromIndex = query.offset();
        E.checkArgument(fromIndex <= Integer.MAX_VALUE,
                        "Offset must be <= 0x7fffffff, but got '%s'",
                        fromIndex);

        if (query.offset() >= ids.size()) {
            return ImmutableSet.of();
        }
        if (query.limit() == Query.NO_LIMIT && query.offset() == 0) {
            return ids;
        }
        long toIndex = query.offset() + query.limit();
        if (query.limit() == Query.NO_LIMIT || toIndex > ids.size()) {
            toIndex = ids.size();
        }
        assert fromIndex < ids.size();
        assert toIndex <= ids.size();
        return CollectionUtil.subSet(ids, (int) fromIndex, (int) toIndex);
    }

    public void removeIndex(IndexLabel indexLabel) {
        HugeIndex index = new HugeIndex(indexLabel);
        this.doRemove(this.serializer.writeIndex(index));
    }

    public void removeIndex(Collection<Id> indexLabelIds) {
        SchemaTransaction schema = graph().schemaTransaction();
        for (Id id : indexLabelIds) {
            IndexLabel indexLabel = schema.getIndexLabel(id);
            if (indexLabel == null) {
                /*
                 * TODO: How to deal with non-existent index name:
                 * continue or throw exception?
                 */
                continue;
            }
            this.removeIndex(indexLabel);
        }
    }

    public void rebuildIndex(SchemaElement schema) {
        switch (schema.type()) {
            case INDEX_LABEL:
                IndexLabel indexLabel = (IndexLabel) schema;
                this.rebuildIndex(indexLabel.baseType(),
                                  indexLabel.baseValue(),
                                  ImmutableSet.of(indexLabel.id()));
                break;
            case VERTEX_LABEL:
                VertexLabel vertexLabel = (VertexLabel) schema;
                this.rebuildIndex(vertexLabel.type(),
                                  vertexLabel.id(),
                                  vertexLabel.indexLabels());
                break;
            case EDGE_LABEL:
                EdgeLabel edgeLabel = (EdgeLabel) schema;
                this.rebuildIndex(edgeLabel.type(),
                                  edgeLabel.id(),
                                  edgeLabel.indexLabels());
                break;
            default:
                assert schema.type() == HugeType.PROPERTY_KEY;
                throw new AssertionError(String.format(
                          "The %s can't rebuild index.", schema.type()));
        }
    }

    public void rebuildIndex(HugeType type, Id label,
                             Collection<Id> indexLabelIds) {
        GraphTransaction tx = graph().graphTransaction();
        // Manually commit avoid deletion override add/update
        boolean autoCommit = this.autoCommit();
        this.autoCommit(false);

        LockUtil.Locks locks = new LockUtil.Locks();
        try {
            locks.lockWrites(LockUtil.INDEX_REBUILD, indexLabelIds);
            locks.lockWrites(LockUtil.INDEX_LABEL, indexLabelIds);
            this.removeIndex(indexLabelIds);
            /**
             * Note: Here must commit index transaction firstly.
             * Because remove index convert to (id like <?>:personByCity):
             * `delete from index table where label = ?`,
             * But append index will convert to (id like Beijing:personByCity):
             * `update index element_ids += xxx where field_value = ?
             * and index_label_name = ?`,
             * They have different id lead to it can't compare and optimize
             */
            this.commit();
            if (type == HugeType.VERTEX_LABEL) {
                ConditionQuery query = new ConditionQuery(HugeType.VERTEX);
                query.eq(HugeKeys.LABEL, label);
                query.capacity(Query.NO_CAPACITY);
                for (Iterator<Vertex> itor = tx.queryVertices(query);
                     itor.hasNext();) {
                    HugeVertex vertex = (HugeVertex) itor.next();
                    for (Id id : indexLabelIds) {
                        this.updateIndex(id, vertex, false);
                    }
                }
            } else {
                assert type == HugeType.EDGE_LABEL;
                ConditionQuery query = new ConditionQuery(HugeType.EDGE);
                query.eq(HugeKeys.LABEL, label);
                query.capacity(Query.NO_CAPACITY);
                for (Iterator<Edge> itor = tx.queryEdges(query);
                     itor.hasNext();) {
                    HugeEdge edge = (HugeEdge) itor.next();
                    for (Id id : indexLabelIds) {
                        this.updateIndex(id, edge, false);
                    }
                }
            }
            this.commit();
        } finally {
            this.autoCommit(autoCommit);
            locks.unlock();
        }
    }

    private static class MatchedLabel {

        private SchemaLabel schemaLabel;
        private Set<IndexLabel> indexLabels;

        public MatchedLabel(SchemaLabel schemaLabel,
                            Set<IndexLabel> indexLabels) {
            this.schemaLabel = schemaLabel;
            this.indexLabels = indexLabels;
        }

        protected SchemaLabel schemaLabel() {
            return this.schemaLabel;
        }

        protected Set<IndexLabel> indexLabels() {
            return Collections.unmodifiableSet(this.indexLabels);
        }

        public IndexQueries constructQueries(ConditionQuery query) {
            // Condition query => Index Queries
            if (this.indexLabels().size() == 1) {
                // Single index or composite index
                IndexLabel il = this.indexLabels().iterator().next();
                ConditionQuery indexQuery = matchIndexLabel(query, il);
                assert indexQuery != null;
                /*
                 * Set limit for single index or composite index
                 * to avoid redundant element ids
                 */
                indexQuery.limit(query.limit());
                IndexQueries indexQueries = new IndexQueries();
                indexQueries.put(il, indexQuery);
                return indexQueries;
            } else {
                // Joint indexes
                IndexQueries indexQueries = buildJointIndexesQueries(query,
                                                                     this);
                assert !indexQueries.isEmpty();
                return indexQueries;
            }
        }
    }

    private static class IndexQueries
                   extends HashMap<IndexLabel, ConditionQuery> {

        public static final IndexQueries EMPTY = new IndexQueries();
    }

    public static enum OptimizedType {
        NONE,
        PRIMARY_KEY,
        SORT_KEY,
        INDEX
    }
}
