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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import com.baidu.hugegraph.HugeException;
import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.analyzer.Analyzer;
import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.id.SplicingIdGenerator;
import com.baidu.hugegraph.backend.query.Condition;
import com.baidu.hugegraph.backend.query.Condition.Relation;
import com.baidu.hugegraph.backend.query.ConditionQuery;
import com.baidu.hugegraph.backend.query.ConditionQueryFlatten;
import com.baidu.hugegraph.backend.query.IdQuery;
import com.baidu.hugegraph.backend.query.Query;
import com.baidu.hugegraph.backend.store.BackendEntry;
import com.baidu.hugegraph.backend.store.BackendStore;
import com.baidu.hugegraph.exception.NoIndexException;
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
import com.baidu.hugegraph.type.define.SchemaStatus;
import com.baidu.hugegraph.util.CollectionUtil;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.InsertionOrderUtil;
import com.baidu.hugegraph.util.LockUtil;
import com.baidu.hugegraph.util.NumericUtil;
import com.google.common.collect.ImmutableSet;

public class GraphIndexTransaction extends AbstractTransaction {

    private static final String INDEX_EMPTY_SYM = "\u0000";
    private static final Query EMPTY_QUERY = new ConditionQuery(null);

    private final Analyzer textAnalyzer;

    public GraphIndexTransaction(HugeGraph graph, BackendStore store) {
        super(graph, store);

        this.textAnalyzer = graph.analyzer();
        assert this.textAnalyzer != null;
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
        // Process secondary index or search index
        this.processSecondaryOrSearchIndexLeft(query, element);

        this.commit();
    }

    private void processRangeIndexLeft(ConditionQuery query,
                                       HugeElement element) {
        // Construct index ConditionQuery
        Set<MatchedLabel> labels = collectMatchedLabels(query);
        IndexQueries queries = null;
        for (MatchedLabel label : labels) {
            if (label.schemaLabel().id().equals(element.schemaLabel().id())) {
                queries = label.constructIndexQueries(query);
                break;
            }
        }
        if (queries == null) {
            throw new HugeException(
                      "Can't construct index query for '%s'", query);
        }

        for (ConditionQuery q : queries.values()) {
            if (!q.resultType().isRangeIndex()) {
                continue;
            }
            // Query and delete index equals element id
            for (Iterator<BackendEntry> it = super.query(q); it.hasNext();) {
                BackendEntry entry = it.next();
                HugeIndex index = this.serializer.readIndex(graph(), q, entry);
                if (index.elementIds().contains(element.id())) {
                    index.resetElementIds();
                    index.elementIds(element.id());
                    this.doEliminate(this.serializer.writeIndex(index));
                }
            }
        }
    }

    private void processSecondaryOrSearchIndexLeft(ConditionQuery query,
                                                   HugeElement element) {
        HugeElement deletion = element.copyAsFresh();
        Set<Id> propKeys = query.userpropKeys();
        Set<Id> incorrectPKs = InsertionOrderUtil.newSet();
        for (Id key : propKeys) {
            Set<Object> conditionValues = query.userpropValues(key);
            E.checkState(!conditionValues.isEmpty(),
                         "Expect user property values for key '%s', " +
                         "but got none", key);
            if (conditionValues.size() > 1) {
                // It's inside/between Query (processed in range index)
                return;
            }
            Object propValue = deletion.getProperty(key).value();
            Object conditionValue = conditionValues.iterator().next();
            if (!propValue.equals(conditionValue)) {
                PropertyKey pkey = this.graph().propertyKey(key);
                deletion.addProperty(pkey, conditionValue);
                incorrectPKs.add(key);
            }
        }

        // Delete unused index
        for (IndexLabel il : relatedIndexLabels(deletion)) {
            if (!CollectionUtil.hasIntersection(il.indexFields(), incorrectPKs)) {
                continue;
            }
            // Skip if search index is not wrong
            if (il.indexType() == IndexType.SEARCH) {
                Id field = il.indexField();
                String cond = deletion.<String>getPropertyValue(field);
                String actual = element.<String>getPropertyValue(field);
                if (this.matchSearchIndexWords(actual, cond)) {
                    /*
                     * If query by two search index, one is correct but
                     * the other is wrong, we should not delete the correct
                     */
                    continue;
                }
            }
            // Delete index with error property
            this.updateIndex(il.id(), deletion, true);
            // Rebuild index if delete correct index part
            if (il.indexType() == IndexType.SECONDARY) {
                /*
                 * When it's a composite secondary index,
                 * if the suffix property is wrong and the prefix property
                 * is correct, the correct prefix part will be deleted,
                 * so rebuild the index again with the correct property.
                 */
                this.updateIndex(il.id(), element, false);
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
                        "Not exist index label with id '%s'", ilId);

        // Collect property values of index fields
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
        if (propValues.isEmpty()) {
            // The property value of first index field is null
            return;
        }

        // Update index for each index type
        switch (indexLabel.indexType()) {
            case RANGE:
                E.checkState(propValues.size() == 1,
                             "Expect only one property in range index");
                Object value = NumericUtil.convertToNumber(propValues.get(0));
                this.updateIndex(indexLabel, value, element.id(), removed);
                break;
            case SEARCH:
                E.checkState(propValues.size() == 1,
                             "Expect only one property in search index");
                value = propValues.get(0);
                Set<String> words = this.segmentWords(value.toString());
                for (String word : words) {
                    this.updateIndex(indexLabel, word, element.id(), removed);
                }
                break;
            case SECONDARY:
                // Secondary index maybe include multi prefix index
                for (int i = 0, n = propValues.size(); i < n; i++) {
                    List<Object> prefixValues = propValues.subList(0, i + 1);
                    value = SplicingIdGenerator.concatValues(prefixValues);

                    // Use `\u0000` as escape for empty String and treat it as
                    // illegal value for text property
                    E.checkArgument(!value.equals(INDEX_EMPTY_SYM),
                                    "Illegal value of index property: '%s'",
                                    INDEX_EMPTY_SYM);
                    if (((String) value).isEmpty()) {
                        value = INDEX_EMPTY_SYM;
                    }

                    this.updateIndex(indexLabel, value, element.id(), removed);
                }
                break;
            default:
                throw new AssertionError(String.format(
                          "Unknown index type '%s'", indexLabel.indexType()));
        }
    }

    private void updateIndex(IndexLabel indexLabel, Object propValue,
                             Id elementId, boolean removed) {
        HugeIndex index = new HugeIndex(indexLabel);
        index.fieldValues(propValue);
        index.elementIds(elementId);

        if (removed) {
            this.doEliminate(this.serializer.writeIndex(index));
        } else {
            this.doAppend(this.serializer.writeIndex(index));
        }
    }

    /**
     * Composite index, an index involving multiple columns.
     * Single index, an index involving only one column.
     * Joint indexes, join of single indexes, composite indexes or mixed
     * of single indexes and composite indexes.
     * @param query original condition query
     * @return      converted id query
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
        HugeType queryType = query.resultType();
        IndexLabel il = IndexLabel.label(queryType);
        Id label = (Id) query.condition(HugeKeys.LABEL);
        assert label != null;

        SchemaLabel schemaLabel;
        if (queryType.isVertex()) {
            schemaLabel = this.graph().vertexLabel(label);
        } else if (queryType.isEdge()) {
            schemaLabel = this.graph().edgeLabel(label);
        } else {
            throw new BackendException("Can't query %s by label", queryType);
        }

        if (!this.store().features().supportsQueryByLabel() &&
            !schemaLabel.enableLabelIndex()) {
            throw new NoIndexException("Don't accept query by label '%s', " +
                                       "it disables label index", schemaLabel);
        }

        ConditionQuery indexQuery;
        indexQuery = new ConditionQuery(HugeType.SECONDARY_INDEX, query);
        indexQuery.eq(HugeKeys.INDEX_LABEL_ID, il.id());
        indexQuery.eq(HugeKeys.FIELD_VALUES, label);
        // Set offset and limit to avoid redundant element ids
        indexQuery.limit(query.limit());
        indexQuery.offset(query.offset());
        indexQuery.capacity(query.capacity());

        return this.doIndexQuery(il, indexQuery);
    }

    @Watched(prefix = "index")
    private Set<Id> queryByUserprop(ConditionQuery query) {
        // Get user applied label or collect all qualified labels with
        // related index labels
        Set<MatchedLabel> labels = this.collectMatchedLabels(query);
        if (labels.isEmpty()) {
            Id label = (Id) query.condition(HugeKeys.LABEL);
            throw noIndexException(this.graph(), query, label);
        }

        // Value type of Condition not matched
        if (!validQueryConditionValues(this.graph(), query)) {
            return ImmutableSet.of();
        }

        // Do index query
        Set<Id> ids = InsertionOrderUtil.newSet();
        for (MatchedLabel label : labels) {
            if (label.containsSearchIndex()) {
                // Do search-index query
                ids.addAll(this.queryByUserpropWithSearchIndex(query, label));
            } else {
                // Do secondary-index or range-index query
                IndexQueries queries = label.constructIndexQueries(query);
                ids.addAll(this.doMultiIndexQuery(queries));
            }

            if (query.reachLimit(ids.size())) {
                break;
            }
        }
        return limit(ids, query);
    }

    @Watched(prefix = "index")
    private Set<Id> queryByUserpropWithSearchIndex(ConditionQuery query,
                                                   MatchedLabel label) {
        ConditionQuery originQuery = query;
        Set<Id> indexFields = new HashSet<>();
        // Convert has(key, text) to has(key, textContainsAny(word1, word2))
        for (IndexLabel il : label.indexLabels()) {
            if (il.indexType() != IndexType.SEARCH) {
                continue;
            }
            Id indexField = il.indexField();
            Object fieldValue = query.userpropValue(indexField);
            Set<String> words = this.segmentWords(fieldValue.toString());
            indexFields.add(indexField);

            query = query.copy();
            query.unsetCondition(indexField);
            query.query(Condition.textContainsAny(indexField, words));
        }

        // Register results filter
        query.registerResultsFilter(elem -> {
            for (Condition cond : originQuery.conditions()) {
                Object key = cond.isRelation() ? ((Relation) cond).key() : null;
                if (key instanceof Id && indexFields.contains(key)) {
                    // This is an index field of search index
                    Id field = (Id) key;
                    String propValue = elem.<String>getPropertyValue(field);
                    String fvalue = originQuery.userpropValue(field).toString();
                    if (this.matchSearchIndexWords(propValue, fvalue)) {
                        continue;
                    }
                    return false;
                }
                if (!cond.test(elem)) {
                    return false;
                }
            }
            return true;
        });

        // Do query
        Set<Id> ids = InsertionOrderUtil.newSet();
        for (ConditionQuery q : ConditionQueryFlatten.flatten(query)) {
            IndexQueries queries = label.constructIndexQueries(q);
            ids.addAll(this.doMultiIndexQuery(queries));
        }
        return ids;
    }

    private boolean matchSearchIndexWords(String propValue, String fieldValue) {
        Set<String> propValues = this.segmentWords(propValue);
        Set<String> words = this.segmentWords(fieldValue);
        return CollectionUtil.hasIntersection(propValues, words);
    }

    private Set<String> segmentWords(String text) {
        return this.textAnalyzer.segment(text);
    }

    private boolean needIndexForLabel() {
        return !this.store().features().supportsQueryByLabel();
    }

    private Set<Id> doIndexQuery(IndexLabel indexLabel, ConditionQuery query) {
        Set<Id> ids = InsertionOrderUtil.newSet();
        LockUtil.Locks locks = new LockUtil.Locks();
        try {
            locks.lockReads(LockUtil.INDEX_LABEL_DELETE, indexLabel.id());
            locks.lockReads(LockUtil.INDEX_LABEL_REBUILD, indexLabel.id());

            Iterator<BackendEntry> entries = super.query(query);
            while(entries.hasNext()) {
                HugeIndex index = this.serializer.readIndex(graph(), query,
                                                            entries.next());
                ids.addAll(index.elementIds());
                if (query.reachLimit(ids.size())) {
                    break;
                }
            }
        } finally {
            locks.unlock();
        }
        return ids;
    }

    private Collection<Id> doMultiIndexQuery(IndexQueries queries) {
        Collection<Id> interIds = null;

        for (Map.Entry<IndexLabel, ConditionQuery> entry: queries.entrySet()) {
            Set<Id> ids = this.doIndexQuery(entry.getKey(), entry.getValue());
            if (interIds == null) {
                interIds = ids;
            } else {
                interIds = CollectionUtil.intersectWithModify(interIds, ids);
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
        Set<IndexLabel> matchedIndexLabels =
                        matchSingleOrCompositeIndex(query, indexLabels);
        if (matchedIndexLabels.isEmpty()) {
            // Joint indexes
            matchedIndexLabels = matchJointIndexes(query, indexLabels);
        }

        if (!matchedIndexLabels.isEmpty()) {
            return new MatchedLabel(schemaLabel, matchedIndexLabels);
        }
        return null;
    }

    private static Set<IndexLabel> matchSingleOrCompositeIndex(
                                   ConditionQuery query,
                                   Set<IndexLabel> indexLabels) {
        boolean reqiureRange = query.hasRangeCondition();
        boolean reqiureSearch = query.hasSearchCondition();
        Set<Id> queryPropKeys = query.userpropKeys();
        for (IndexLabel indexLabel : indexLabels) {
            List<Id> indexFields = indexLabel.indexFields();
            // Try to match fields
            if (!matchIndexFields(queryPropKeys, indexFields)) {
                continue;
            }
            /*
             * Matched all fields, try to match index type.
             * For range-index or search-index there must be only one condition.
             * The following terms are legal:
             *   1.hasSearchCondition and IndexType.SEARCH
             *   2.hasRangeCondition and IndexType.RANGE
             *   3.not hasRangeCondition but has range-index equal-condition
             *   4.secondary (composite) index
             */
            IndexType indexType = indexLabel.indexType();
            if ((reqiureSearch && indexType != IndexType.SEARCH) ||
                (!reqiureSearch && indexType == IndexType.SEARCH)) {
                continue;
            }
            if (reqiureRange && indexType != IndexType.RANGE) {
                continue;
            }
            return ImmutableSet.of(indexLabel);
        }
        return ImmutableSet.of();
    }

    /**
     * Collect index label(s) whose prefix index fields are contained in
     * property-keys in query
     */
    private static Set<IndexLabel> matchJointIndexes(
                                   ConditionQuery query,
                                   Set<IndexLabel> indexLabels) {
        Set<Id> queryPropKeys = query.userpropKeys();
        assert !queryPropKeys.isEmpty();
        Set<IndexLabel> allILs = InsertionOrderUtil.newSet(indexLabels);

        // Handle range/search index first
        Set<IndexLabel> matchedIndexLabels = InsertionOrderUtil.newSet();
        if (query.hasRangeCondition() || query.hasSearchCondition()) {
            matchedIndexLabels = matchRangeOrSearchIndexLabels(query, allILs);
            if (matchedIndexLabels.isEmpty()) {
                return ImmutableSet.of();
            }
            allILs.removeAll(matchedIndexLabels);

            // Remove matched queryPropKeys
            for (IndexLabel il : matchedIndexLabels) {
                // Only one field each range/search index-label
                queryPropKeys.remove(il.indexField());
            }
            // Return if all fields are matched
            if (queryPropKeys.isEmpty()) {
                return matchedIndexLabels;
            }
        }

        // Handle secondary indexes
        Set<Id> indexFields = InsertionOrderUtil.newSet();
        for (IndexLabel indexLabel : allILs) {
            // Range index equal-condition and secondary index can joint
            if (indexLabel.indexType() == IndexType.SEARCH) {
                // Search index must be handled at the previous step
                continue;
            }

            List<Id> fields = indexLabel.indexFields();
            // Collect all fields prefix
            for (Id field : fields) {
                if (!queryPropKeys.contains(field)) {
                    break;
                }
                matchedIndexLabels.add(indexLabel);
                indexFields.add(field);
            }
        }
        // Must match all fields
        if(indexFields.equals(queryPropKeys)) {
            return matchedIndexLabels;
        } else {
            return ImmutableSet.of();
        }
    }

    private static Set<IndexLabel> matchRangeOrSearchIndexLabels(
                                   ConditionQuery query,
                                   Set<IndexLabel> indexLabels) {
        Set<IndexLabel> matchedIndexLabels = InsertionOrderUtil.newSet();
        for (Condition.Relation relation : query.userpropRelations()) {
            if (!relation.relation().isRangeType() &&
                !relation.relation().isSearchType()) {
                continue;
            }
            Id key = (Id) relation.key();
            boolean matched = false;
            for (IndexLabel indexLabel : indexLabels) {
                if (indexLabel.indexType() == IndexType.RANGE ||
                    indexLabel.indexType() == IndexType.SEARCH) {
                    if (indexLabel.indexField().equals(key)) {
                        matched = true;
                        matchedIndexLabels.add(indexLabel);
                        break;
                    }
                }
            }
            if (!matched) {
                return ImmutableSet.of();
            }
        }
        return matchedIndexLabels;
    }

    private static IndexQueries buildJointIndexesQueries(ConditionQuery query,
                                                         MatchedLabel info) {
        IndexQueries queries = new IndexQueries();
        List<IndexLabel> allILs = new ArrayList<>(info.indexLabels());

        // Handle range/search indexes
        if (query.hasRangeCondition() || query.hasSearchCondition()) {
            Set<IndexLabel> matchedILs =
                    matchRangeOrSearchIndexLabels(query, info.indexLabels());
            assert !matchedILs.isEmpty();
            allILs.removeAll(matchedILs);

            Set<Id> queryPropKeys = InsertionOrderUtil.newSet();
            for (IndexLabel il : matchedILs) {
                // Only one field each range/search index-label
                queryPropKeys.add(il.indexField());
            }

            // Construct queries by matched index-labels
            queries.putAll(constructQueries(query, matchedILs, queryPropKeys));

            // Remove matched queryPropKeys
            query = query.copy();
            for (Id field : queryPropKeys) {
                query.unsetCondition(field);
            }
            // Return if matched indexes satisfies query-conditions already
            if (query.userpropKeys().isEmpty()) {
                return queries;
            }
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
        indexLabels = matchJointIndexes(query, indexLabels);
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

    private static ConditionQuery matchIndexLabel(ConditionQuery query,
                                                  IndexLabel indexLabel) {
        IndexType indexType = indexLabel.indexType();
        boolean requireRange = query.hasRangeCondition();
        boolean isRange = indexType == IndexType.RANGE;
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

        switch (indexType) {
            case SEARCH:
                E.checkState(indexFields.size() == 1,
                             "Invalid index fields size for %s: %s",
                             indexType, indexFields);
                Object fieldValue = query.userpropValue(indexFields.get(0));
                // Query search index from SECONDARY_INDEX table
                indexQuery = new ConditionQuery(indexType.type(), query);
                indexQuery.eq(HugeKeys.INDEX_LABEL_ID, indexLabel.id());
                indexQuery.eq(HugeKeys.FIELD_VALUES, fieldValue);
                break;
            case SECONDARY:
                List<Id> joinedKeys = indexFields.subList(0, queryKeys.size());
                String joinedValues = query.userpropValuesString(joinedKeys);

                // Escape empty String to '\u0000'
                if (joinedValues.isEmpty()) {
                    joinedValues = INDEX_EMPTY_SYM;
                }
                indexQuery = new ConditionQuery(indexType.type(), query);
                indexQuery.eq(HugeKeys.INDEX_LABEL_ID, indexLabel.id());
                indexQuery.eq(HugeKeys.FIELD_VALUES, joinedValues);
                break;
            case RANGE:
                if (query.userpropConditions().size() > 2) {
                    throw new BackendException(
                              "Range query has two conditions at most, " +
                              "but got: %s", query.userpropConditions());
                }
                // Replace the query key with PROPERTY_VALUES, set number value
                indexQuery = new ConditionQuery(indexType.type(), query);
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
                break;
            default:
                throw new AssertionError(String.format(
                          "Unknown index type '%s'", indexType));
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
            Set<Object> values = query.userpropValues(key);
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

    private static NoIndexException noIndexException(HugeGraph graph,
                                                     ConditionQuery query,
                                                     Id label) {
        String name = label == null ? "any label" : String.format("label '%s'",
                      query.resultType().isVertex() ?
                      graph.vertexLabel(label).name() :
                      graph.edgeLabel(label).name());
        List<String> mismatched = new ArrayList<>();
        if (query.hasSecondaryCondition()) {
            mismatched.add("secondary");
        }
        if (query.hasRangeCondition()) {
            mismatched.add("range");
        }
        if (query.hasSearchCondition()) {
            mismatched.add("search");
        }
        return new NoIndexException("Don't accept query based on properties " +
                                    "%s that are not indexed in %s, " +
                                    "may not match %s condition",
                                    graph.mapPkId2Name(query.userpropKeys()),
                                    name, String.join("/", mismatched));
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
        GraphTransaction graphTx = graph().graphTransaction();
        SchemaTransaction schemaTx = graph().schemaTransaction();
        // Manually commit avoid deletion override add/update
        boolean autoCommit = this.autoCommit();
        this.autoCommit(false);

        LockUtil.Locks locks = new LockUtil.Locks();
        try {
            locks.lockWrites(LockUtil.INDEX_LABEL_REBUILD, indexLabelIds);
            locks.lockWrites(LockUtil.INDEX_LABEL_DELETE, indexLabelIds);

            Set<IndexLabel> ils = indexLabelIds.stream()
                                               .map(schemaTx::getIndexLabel)
                                               .collect(Collectors.toSet());
            for (IndexLabel il : ils) {
                if (il.status() == SchemaStatus.CREATING) {
                    continue;
                }
                schemaTx.updateSchemaStatus(il, SchemaStatus.REBUILDING);
            }

            this.removeIndex(indexLabelIds);
            /*
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
                for (Iterator<Vertex> itor = graphTx.queryVertices(query);
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
                for (Iterator<Edge> itor = graphTx.queryEdges(query);
                     itor.hasNext();) {
                    HugeEdge edge = (HugeEdge) itor.next();
                    for (Id id : indexLabelIds) {
                        this.updateIndex(id, edge, false);
                    }
                }
            }
            this.commit();

            for (IndexLabel il : ils) {
                schemaTx.updateSchemaStatus(il, SchemaStatus.CREATED);
            }
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

        public SchemaLabel schemaLabel() {
            return this.schemaLabel;
        }

        public Set<IndexLabel> indexLabels() {
            return Collections.unmodifiableSet(this.indexLabels);
        }

        public IndexQueries constructIndexQueries(ConditionQuery query) {
            // Condition query => Index Queries
            if (this.indexLabels().size() == 1) {
                // Single index or composite index

                IndexLabel il = this.indexLabels().iterator().next();
                ConditionQuery indexQuery = matchIndexLabel(query, il);
                assert indexQuery != null;

                /*
                 * Set limit for single index or composite index
                 * to avoid redundant element ids.
                 * Not set offset because this query might be a sub-query,
                 * see queryByUserprop()
                 */
                indexQuery.limit(query.total());

                return IndexQueries.of(il, indexQuery);
            } else {
                // Joint indexes
                IndexQueries queries = buildJointIndexesQueries(query, this);
                assert !queries.isEmpty();
                return queries;
            }
        }

        public boolean containsSearchIndex() {
            for (IndexLabel il : this.indexLabels) {
                if (il.indexType() == IndexType.SEARCH) {
                    return true;
                }
            }
            return false;
        }
    }

    private static class IndexQueries
                   extends HashMap<IndexLabel, ConditionQuery> {

        private static final long serialVersionUID = 1400326138090922676L;

        public static final IndexQueries EMPTY = new IndexQueries();

        public static IndexQueries of(IndexLabel il, ConditionQuery query) {
            IndexQueries indexQueries = new IndexQueries();
            indexQueries.put(il, query);
            return indexQueries;
        }
    }

    public static enum OptimizedType {
        NONE,
        PRIMARY_KEY,
        SORT_KEY,
        INDEX
    }
}
