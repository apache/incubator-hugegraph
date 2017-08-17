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
package com.baidu.hugegraph.backend.tx;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
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
import com.baidu.hugegraph.schema.EdgeLabel;
import com.baidu.hugegraph.schema.IndexLabel;
import com.baidu.hugegraph.schema.SchemaElement;
import com.baidu.hugegraph.schema.SchemaLabel;
import com.baidu.hugegraph.schema.VertexLabel;
import com.baidu.hugegraph.structure.HugeEdge;
import com.baidu.hugegraph.structure.HugeElement;
import com.baidu.hugegraph.structure.HugeIndex;
import com.baidu.hugegraph.structure.HugeProperty;
import com.baidu.hugegraph.structure.HugeVertex;
import com.baidu.hugegraph.type.ExtendableIterator;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.define.HugeKeys;
import com.baidu.hugegraph.type.define.IndexType;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.LockUtil;
import com.baidu.hugegraph.util.NumericUtil;

public class IndexTransaction extends AbstractTransaction {

    public IndexTransaction(HugeGraph graph, BackendStore store) {
        super(graph, store);
    }

    public void updateVertexIndex(HugeVertex vertex, boolean removed) {
        // Update index(only property, no edge) of a vertex
        for (String indexName : vertex.vertexLabel().indexNames()) {
            updateIndex(indexName, vertex, removed);
        }
    }

    public void updateEdgeIndex(HugeEdge edge, boolean removed) {
        // Update index of an edge
        for (String indexName : edge.edgeLabel().indexNames()) {
            updateIndex(indexName, edge, removed);
        }
    }

    protected void updateIndex(String indexName,
                               HugeElement element,
                               boolean removed) {
        SchemaTransaction schema = graph().schemaTransaction();
        IndexLabel indexLabel = schema.getIndexLabel(indexName);
        E.checkArgumentNotNull(indexLabel,
                               "Not exist index label: '%s'", indexName);

        List<Object> propValues = new ArrayList<>();
        for (String field : indexLabel.indexFields()) {
            HugeProperty<Object> property = element.getProperty(field);
            E.checkState(property != null,
                         "Not exist property '%s' in %s '%s'",
                         field, element.type(), element.id());
            propValues.add(property.value());
        }

        for (int i = 0, n = propValues.size(); i < n; i++) {
            List<Object> subPropValues = propValues.subList(0, i + 1);

            Object propValue = null;
            if (indexLabel.indexType() == IndexType.SECONDARY) {
                propValue = SplicingIdGenerator.concatValues(subPropValues);
            } else {
                assert indexLabel.indexType() == IndexType.SEARCH;
                E.checkState(subPropValues.size() == 1,
                             "Expect searching by only one property");
                propValue = NumericUtil.convert2Number(subPropValues.get(0));
            }

            HugeIndex index = new HugeIndex(indexLabel);
            index.fieldValues(propValue);
            index.elementIds(element.id());

            if (!removed) {
                this.appendEntry(this.serializer.writeIndex(index));
            } else {
                this.eliminateEntry(this.serializer.writeIndex(index));
            }
        }
    }

    public Query query(ConditionQuery query) {

        SchemaTransaction schema = graph().schemaTransaction();

        List<IndexLabel> indexLabels = schema.getIndexLabels();
        // Get user applied label or collect all qualified labels
        Set<String> labels = collectQueryLabels(query, indexLabels);

        if (labels.isEmpty()) {
            throw new HugeException("Don't accept query based on properties: " +
                                    "'%s' that are not indexed",
                                    query.userpropKeys());
        }

        ExtendableIterator<BackendEntry> entries = new ExtendableIterator<>();
        for (String label : labels) {
            LockUtil.Locks locks = new LockUtil.Locks();
            try {
                locks.lockReads(LockUtil.INDEX_LABEL, label);
                locks.lockReads(LockUtil.INDEX_REBUILD, label);
                // Condition => Entry
                ConditionQuery indexQuery = this.constructIndexQuery(query, label);
                entries.extend(super.query(indexQuery).iterator());
            } finally {
                locks.unlock();
            }
        }

        // Entry => Id
        Set<Id> ids = new LinkedHashSet<>();
        while (entries.hasNext()) {
            HugeIndex index = this.serializer.readIndex(entries.next());
            ids.addAll(index.elementIds());
        }
        return new IdQuery(query.resultType(), ids);
    }

    private Set<String> collectQueryLabels(ConditionQuery query,
                                           List<IndexLabel> indexLabels) {
        Set<String> labels = new HashSet<>();

        String label = (String) query.condition(HugeKeys.LABEL);
        if (label != null) {
            labels.add(label);
        } else {
            Set<String> queryKeys = query.userpropKeys();
            for (IndexLabel indexLabel : indexLabels) {
                List<String> indexFields = indexLabel.indexFields();
                if (query.resultType() == indexLabel.queryType() &&
                    matchIndexFields(queryKeys, indexFields)) {

                    labels.add(indexLabel.baseValue());
                }
            }
        }
        return labels;
    }

    protected ConditionQuery constructIndexQuery(ConditionQuery query,
                                                 String label) {
        ConditionQuery indexQuery = null;
        SchemaLabel schemaLabel;

        SchemaTransaction schema = graph().schemaTransaction();
        switch (query.resultType()) {
            case VERTEX:
                schemaLabel = schema.getVertexLabel(label);
                break;
            case EDGE:
                schemaLabel = schema.getEdgeLabel(label);
                break;
            default:
                throw new AssertionError(String.format(
                          "Unsupported index query: %s", query.resultType()));
        }

        E.checkArgumentNotNull(schemaLabel, "Invalid label: '%s'", label);

        Set<String> indexNames = schemaLabel.indexNames();
        LOG.debug("The label '{}' with index names: {}", label, indexNames);
        for (String name : indexNames) {
            IndexLabel indexLabel = schema.getIndexLabel(name);
            indexQuery = matchIndexLabel(indexLabel, query);
            if (indexQuery != null) {
                break;
            }
        }

        if (indexQuery == null) {
            throw new BackendException("No matching index for query: " + query);
        }
        return indexQuery;
    }

    private static ConditionQuery matchIndexLabel(IndexLabel indexLabel,
                                                  ConditionQuery query) {
        ConditionQuery indexQuery;

        boolean requireSearch = query.hasSearchCondition();
        boolean searching = indexLabel.indexType() == IndexType.SEARCH;
        if (requireSearch && !searching) {
            LOG.debug("There is search condition in '{}'," +
                      "but the index label '{}' is unable to search",
                      query, indexLabel.name());
            return null;
        }

        Set<String> queryKeys = query.userpropKeys();
        List<String> indexFields = indexLabel.indexFields();

        if (!matchIndexFields(queryKeys, indexFields)) {
            return null;
        }
        LOG.debug("Matched index fields: {} of index '{}'",
                  indexFields, indexLabel.name());

        if (indexLabel.indexType() == IndexType.SECONDARY) {
            List<String> joinedKeys = indexFields.subList(0, queryKeys.size());
            String joinedValues = query.userpropValuesString(joinedKeys);

            indexQuery = new ConditionQuery(HugeType.SECONDARY_INDEX);
            indexQuery.eq(HugeKeys.INDEX_LABEL_NAME, indexLabel.name());
            indexQuery.eq(HugeKeys.FIELD_VALUES, joinedValues);
        } else {
            assert indexLabel.indexType() == IndexType.SEARCH;
            if (query.userpropConditions().size() != 1) {
                throw new BackendException(
                          "Only support searching by one field");
            }
            // Replace the query key with PROPERTY_VALUES, and set number value
            Condition condition = query.userpropConditions().get(0).copy();
            for (Condition.Relation r : condition.relations()) {
                Condition.Relation sys = new Condition.SyspropRelation(
                        HugeKeys.FIELD_VALUES,
                        r.relation(),
                        NumericUtil.convert2Number(r.value()));
                condition = condition.replace(r, sys);
            }

            indexQuery = new ConditionQuery(HugeType.SEARCH_INDEX);
            indexQuery.eq(HugeKeys.INDEX_LABEL_NAME, indexLabel.name());
            indexQuery.query(condition);
        }
        return indexQuery;
    }

    private static boolean matchIndexFields(Set<String> queryKeys,
                                            List<String> indexFields) {
        if (queryKeys.size() > indexFields.size()) {
            return false;
        }

        // Is queryKeys the prefix of indexFields?
        List<String> subFields = indexFields.subList(0, queryKeys.size());
        if (!subFields.containsAll(queryKeys)) {
            return false;
        }
        return true;
    }

    public void removeIndex(String indexName) {
        SchemaTransaction schema = graph().schemaTransaction();
        IndexLabel indexLabel = schema.getIndexLabel(indexName);
        E.checkArgumentNotNull(indexLabel,
                               "Not exist index label: '%s'", indexName);
        HugeIndex index = new HugeIndex(indexLabel);
        this.removeEntry(this.serializer.writeIndex(index));
    }

    public void removeIndex(Collection<String> indexNames) {
        for (String index : indexNames) {
            removeIndex(index);
        }
    }

    public void rebuildIndex(SchemaElement schemaElement) {
        switch (schemaElement.type()) {
            case INDEX_LABEL:
                IndexLabel indexLabel = (IndexLabel) schemaElement;
                this.rebuildIndex(indexLabel.baseType(),
                                  indexLabel.baseValue(),
                                  ImmutableSet.of(indexLabel.name()));
                break;
            case VERTEX_LABEL:
                VertexLabel vertexLabel = (VertexLabel) schemaElement;
                this.rebuildIndex(vertexLabel.type(),
                                  vertexLabel.name(),
                                  vertexLabel.indexNames());
                break;
            case EDGE_LABEL:
                EdgeLabel edgeLabel = (EdgeLabel) schemaElement;
                this.rebuildIndex(edgeLabel.type(),
                                  edgeLabel.name(),
                                  edgeLabel.indexNames());
                break;
            default:
                assert schemaElement.type() == HugeType.PROPERTY_KEY;
                throw new AssertionError(String.format(
                          "The %s can't rebuild index.", schemaElement.type()));
        }
    }

    public void rebuildIndex(HugeType type,
                             String label,
                             Collection<String> indexNames) {
        GraphTransaction graphTransaction = graph().graphTransaction();
        // Manually commit avoid deletion override add/update
        boolean autoCommit = this.autoCommit();
        this.autoCommit(false);

        LockUtil.Locks locks = new LockUtil.Locks();
        try {
            locks.lockWrites(LockUtil.INDEX_REBUILD, indexNames);
            locks.lockWrites(LockUtil.INDEX_LABEL, indexNames);
            this.removeIndex(indexNames);
            this.commit();
            if (type == HugeType.VERTEX_LABEL) {
                ConditionQuery query = new ConditionQuery(HugeType.VERTEX);
                query.eq(HugeKeys.LABEL, label);
                for (Vertex vertex : graphTransaction.queryVertices(query)) {
                    for (String indexName : indexNames) {
                        updateIndex(indexName, (HugeVertex) vertex, false);
                    }
                }
                this.commit();
            } else {
                assert type == HugeType.EDGE_LABEL;
                ConditionQuery query = new ConditionQuery(HugeType.EDGE);
                query.eq(HugeKeys.LABEL, label);
                for (Edge edge : graphTransaction.queryEdges(query)) {
                    for (String indexName : indexNames) {
                        updateIndex(indexName, (HugeEdge) edge, false);
                    }
                }
                this.commit();
            }
        } finally {
            this.autoCommit(autoCommit);
            locks.unlock();
        }
    }
}
