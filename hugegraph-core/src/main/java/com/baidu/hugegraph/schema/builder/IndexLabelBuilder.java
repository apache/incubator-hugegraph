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

package com.baidu.hugegraph.schema.builder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.query.ConditionQuery;
import com.baidu.hugegraph.backend.tx.SchemaTransaction;
import com.baidu.hugegraph.exception.ExistedException;
import com.baidu.hugegraph.exception.NotSupportException;
import com.baidu.hugegraph.schema.EdgeLabel;
import com.baidu.hugegraph.schema.IndexLabel;
import com.baidu.hugegraph.schema.PropertyKey;
import com.baidu.hugegraph.schema.SchemaElement;
import com.baidu.hugegraph.schema.SchemaLabel;
import com.baidu.hugegraph.schema.VertexLabel;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.define.Cardinality;
import com.baidu.hugegraph.type.define.HugeKeys;
import com.baidu.hugegraph.type.define.IndexType;
import com.baidu.hugegraph.util.CollectionUtil;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.NumericUtil;

public class IndexLabelBuilder implements IndexLabel.Builder {

    private String name;
    private HugeType baseType;
    private String baseValue;
    private IndexType indexType;
    private List<String> indexFields;
    private boolean checkExist;

    private SchemaTransaction transaction;

    public IndexLabelBuilder(String name, SchemaTransaction transaction) {
        E.checkNotNull(name, "name");
        E.checkNotNull(transaction, "transaction");
        this.name = name;
        this.indexType = IndexType.SECONDARY;
        this.indexFields = new ArrayList<>();
        this.checkExist = true;
        this.transaction = transaction;
    }

    @Override
    public IndexLabel build() {
        Id id = this.transaction.getNextId(HugeType.INDEX_LABEL);
        IndexLabel indexLabel = new IndexLabel(id, this.name);
        indexLabel.baseType(this.baseType);
        SchemaLabel schemaLabel = this.loadElement();
        indexLabel.baseValue(schemaLabel.id());
        indexLabel.indexType(this.indexType);
        for (String field : this.indexFields) {
            PropertyKey propertyKey = this.transaction.getPropertyKey(field);
            indexLabel.indexField(propertyKey.id());
        }
        return indexLabel;
    }

    @Override
    public IndexLabel create() {
        SchemaElement.checkName(this.name,
                                this.transaction.graph().configuration());
        IndexLabel indexLabel = this.transaction.getIndexLabel(this.name);
        if (indexLabel != null) {
            if (this.checkExist) {
                throw new ExistedException("index label", this.name);
            }
            return indexLabel;
        }

        SchemaLabel schemaLabel = this.loadElement();
        /*
         * If new index label is prefix of existed index label, or has
         * the same fields, fail to create new index label.
         */
        this.checkFields(schemaLabel.properties());
        this.checkRepeatIndex(schemaLabel);

        indexLabel = this.build();
        this.transaction.addIndexLabel(indexLabel);
        // Delete index label which is prefix of the new index label
        // TODO: use event to replace direct call
        this.removeSubIndex(schemaLabel);
        // TODO: should wrap update and add operation in one transaction.
        this.updateSchemaIndexName(schemaLabel, indexLabel);

        // TODO: use event to replace direct call
        this.rebuildIndexIfNeeded(indexLabel);

        return indexLabel;
    }

    @Override
    public IndexLabel append() {
        throw new NotSupportException("action append on index label");
    }

    @Override
    public IndexLabel eliminate() {
        throw new NotSupportException("action eliminate on index label");
    }

    @Override
    public void remove() {
        IndexLabel indexLabel = this.transaction.getIndexLabel(this.name);
        if (indexLabel == null) {
            return;
        }
        this.transaction.removeIndexLabel(indexLabel.id());
    }

    @Override
    public void rebuild() {
        IndexLabel indexLabel = this.transaction.graph().indexLabel(this.name);
        this.transaction.rebuildIndex(indexLabel);
    }

    @Override
    public IndexLabelBuilder onV(String baseValue) {
        this.baseType = HugeType.VERTEX_LABEL;
        this.baseValue = baseValue;
        return this;
    }

    @Override
    public IndexLabelBuilder onE(String baseValue) {
        this.baseType = HugeType.EDGE_LABEL;
        this.baseValue = baseValue;
        return this;
    }

    @Override
    public IndexLabelBuilder by(String... fields) {
        E.checkArgument(fields.length > 0, "Empty index fields");
        E.checkArgument(this.indexFields.isEmpty(),
                        "Not allowed to assign index fields multitimes");

        List<String> indexFields = Arrays.asList(fields);
        E.checkArgument(CollectionUtil.allUnique(indexFields),
                        "Invalid index fields %s, which contains some " +
                        "duplicate properties", indexFields);
        this.indexFields.addAll(indexFields);
        return this;
    }

    @Override
    public IndexLabelBuilder secondary() {
        this.indexType = IndexType.SECONDARY;
        return this;
    }

    @Override
    public IndexLabelBuilder range() {
        this.indexType = IndexType.RANGE;
        return this;
    }

    @Override
    public IndexLabelBuilder search() {
        this.indexType = IndexType.SEARCH;
        return this;
    }

    @Override
    public IndexLabelBuilder on(HugeType baseType, String baseValue) {
        E.checkArgument(baseType == HugeType.VERTEX_LABEL ||
                        baseType == HugeType.EDGE_LABEL,
                        "The base type of index label '%s' can only be " +
                        "either VERTEX_LABEL or EDGE_LABEL", this.name);
        if (baseType == HugeType.VERTEX_LABEL) {
            this.onV(baseValue);
        } else {
            assert baseType == HugeType.EDGE_LABEL;
            this.onE(baseValue);
        }
        return this;
    }

    @Override
    public IndexLabelBuilder indexType(IndexType indexType) {
        this.indexType = indexType;
        return this;
    }

    @Override
    public IndexLabelBuilder ifNotExist() {
        this.checkExist = false;
        return this;
    }

    @Override
    public IndexLabelBuilder checkExist(boolean checkExist) {
        this.checkExist = checkExist;
        return this;
    }

    private SchemaLabel loadElement() {
        E.checkNotNull(this.baseType, "base type", "index label");
        E.checkNotNull(this.baseValue, "base value", "index label");

        SchemaLabel schemaLabel;
        switch (this.baseType) {
            case VERTEX_LABEL:
                schemaLabel = this.transaction.getVertexLabel(this.baseValue);
                break;
            case EDGE_LABEL:
                schemaLabel = this.transaction.getEdgeLabel(this.baseValue);
                break;
            default:
                throw new AssertionError(String.format(
                          "Unsupported base type '%s' of index label '%s'",
                          this.baseType, this.name));
        }

        E.checkArgumentNotNull(schemaLabel, "Can't find the %s with name '%s'",
                               this.baseType, this.baseValue);
        return schemaLabel;
    }

    private void checkFields(Set<Id> propertyIds) {
        List<String> fields = this.indexFields;
        E.checkNotEmpty(fields, "index fields", this.name);

        for (String field : fields) {
            PropertyKey pkey = this.transaction.getPropertyKey(field);
            // In general this will not happen
            E.checkArgumentNotNull(pkey, "Can't build index on undefined " +
                                   "property key '%s' for '%s': '%s'",
                                   field, this.baseType, this.baseValue);
            E.checkArgument(pkey.cardinality() == Cardinality.SINGLE,
                            "Not allowed to build index on property key " +
                            "'%s' whose cardinality is list or set",
                            pkey.name());
        }

        List<String> properties = this.transaction.graph()
                                      .mapPkId2Name(propertyIds);
        E.checkArgument(CollectionUtil.containsAll(properties, fields),
                        "Not all index fields '%s' are contained in " +
                        "schema properties '%s'", fields, properties);

        // Range index must build on single numeric column
        if (this.indexType == IndexType.RANGE) {
            E.checkArgument(fields.size() == 1,
                            "Range index can only build on " +
                            "one property, but got %s properties: '%s'",
                            fields.size(), fields);
            String field = fields.iterator().next();
            PropertyKey pk = this.transaction.getPropertyKey(field);
            E.checkArgument(NumericUtil.isNumber(pk.dataType().clazz()),
                            "Range index can only build on " +
                            "numeric property, but got %s(%s)",
                            pk.dataType(), pk.name());
        }
    }

    private void updateSchemaIndexName(SchemaLabel schemaLabel,
                                       IndexLabel indexLabel) {
        schemaLabel.indexLabel(indexLabel.id());
        switch (this.baseType) {
            case VERTEX_LABEL:
                this.transaction.addVertexLabel((VertexLabel) schemaLabel);
                break;
            case EDGE_LABEL:
                this.transaction.addEdgeLabel((EdgeLabel) schemaLabel);
                break;
            default:
                throw new AssertionError(String.format(
                          "Can't update index name of schema type: %s",
                          this.baseType));
        }
    }

    private void checkRepeatIndex(SchemaLabel schemaLabel) {
        for (Id id : schemaLabel.indexLabels()) {
            IndexLabel old = this.transaction.getIndexLabel(id);
            if (this.indexType != old.indexType()) {
                continue;
            }
            List<String> newFields = this.indexFields;
            List<String> oldFields = this.transaction.graph()
                                         .mapPkId2Name(old.indexFields());
            // New created label can't be prefix of existed label
            E.checkArgument(!CollectionUtil.prefixOf(newFields, oldFields),
                            "Fields %s of new index label '%s' is prefix of " +
                            "existed index label '%s'",
                            newFields, oldFields, old.name());
        }
    }

    private void removeSubIndex(SchemaLabel schemaLabel) {
        HashSet<Id> overrideIndexLabelIds = new HashSet<>();
        for (Id id : schemaLabel.indexLabels()) {
            IndexLabel old = this.transaction.getIndexLabel(id);
            if (this.indexType != old.indexType()) {
                continue;
            }
            /*
             * If existed label is prefix of new created label,
             * remove the existed label.
             */
            List<String> oldFields = this.transaction.graph()
                                         .mapPkId2Name(old.indexFields());
            List<String> newFields = this.indexFields;
            if (CollectionUtil.prefixOf(oldFields, newFields)) {
                overrideIndexLabelIds.add(id);
            }
        }
        for (Id id : overrideIndexLabelIds) {
            schemaLabel.removeIndexLabel(id);
            this.transaction.removeIndexLabel(id);
        }
    }

    private void rebuildIndexIfNeeded(IndexLabel indexLabel) {
        if (this.baseType == HugeType.VERTEX_LABEL) {
            ConditionQuery query = new ConditionQuery(HugeType.VERTEX);
            query.eq(HugeKeys.LABEL, indexLabel.baseValue());
            query.limit(1L);
            if (this.transaction.graph().graphTransaction()
                    .queryVertices(query).iterator().hasNext()) {
                this.transaction.rebuildIndex(indexLabel);
            }
        } else {
            assert this.baseType == HugeType.EDGE_LABEL;
            ConditionQuery query = new ConditionQuery(HugeType.EDGE);
            query.eq(HugeKeys.LABEL, indexLabel.baseValue());
            query.limit(1L);
            if (this.transaction.graph().graphTransaction()
                    .queryEdges(query).iterator().hasNext()) {
                this.transaction.rebuildIndex(indexLabel);
            }
        }
    }
}
