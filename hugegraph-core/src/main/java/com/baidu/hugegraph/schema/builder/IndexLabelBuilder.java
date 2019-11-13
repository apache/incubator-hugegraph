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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.function.BiPredicate;

import com.baidu.hugegraph.HugeException;
import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.id.IdGenerator;
import com.baidu.hugegraph.backend.tx.SchemaTransaction;
import com.baidu.hugegraph.config.CoreOptions;
import com.baidu.hugegraph.exception.ExistedException;
import com.baidu.hugegraph.exception.NotAllowException;
import com.baidu.hugegraph.exception.NotFoundException;
import com.baidu.hugegraph.schema.IndexLabel;
import com.baidu.hugegraph.schema.PropertyKey;
import com.baidu.hugegraph.schema.SchemaElement;
import com.baidu.hugegraph.schema.SchemaLabel;
import com.baidu.hugegraph.schema.Userdata;
import com.baidu.hugegraph.schema.VertexLabel;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.define.Action;
import com.baidu.hugegraph.type.define.Cardinality;
import com.baidu.hugegraph.type.define.DataType;
import com.baidu.hugegraph.type.define.IndexType;
import com.baidu.hugegraph.type.define.SchemaStatus;
import com.baidu.hugegraph.util.CollectionUtil;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.InsertionOrderUtil;

public class IndexLabelBuilder implements IndexLabel.Builder {

    private Id id;
    private String name;
    private HugeType baseType;
    private String baseValue;
    private IndexType indexType;
    private List<String> indexFields;
    private Userdata userdata;
    private boolean checkExist;

    private SchemaTransaction transaction;

    public IndexLabelBuilder(String name, SchemaTransaction transaction) {
        E.checkNotNull(name, "name");
        E.checkNotNull(transaction, "transaction");
        this.id = null;
        this.name = name;
        this.baseType = null;
        this.baseValue = null;
        this.indexType = null;
        this.indexFields = new ArrayList<>();
        this.userdata = new Userdata();
        this.checkExist = true;
        this.transaction = transaction;
    }

    @Override
    public IndexLabel build() {
        Id id = this.transaction.validOrGenerateId(HugeType.INDEX_LABEL,
                                                   this.id, this.name);
        this.checkBaseType();
        this.checkIndexType();

        HugeGraph graph = this.transaction.graph();
        IndexLabel indexLabel = new IndexLabel(graph, id, this.name);
        indexLabel.baseType(this.baseType);
        SchemaLabel schemaLabel = this.loadElement();
        indexLabel.baseValue(schemaLabel.id());
        indexLabel.indexType(this.indexType);
        for (String field : this.indexFields) {
            PropertyKey propertyKey = this.transaction.getPropertyKey(field);
            indexLabel.indexField(propertyKey.id());
        }
        indexLabel.userdata(this.userdata);
        return indexLabel;
    }

    /**
     * Create index label with async mode
     */
    @Override
    public IndexLabel.CreatedIndexLabel createWithTask() {
        HugeType type = HugeType.INDEX_LABEL;
        SchemaTransaction tx = this.transaction;
        SchemaElement.checkName(this.name, tx.graph().configuration());
        IndexLabel indexLabel = tx.getIndexLabel(this.name);
        if (indexLabel != null) {
            if (this.checkExist) {
                throw new ExistedException(type, this.name);
            }
            return new IndexLabel.CreatedIndexLabel(indexLabel, null);
        }
        tx.checkIdIfRestoringMode(type, this.id);

        this.checkBaseType();
        this.checkIndexType();

        SchemaLabel schemaLabel = this.loadElement();

        /*
         * If new index label is prefix of existed index label, or has
         * the same fields, fail to create new index label.
         */
        this.checkFields(schemaLabel.properties());
        this.checkRepeatIndex(schemaLabel);
        this.checkUserdata(Action.INSERT);

        // Async delete index label which is prefix of the new index label
        // TODO: use event to replace direct call
        Set<Id> removeTasks = this.removeSubIndex(schemaLabel);

        // Create index label (just schema)
        indexLabel = this.build();
        indexLabel.status(SchemaStatus.CREATING);
        tx.addIndexLabel(schemaLabel, indexLabel);

        // Async rebuild index
        Id rebuildTask = tx.rebuildIndex(indexLabel, removeTasks);
        E.checkNotNull(rebuildTask, "rebuild-index task");

        return new IndexLabel.CreatedIndexLabel(indexLabel, rebuildTask);
    }

    /**
     * Create index label with sync mode
     */
    @Override
    public IndexLabel create() {
        // Create index label async
        IndexLabel.CreatedIndexLabel createdIndexLabel = this.createWithTask();

        Id task = createdIndexLabel.task();
        if (task == null) {
            // Task id will be null if creating index label already exists.
            return createdIndexLabel.indexLabel();
        }

        // Wait task completed (change to sync mode)
        HugeGraph graph = this.transaction.graph();
        long timeout = graph.configuration().get(CoreOptions.TASK_WAIT_TIMEOUT);
        try {
            graph.taskScheduler().waitUntilTaskCompleted(task, timeout);
        } catch (TimeoutException e) {
            throw new HugeException(
                      "Failed to wait index-creating task completed", e);
        }

        // Return index label without task-info
        return createdIndexLabel.indexLabel();
    }

    @Override
    public IndexLabel append() {
        IndexLabel indexLabel = this.transaction.getIndexLabel(this.name);
        if (indexLabel == null) {
            throw new NotFoundException("Can't update index label '%s' " +
                                        "since it doesn't exist", this.name);
        }
        this.checkStableVars();
        this.checkUserdata(Action.APPEND);

        SchemaLabel schemaLabel = this.loadElement(indexLabel.baseType(),
                                                   indexLabel.baseValue());
        indexLabel.userdata(this.userdata);
        this.transaction.addIndexLabel(schemaLabel, indexLabel);
        return indexLabel;
    }

    @Override
    public IndexLabel eliminate() {
        IndexLabel indexLabel = this.transaction.getIndexLabel(this.name);
        if (indexLabel == null) {
            throw new NotFoundException("Can't update index label '%s' " +
                                        "since it doesn't exist", this.name);
        }
        this.checkStableVars();
        this.checkUserdata(Action.ELIMINATE);

        SchemaLabel schemaLabel = this.loadElement(indexLabel.baseType(),
                                                   indexLabel.baseValue());
        indexLabel.removeUserdata(this.userdata);
        this.transaction.addIndexLabel(schemaLabel, indexLabel);
        return indexLabel;
    }

    @Override
    public Id remove() {
        IndexLabel indexLabel = this.transaction.getIndexLabel(this.name);
        if (indexLabel == null) {
            return null;
        }
        return this.transaction.removeIndexLabel(indexLabel.id());
    }

    @Override
    public Id rebuild() {
        IndexLabel indexLabel = this.transaction.graph().indexLabel(this.name);
        if (indexLabel == null) {
            return null;
        }
        return this.transaction.rebuildIndex(indexLabel);
    }

    @Override
    public IndexLabelBuilder id(long id) {
        E.checkArgument(id != 0L,
                        "Not allowed to assign 0 as index label id");
        this.id = IdGenerator.of(id);
        return this;
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
    public IndexLabelBuilder shard() {
        this.indexType = IndexType.SHARD;
        return this;
    }

    @Override
    public IndexLabelBuilder unique() {
        this.indexType = IndexType.UNIQUE;
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
    public IndexLabel.Builder userdata(String key, Object value) {
        this.userdata.put(key, value);
        return this;
    }

    @Override
    public IndexLabel.Builder userdata(Map<String, Object> userdata) {
        this.userdata.putAll(userdata);
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

    private void checkBaseType() {
        if (this.baseType == null) {
            this.baseType = HugeType.SYS_SCHEMA;
        }
    }

    private void checkIndexType() {
        if (this.indexType == null) {
            this.indexType = IndexType.SECONDARY;
        }
    }

    private SchemaLabel loadElement() {
        return this.loadElement(this.baseType, this.baseValue);
    }

    private SchemaLabel loadElement(HugeType baseType, Object baseValue) {
        E.checkNotNull(baseType, "base type", "index label");
        E.checkNotNull(baseValue, "base value", "index label");
        E.checkArgument(baseValue instanceof String || baseValue instanceof Id,
                        "The base value must be instance of String or Id, " +
                        "but got %s(%s)", baseValue,
                        baseValue.getClass().getSimpleName());

        SchemaLabel label;
        switch (baseType) {
            case VERTEX_LABEL:
                if (baseValue instanceof String) {
                    label = this.transaction.getVertexLabel((String) baseValue);
                } else {
                    assert baseValue instanceof Id;
                    label = this.transaction.getVertexLabel((Id) baseValue);
                }
                break;
            case EDGE_LABEL:
                if (baseValue instanceof String) {
                    label = this.transaction.getEdgeLabel((String) baseValue);
                } else {
                    assert baseValue instanceof Id;
                    label = this.transaction.getEdgeLabel((Id) baseValue);
                }
                break;
            default:
                throw new AssertionError(String.format(
                          "Unsupported base type '%s' of index label '%s'",
                          baseType, this.name));
        }

        E.checkArgumentNotNull(label, "Can't find the %s with name '%s'",
                               baseType.readableName(), baseValue);
        return label;
    }

    private void checkFields(Set<Id> propertyIds) {
        List<String> fields = this.indexFields;
        E.checkNotEmpty(fields, "index fields", this.name);

        for (String field : fields) {
            PropertyKey pkey = this.transaction.getPropertyKey(field);
            // In general this will not happen
            E.checkArgument(pkey != null,
                            "Can't build index on undefined property key " +
                            "'%s' for '%s': '%s'", field,
                            this.baseType.readableName(), this.baseValue);
            E.checkArgument(pkey.aggregateType().isIndexable(),
                            "The aggregate type %s is not indexable",
                            pkey.aggregateType());
            E.checkArgument(pkey.cardinality() == Cardinality.SINGLE,
                            "Not allowed to build index on property key " +
                            "'%s' whose cardinality is list or set",
                            pkey.name());
        }

        List<String> properties = this.transaction.graph()
                                      .mapPkId2Name(propertyIds);
        E.checkArgument(properties.containsAll(fields),
                        "Not all index fields '%s' are contained in " +
                        "schema properties '%s'", fields, properties);

        // Range index must build on single numeric column
        if (this.indexType == IndexType.RANGE) {
            E.checkArgument(fields.size() == 1,
                            "Range index can only build on " +
                            "one field, but got %s fields: '%s'",
                            fields.size(), fields);
            String field = fields.iterator().next();
            DataType dataType = this.transaction.getPropertyKey(field)
                                                .dataType();
            E.checkArgument(dataType.isNumber() || dataType.isDate(),
                            "Range index can only build on numeric or " +
                            "date property, but got %s(%s)", dataType, field);
            switch (dataType) {
                case BYTE:
                case INT:
                    this.indexType = IndexType.RANGE_INT;
                    break;
                case FLOAT:
                    this.indexType = IndexType.RANGE_FLOAT;
                    break;
                case LONG:
                case DATE:
                    this.indexType = IndexType.RANGE_LONG;
                    break;
                case DOUBLE:
                    this.indexType = IndexType.RANGE_DOUBLE;
                    break;
                default:
                    throw new AssertionError("Invalid datatype: " + dataType);
            }
        }

        // Search index must build on single text column
        if (this.indexType.isSearch()) {
            E.checkArgument(fields.size() == 1,
                            "Search index can only build on " +
                            "one field, but got %s fields: '%s'",
                            fields.size(), fields);
            String field = fields.iterator().next();
            DataType dataType = this.transaction.getPropertyKey(field)
                                                .dataType();
            E.checkArgument(dataType.isText(),
                            "Search index can only build on text property, " +
                            "but got %s(%s)", dataType, field);
        }
    }

    private void checkRepeatIndex(SchemaLabel schemaLabel) {
        this.checkPrimaryKeyIndex(schemaLabel);
        switch (this.indexType) {
            case RANGE_INT:
            case RANGE_FLOAT:
            case RANGE_LONG:
            case RANGE_DOUBLE:
                this.checkRepeatRangeIndex(schemaLabel);
                break;
            case SEARCH:
                this.checkRepeatSearchIndex(schemaLabel);
                break;
            case SECONDARY:
                this.checkRepeatSecondaryIndex(schemaLabel);
                break;
            case SHARD:
                this.checkRepeatShardIndex(schemaLabel);
                break;
            case UNIQUE:
                this.checkRepeatUniqueIndex(schemaLabel);
                break;
            default:
                throw new AssertionError(String.format(
                          "Unsupported index type: %s", this.indexType));
        }
    }

    private Set<Id> removeSubIndex(SchemaLabel schemaLabel) {
        Set<Id> overrideIndexLabelIds = InsertionOrderUtil.newSet();
        for (Id id : schemaLabel.indexLabels()) {
            IndexLabel old = this.transaction.getIndexLabel(id);
            if (!this.hasSubIndex(old)) {
                continue;
            }
            List<String> oldFields = this.transaction.graph()
                                         .mapPkId2Name(old.indexFields());
            List<String> newFields = this.indexFields;
            /*
             * Remove the existed index label if:
             * 1. new unique index label is subset of existed unique index label
             * or
             * 2. existed index label is prefix of new created index label
             * (except for unique index)
             */
            if (this.indexType.isUniuqe() && oldFields.containsAll(newFields) ||
                !this.indexType.isUniuqe() &&
                CollectionUtil.prefixOf(oldFields, newFields)) {
                overrideIndexLabelIds.add(id);
            }
        }
        Set<Id> tasks = InsertionOrderUtil.newSet();
        for (Id id : overrideIndexLabelIds) {
            schemaLabel.removeIndexLabel(id);
            Id task = this.transaction.removeIndexLabel(id);
            E.checkNotNull(task, "remove sub index label task");
            tasks.add(task);
        }
        return tasks;
    }

    private void checkPrimaryKeyIndex(SchemaLabel schemaLabel) {
        if (schemaLabel instanceof VertexLabel) {
            VertexLabel vl = (VertexLabel) schemaLabel;
            if (vl.idStrategy().isPrimaryKey()) {
                if (this.indexType.isSecondary() ||
                    this.indexType.isUniuqe() ||
                    this.indexType.isShard() &&
                    this.allStringIndex(this.indexFields)) {
                    List<String> pks = this.transaction.graph()
                                           .mapPkId2Name(vl.primaryKeys());
                    E.checkArgument(!this.indexFields.containsAll(pks),
                                    "No need to build index on properties " +
                                    "%s, because they contains all primary " +
                                    "keys %s for vertex label '%s'",
                                    this.indexFields, pks, vl.name());
                }
            }
        }
    }

    private void checkRepeatRangeIndex(SchemaLabel schemaLabel) {
        this.checkRepeatIndex(schemaLabel, IndexType.RANGE_INT,
                              IndexType.RANGE_FLOAT, IndexType.RANGE_LONG,
                              IndexType.RANGE_DOUBLE);
    }

    private void checkRepeatSearchIndex(SchemaLabel schemaLabel) {
        this.checkRepeatIndex(schemaLabel, IndexType.SEARCH);
    }

    private void checkRepeatSecondaryIndex(SchemaLabel schemaLabel) {
        this.checkRepeatIndex(schemaLabel, IndexType.RANGE_INT,
                              IndexType.RANGE_FLOAT, IndexType.RANGE_LONG,
                              IndexType.RANGE_DOUBLE, IndexType.SECONDARY,
                              IndexType.SHARD);
    }

    private void checkRepeatShardIndex(SchemaLabel schemaLabel) {
        if (this.oneNumericField()) {
            checkRepeatIndex(schemaLabel, IndexType.RANGE_INT,
                             IndexType.RANGE_FLOAT, IndexType.RANGE_LONG,
                             IndexType.RANGE_DOUBLE, IndexType.SHARD);
        } else if (this.allStringIndex(this.indexFields)) {
            this.checkRepeatIndex(schemaLabel, IndexType.SECONDARY,
                                  IndexType.SHARD);
        } else {
            this.checkRepeatIndex(schemaLabel, IndexType.SHARD);
        }
    }

    private void checkRepeatUniqueIndex(SchemaLabel schemaLabel) {
        this.checkRepeatIndex(schemaLabel, List::containsAll, IndexType.UNIQUE);
    }

    private void checkRepeatIndex(SchemaLabel schemaLabel,
                                  IndexType... checkedTypes) {
        this.checkRepeatIndex(schemaLabel, CollectionUtil::prefixOf,
                              checkedTypes);
    }

    private void checkRepeatIndex(SchemaLabel schemaLabel,
                                  BiPredicate<List<String>, List<String>> check,
                                  IndexType... checkedTypes) {
        for (Id id : schemaLabel.indexLabels()) {
            IndexLabel old = this.transaction.getIndexLabel(id);
            if (!Arrays.asList(checkedTypes).contains(old.indexType())) {
                continue;
            }
            List<String> newFields = this.indexFields;
            List<String> oldFields = this.transaction.graph()
                                         .mapPkId2Name(old.indexFields());
            E.checkArgument(!check.test(newFields, oldFields),
                            "Repeated new index label %s(%s) with fields %s " +
                            "due to existed index label %s(%s) with fields %s",
                            this.name, this.indexType, newFields,
                            old.name(), old.indexType(), old.indexFields());
        }
    }

    private boolean hasSubIndex(IndexLabel indexLabel) {
        return (this.indexType == indexLabel.indexType()) ||
               (this.indexType.isShard() &&
                indexLabel.indexType().isSecondary()) ||
               (this.indexType.isSecondary() &&
                indexLabel.indexType().isShard() &&
                this.allStringIndex(indexLabel.indexFields())) ||
               (this.indexType.isRange() &&
                (indexLabel.indexType().isSecondary() ||
                 indexLabel.indexType().isShard()));
    }

    private boolean allStringIndex(List<?> fields) {
        for (Object field : fields) {
            PropertyKey pk = field instanceof Id ?
                             this.transaction.getPropertyKey((Id) field) :
                             this.transaction.getPropertyKey((String) field);
            DataType dataType = pk.dataType();
            if (dataType.isNumber() || dataType.isDate()) {
                return false;
            }
        }
        return true;
    }

    private boolean oneNumericField() {
        if (this.indexFields.size() != 1) {
            return false;
        }
        String field = this.indexFields.get(0);
        PropertyKey propertyKey = this.transaction.getPropertyKey(field);
        DataType dataType = propertyKey.dataType();
        return dataType.isNumber() || dataType.isDate();
    }

    private void checkStableVars() {
        if (this.baseType != null) {
            throw new NotAllowException("Not allowed to update base type " +
                                        "for index label '%s'", this.name);
        }
        if (this.baseValue != null) {
            throw new NotAllowException("Not allowed to update base value " +
                                        "for index label '%s'", this.name);
        }
        if (this.indexType != null) {
            throw new NotAllowException("Not allowed to update index type " +
                                        "for index label '%s'", this.name);
        }
        if (this.indexFields != null && !this.indexFields.isEmpty()) {
            throw new NotAllowException("Not allowed to update index fields " +
                                        "for index label '%s'", this.name);
        }
    }

    private void checkUserdata(Action action) {
        switch (action) {
            case INSERT:
            case APPEND:
                for (Map.Entry<String, Object> e : this.userdata.entrySet()) {
                    if (e.getValue() == null) {
                        throw new NotAllowException(
                                  "Not allowed pass null userdata value when " +
                                  "create or append index label");
                    }
                }
                break;
            case ELIMINATE:
            case DELETE:
                // pass
                break;
            default:
                throw new AssertionError(String.format(
                          "Unknown schema action '%s'", action));
        }
    }
}
