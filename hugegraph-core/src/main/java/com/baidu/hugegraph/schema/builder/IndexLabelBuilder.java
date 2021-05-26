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
import com.baidu.hugegraph.schema.SchemaLabel;
import com.baidu.hugegraph.schema.Userdata;
import com.baidu.hugegraph.schema.VertexLabel;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.define.Action;
import com.baidu.hugegraph.type.define.DataType;
import com.baidu.hugegraph.type.define.IndexType;
import com.baidu.hugegraph.type.define.SchemaStatus;
import com.baidu.hugegraph.util.CollectionUtil;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.InsertionOrderUtil;

public class IndexLabelBuilder extends AbstractBuilder
                               implements IndexLabel.Builder {

    private Id id;
    private String name;
    private HugeType baseType;
    private String baseValue;
    private IndexType indexType;
    private List<String> indexFields;
    private Userdata userdata;
    private boolean checkExist;
    private boolean rebuild;

    public IndexLabelBuilder(SchemaTransaction transaction,
                             HugeGraph graph, String name) {
        super(transaction, graph);
        E.checkNotNull(name, "name");
        this.id = null;
        this.name = name;
        this.baseType = null;
        this.baseValue = null;
        this.indexType = null;
        this.indexFields = new ArrayList<>();
        this.userdata = new Userdata();
        this.checkExist = true;
        this.rebuild = true;
    }

    public IndexLabelBuilder(SchemaTransaction transaction,
                             HugeGraph graph, IndexLabel copy) {
        super(transaction, graph);
        E.checkNotNull(copy, "copy");
        // Get base element from self graph
        SchemaLabel schemaLabel = IndexLabel.getElement(graph, copy.baseType(),
                                                        copy.baseValue());
        this.id = null;
        this.name = copy.name();
        this.baseType = copy.baseType();
        this.baseValue = schemaLabel.name();
        this.indexType = copy.indexType();
        this.indexFields = copy.graph().mapPkId2Name(copy.indexFields());
        this.userdata = new Userdata(copy.userdata());
        this.checkExist = false;
        this.rebuild = true;
    }

    @Override
    public IndexLabel build() {
        Id id = this.validOrGenerateId(HugeType.INDEX_LABEL,
                                       this.id, this.name);
        this.checkBaseType();
        this.checkIndexType();

        HugeGraph graph = this.graph();
        this.checkFields4Range();
        IndexLabel indexLabel = new IndexLabel(graph, id, this.name);
        indexLabel.baseType(this.baseType);
        SchemaLabel schemaLabel = this.loadElement();
        indexLabel.baseValue(schemaLabel.id());
        indexLabel.indexType(this.indexType);
        for (String field : this.indexFields) {
            PropertyKey propertyKey = graph.propertyKey(field);
            indexLabel.indexField(propertyKey.id());
        }
        indexLabel.userdata(this.userdata);
        return indexLabel;
    }


    /**
     * Check whether this has same properties with existedIndexLabel.
     * Only baseType, baseValue, indexType, indexFields are checked.
     * The id, checkExist, userdata are not checked.
     * @param existedIndexLabel to be compared with
     * @return true if this has same properties with existedIndexLabel
     */
    private boolean hasSameProperties(IndexLabel existedIndexLabel) {
        // baseType is null, it means HugeType.SYS_SCHEMA
        if ((this.baseType == null &&
             existedIndexLabel.baseType() != HugeType.SYS_SCHEMA) ||
            (this.baseType != null &&
             this.baseType != existedIndexLabel.baseType())) {
            return false;
        }

        SchemaLabel schemaLabel = this.loadElement();
        if (!schemaLabel.id().equals(existedIndexLabel.baseValue())) {
            return false;
        }

        if (this.indexType == null) {
            // The default index type is SECONDARY
            if (existedIndexLabel.indexType() != IndexType.SECONDARY) {
                return false;
            }
        } else {
            // NOTE: IndexType.RANGE.isRange() return false
            if (this.indexType == IndexType.RANGE) {
                // existedIndexLabel index type format: RANGE_INT, RANGE_LONG
                if (!existedIndexLabel.indexType().isRange()) {
                    return false;
                }
            } else if (this.indexType != existedIndexLabel.indexType()) {
                return false;
            }
        }

        List<Id> existedIndexFieldIds = existedIndexLabel.indexFields();
        if (this.indexFields.size() != existedIndexFieldIds.size()) {
            return false;
        }
        for (String field : this.indexFields) {
            PropertyKey propertyKey = graph().propertyKey(field);
            if (!existedIndexFieldIds.contains(propertyKey.id())) {
                return false;
            }
        }
        // all properties are same, return true.
        return true;
    }

    /**
     * Create index label with async mode
     */
    @Override
    public IndexLabel.CreatedIndexLabel createWithTask() {
        HugeType type = HugeType.INDEX_LABEL;
        this.checkSchemaName(this.name);

        return this.lockCheckAndCreateSchema(type, this.name, name -> {
            IndexLabel indexLabel = this.indexLabelOrNull(name);
            if (indexLabel != null) {
                if (this.checkExist || !hasSameProperties(indexLabel)) {
                    throw new ExistedException(type, name);
                }
                return new IndexLabel.CreatedIndexLabel(indexLabel,
                                                        IdGenerator.ZERO);
            }
            this.checkSchemaIdIfRestoringMode(type, this.id);

            this.checkBaseType();
            this.checkIndexType();

            SchemaLabel schemaLabel = this.loadElement();

            /*
             * If new index label is prefix of existed index label, or has
             * the same fields, fail to create new index label.
             */
            this.checkFields(schemaLabel.properties());
            this.checkRepeatIndex(schemaLabel);
            Userdata.check(this.userdata, Action.INSERT);

            // Async delete index label which is prefix of the new index label
            // TODO: use event to replace direct call
            Set<Id> removeTasks = this.removeSubIndex(schemaLabel);

            indexLabel = this.build();
            assert indexLabel.name().equals(name);

            /*
             * If not rebuild, just create index label and return.
             * The actual indexes may be rebuilt later as needed
             */
            if (!this.rebuild) {
                indexLabel.status(SchemaStatus.CREATED);
                this.graph().addIndexLabel(schemaLabel, indexLabel);
                return new IndexLabel.CreatedIndexLabel(indexLabel,
                                                        IdGenerator.ZERO);
            }

            // Create index label (just schema)
            indexLabel.status(SchemaStatus.CREATING);
            this.graph().addIndexLabel(schemaLabel, indexLabel);
            try {
                // Async rebuild index
                Id rebuildTask = this.rebuildIndex(indexLabel, removeTasks);
                E.checkNotNull(rebuildTask, "rebuild-index task");

                return new IndexLabel.CreatedIndexLabel(indexLabel,
                                                        rebuildTask);
            } catch (Throwable e) {
                this.updateSchemaStatus(indexLabel, SchemaStatus.INVALID);
                throw e;
            }
        });
    }

    /**
     * Create index label with sync mode
     */
    @Override
    public IndexLabel create() {
        // Create index label async
        IndexLabel.CreatedIndexLabel createdIndexLabel = this.createWithTask();

        Id task = createdIndexLabel.task();
        if (task == IdGenerator.ZERO) {
            /*
             * Task id will be IdGenerator.ZERO if creating index label
             * already exists.
             */
            return createdIndexLabel.indexLabel();
        }

        // Wait task completed (change to sync mode)
        HugeGraph graph = this.graph();
        long timeout = graph.option(CoreOptions.TASK_WAIT_TIMEOUT);
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
        IndexLabel indexLabel = this.indexLabelOrNull(this.name);
        if (indexLabel == null) {
            throw new NotFoundException("Can't update index label '%s' " +
                                        "since it doesn't exist", this.name);
        }
        this.checkStableVars();
        Userdata.check(this.userdata, Action.APPEND);
        indexLabel.userdata(this.userdata);
        SchemaLabel schemaLabel = indexLabel.baseElement();
        this.graph().addIndexLabel(schemaLabel, indexLabel);
        return indexLabel;
    }

    @Override
    public IndexLabel eliminate() {
        IndexLabel indexLabel = this.indexLabelOrNull(this.name);
        if (indexLabel == null) {
            throw new NotFoundException("Can't update index label '%s' " +
                                        "since it doesn't exist", this.name);
        }
        this.checkStableVars();
        Userdata.check(this.userdata, Action.ELIMINATE);

        indexLabel.removeUserdata(this.userdata);
        SchemaLabel schemaLabel = indexLabel.baseElement();
        this.graph().addIndexLabel(schemaLabel, indexLabel);
        return indexLabel;
    }

    @Override
    public Id remove() {
        IndexLabel indexLabel = this.indexLabelOrNull(this.name);
        if (indexLabel == null) {
            return null;
        }
        return this.graph().removeIndexLabel(indexLabel.id());
    }

    @Override
    public Id rebuild() {
        IndexLabel indexLabel = this.indexLabelOrNull(this.name);
        if (indexLabel == null) {
            return null;
        }
        return this.graph().rebuildIndex(indexLabel);
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
    public IndexLabel.Builder rebuild(boolean rebuild) {
        this.rebuild = rebuild;
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
        return IndexLabel.getElement(this.graph(),
                                     this.baseType, this.baseValue);
    }

    private void checkFields(Set<Id> propertyIds) {
        List<String> fields = this.indexFields;
        E.checkNotEmpty(fields, "index fields", this.name);

        for (String field : fields) {
            PropertyKey pkey = this.propertyKeyOrNull(field);
            // In general this will not happen
            E.checkArgument(pkey != null,
                            "Can't build index on undefined property key " +
                            "'%s' for '%s': '%s'", field,
                            this.baseType.readableName(), this.baseValue);
            E.checkArgument(pkey.aggregateType().isIndexable(),
                            "The aggregate type %s is not indexable",
                            pkey.aggregateType());
        }

        List<String> properties = this.graph().mapPkId2Name(propertyIds);
        E.checkArgument(properties.containsAll(fields),
                        "Not all index fields '%s' are contained in " +
                        "schema properties '%s'", fields, properties);

        // Range index must build on single numeric column
        if (this.indexType == IndexType.RANGE) {
            this.checkFields4Range();
        }

        // Search index must build on single text column
        if (this.indexType.isSearch()) {
            E.checkArgument(fields.size() == 1,
                            "Search index can only build on " +
                            "one field, but got %s fields: '%s'",
                            fields.size(), fields);
            String field = fields.iterator().next();
            DataType dataType = this.graph().propertyKey(field).dataType();
            E.checkArgument(dataType.isText(),
                            "Search index can only build on text property, " +
                            "but got %s(%s)", dataType, field);
        }
    }

    private void checkFields4Range() {
        if (this.indexType != IndexType.RANGE) {
            return;
        }
        List<String> fields = this.indexFields;
        E.checkArgument(fields.size() == 1,
                        "Range index can only build on " +
                        "one field, but got %s fields: '%s'",
                        fields.size(), fields);
        String field = fields.iterator().next();
        DataType dataType = this.graph().propertyKey(field).dataType();
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
            IndexLabel old = this.graph().indexLabel(id);
            if (!this.hasSubIndex(old)) {
                continue;
            }
            List<String> oldFields = this.graph()
                                         .mapPkId2Name(old.indexFields());
            List<String> newFields = this.indexFields;
            /*
             * Remove the existed index label if:
             * 1. new unique index label is subset of existed unique index label
             * or
             * 2. existed index label is prefix of new created index label
             * (except for unique index)
             */
            if (this.indexType.isUnique() && oldFields.containsAll(newFields) ||
                !this.indexType.isUnique() &&
                CollectionUtil.prefixOf(oldFields, newFields)) {
                overrideIndexLabelIds.add(id);
            }
        }
        Set<Id> tasks = InsertionOrderUtil.newSet();
        for (Id id : overrideIndexLabelIds) {
            schemaLabel.removeIndexLabel(id);
            Id task = this.graph().removeIndexLabel(id);
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
                    this.indexType.isUnique() ||
                    this.indexType.isShard() &&
                    this.allStringIndex(this.indexFields)) {
                    List<String> pks = this.graph()
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
            IndexLabel old = this.graph().indexLabel(id);
            if (!Arrays.asList(checkedTypes).contains(old.indexType())) {
                continue;
            }
            List<String> newFields = this.indexFields;
            List<String> oldFields = this.graph()
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
                             this.graph().propertyKey((Id) field) :
                             this.graph().propertyKey((String) field);
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
        PropertyKey propertyKey = this.graph().propertyKey(field);
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
}
