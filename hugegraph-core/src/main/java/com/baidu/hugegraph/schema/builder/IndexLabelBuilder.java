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
import java.util.Set;
import java.util.concurrent.TimeoutException;

import com.baidu.hugegraph.HugeException;
import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.id.IdGenerator;
import com.baidu.hugegraph.backend.tx.SchemaTransaction;
import com.baidu.hugegraph.config.CoreOptions;
import com.baidu.hugegraph.exception.ExistedException;
import com.baidu.hugegraph.exception.NotSupportException;
import com.baidu.hugegraph.schema.IndexLabel;
import com.baidu.hugegraph.schema.PropertyKey;
import com.baidu.hugegraph.schema.SchemaElement;
import com.baidu.hugegraph.schema.SchemaLabel;
import com.baidu.hugegraph.schema.VertexLabel;
import com.baidu.hugegraph.type.HugeType;
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
    private boolean checkExist;

    private SchemaTransaction transaction;

    public IndexLabelBuilder(String name, SchemaTransaction transaction) {
        E.checkNotNull(name, "name");
        E.checkNotNull(transaction, "transaction");
        this.id = null;
        this.name = name;
        this.indexType = IndexType.SECONDARY;
        this.indexFields = new ArrayList<>();
        this.checkExist = true;
        this.transaction = transaction;
    }

    @Override
    public IndexLabel build() {
        Id id = this.transaction.validOrGenerateId(HugeType.INDEX_LABEL,
                                                   this.id, this.name);
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
        return indexLabel;
    }

    /**
     * Create index label with async mode
     */
    @Override
    public IndexLabel.CreatedIndexLabel createWithTask() {
        SchemaTransaction tx = this.transaction;
        SchemaElement.checkName(this.name, tx.graph().configuration());
        IndexLabel indexLabel = tx.getIndexLabel(this.name);
        if (indexLabel != null) {
            if (this.checkExist) {
                throw new ExistedException("index label", this.name);
            }
            return new IndexLabel.CreatedIndexLabel(indexLabel, null);
        }
        tx.checkIdIfRestoringMode(HugeType.INDEX_LABEL, this.id);

        SchemaLabel schemaLabel = this.loadElement();

        /*
         * If new index label is prefix of existed index label, or has
         * the same fields, fail to create new index label.
         */
        this.checkFields(schemaLabel.properties());
        this.checkRepeatIndex(schemaLabel);

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
        throw new NotSupportException("action append on index label");
    }

    @Override
    public IndexLabel eliminate() {
        throw new NotSupportException("action eliminate on index label");
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
        }

        // Search index must build on single text column
        if (this.indexType == IndexType.SEARCH) {
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
        if (schemaLabel instanceof VertexLabel) {
            VertexLabel vl = (VertexLabel) schemaLabel;
            if (vl.idStrategy().isPrimaryKey()) {
                List<String> pkFields = this.transaction.graph()
                                            .mapPkId2Name(vl.primaryKeys());
                E.checkArgument(!this.indexFields.containsAll(pkFields),
                                "No need to build index on properties %s, " +
                                "because they contains all primary keys %s " +
                                "for vertex label '%s'",
                                this.indexFields, pkFields, vl.name());
            }
        }
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
                            "fields %s of existed index label '%s'",
                            newFields, this.name, oldFields, old.name());
        }
    }

    private Set<Id> removeSubIndex(SchemaLabel schemaLabel) {
        Set<Id> overrideIndexLabelIds = InsertionOrderUtil.newSet();
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
        Set<Id> tasks = InsertionOrderUtil.newSet();
        for (Id id : overrideIndexLabelIds) {
            schemaLabel.removeIndexLabel(id);
            Id task = this.transaction.removeIndexLabel(id);
            E.checkNotNull(task, "remove sub index label task");
            tasks.add(task);
        }
        return tasks;
    }
}
