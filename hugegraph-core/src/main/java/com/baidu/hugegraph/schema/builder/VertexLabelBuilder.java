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
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.tx.SchemaTransaction;
import com.baidu.hugegraph.exception.ExistedException;
import com.baidu.hugegraph.exception.NotAllowException;
import com.baidu.hugegraph.exception.NotFoundException;
import com.baidu.hugegraph.exception.NotSupportException;
import com.baidu.hugegraph.schema.PropertyKey;
import com.baidu.hugegraph.schema.SchemaElement;
import com.baidu.hugegraph.schema.VertexLabel;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.define.IdStrategy;
import com.baidu.hugegraph.util.CollectionUtil;
import com.baidu.hugegraph.util.E;
import com.google.common.collect.ImmutableList;

public class VertexLabelBuilder implements VertexLabel.Builder {

    private String name;
    private IdStrategy idStrategy;
    private Set<String> properties;
    private List<String> primaryKeys;
    private Set<String> nullableKeys;
    private Map<String, Object> userData;
    private boolean checkExist;

    private SchemaTransaction transaction;

    public VertexLabelBuilder(String name, SchemaTransaction transaction) {
        E.checkNotNull(name, "name");
        E.checkNotNull(transaction, "transaction");
        this.name = name;
        this.idStrategy = IdStrategy.DEFAULT;
        this.properties = new HashSet<>();
        this.primaryKeys = new ArrayList<>();
        this.nullableKeys = new HashSet<>();
        this.userData = new HashMap<>();
        this.checkExist = true;
        this.transaction = transaction;
    }

    @Override
    public VertexLabel build() {
        HugeGraph graph = this.transaction.graph();
        Id id = this.transaction.getNextId(HugeType.VERTEX_LABEL);
        VertexLabel vertexLabel = new VertexLabel(graph, id, this.name);
        vertexLabel.idStrategy(this.idStrategy);
        // Assign properties
        for (String key : this.properties) {
            PropertyKey propertyKey = this.transaction.getPropertyKey(key);
            vertexLabel.property(propertyKey.id());
        }
        for (String key : this.primaryKeys) {
            PropertyKey propertyKey = this.transaction.getPropertyKey(key);
            vertexLabel.primaryKey(propertyKey.id());
        }
        for (String key : this.nullableKeys) {
            PropertyKey propertyKey = this.transaction.getPropertyKey(key);
            vertexLabel.nullableKey(propertyKey.id());
        }
        for (String key : this.userData.keySet()) {
            vertexLabel.userData(key, this.userData.get(key));
        }
        return vertexLabel;
    }

    @Override
    public VertexLabel create() {
        SchemaElement.checkName(this.name,
                                this.transaction.graph().configuration());
        VertexLabel vertexLabel = this.transaction.getVertexLabel(this.name);
        if (vertexLabel != null) {
            if (this.checkExist) {
                throw new ExistedException("vertex label", this.name);
            }
            return vertexLabel;
        }

        this.checkProperties();
        this.checkIdStrategy();
        this.checkNullableKeys(false);

        vertexLabel = this.build();
        this.transaction.addVertexLabel(vertexLabel);
        return vertexLabel;
    }

    @Override
    public VertexLabel append() {
        VertexLabel vertexLabel = this.transaction.getVertexLabel(this.name);
        if (vertexLabel == null) {
            throw new NotFoundException("Can't update vertex label '%s' " +
                                        "since it doesn't exist", this.name);
        }

        this.checkStableVars();
        this.checkProperties();
        this.checkNullableKeys(true);

        for (String key : this.properties) {
            PropertyKey propertyKey = this.transaction.getPropertyKey(key);
            vertexLabel.property(propertyKey.id());
        }
        for (String key : this.nullableKeys) {
            PropertyKey propertyKey = this.transaction.getPropertyKey(key);
            vertexLabel.nullableKey(propertyKey.id());
        }
        this.transaction.addVertexLabel(vertexLabel);
        return vertexLabel;
    }

    @Override
    public VertexLabel eliminate() {
        throw new NotSupportException("action eliminate on vertex label");
    }

    @Override
    public void remove() {
        VertexLabel vertexLabel = this.transaction.getVertexLabel(this.name);
        if (vertexLabel == null) {
            return;
        }
        this.transaction.removeVertexLabel(vertexLabel.id());
    }

    @Override
    public void rebuildIndex() {
        VertexLabel vertexLabel = this.transaction.graph().vertexLabel(
                                  this.name);
        this.transaction.rebuildIndex(vertexLabel);
    }

    @Override
    public VertexLabelBuilder useAutomaticId() {
        E.checkArgument(this.idStrategy == IdStrategy.DEFAULT ||
                        this.idStrategy == IdStrategy.AUTOMATIC,
                        "Not allowed to change id strategy for " +
                        "vertex label '%s'", this.name);
        this.idStrategy = IdStrategy.AUTOMATIC;
        return this;
    }

    @Override
    public VertexLabelBuilder usePrimaryKeyId() {
        E.checkArgument(this.idStrategy == IdStrategy.DEFAULT ||
                        this.idStrategy == IdStrategy.PRIMARY_KEY,
                        "Not allowed to change id strategy for " +
                        "vertex label '%s'", this.name);
        this.idStrategy = IdStrategy.PRIMARY_KEY;
        return this;
    }

    @Override
    public VertexLabelBuilder useCustomizeStringId() {
        E.checkArgument(this.idStrategy == IdStrategy.DEFAULT ||
                        this.idStrategy == IdStrategy.CUSTOMIZE_STRING,
                        "Not allowed to change id strategy for " +
                        "vertex label '%s'", this.name);
        this.idStrategy = IdStrategy.CUSTOMIZE_STRING;
        return this;
    }

    @Override
    public VertexLabelBuilder useCustomizeNumberId() {
        E.checkArgument(this.idStrategy == IdStrategy.DEFAULT ||
                        this.idStrategy == IdStrategy.CUSTOMIZE_NUMBER,
                        "Not allowed to change id strategy for " +
                        "vertex label '%s'", this.name);
        this.idStrategy = IdStrategy.CUSTOMIZE_NUMBER;
        return this;
    }

    @Override
    public VertexLabelBuilder properties(String... properties) {
        this.properties.addAll(Arrays.asList(properties));
        return this;
    }

    @Override
    public VertexLabelBuilder primaryKeys(String... keys) {
        if (keys.length == 0) {
            return this;
        }

        E.checkArgument(this.primaryKeys.isEmpty(),
                        "Not allowed to assign primary keys multitimes");

        List<String> primaryKeys = Arrays.asList(keys);
        E.checkArgument(CollectionUtil.allUnique(primaryKeys),
                        "Invalid primary keys %s, which contains some " +
                        "duplicate properties", primaryKeys);
        this.primaryKeys.addAll(primaryKeys);
        return this;
    }

    @Override
    public VertexLabelBuilder nullableKeys(String... keys) {
        this.nullableKeys.addAll(Arrays.asList(keys));
        return this;
    }

    @Override
    public VertexLabelBuilder userData(String key, Object value) {
        this.userData.put(key, value);
        return this;
    }

    @Override
    public VertexLabelBuilder idStrategy(IdStrategy idStrategy) {
        E.checkArgument(this.idStrategy == IdStrategy.DEFAULT ||
                        this.idStrategy == idStrategy,
                        "Not allowed to change id strategy for " +
                        "vertex label '%s'", this.name);
        this.idStrategy = idStrategy;
        return this;
    }

    @Override
    public VertexLabel.Builder userData(Map<String, Object> userData) {
        this.userData.putAll(userData);
        return this;
    }

    @Override
    public VertexLabelBuilder ifNotExist() {
        this.checkExist = false;
        return this;
    }

    @Override
    public VertexLabelBuilder checkExist(boolean checkExist) {
        this.checkExist = checkExist;
        return this;
    }

    private void checkProperties() {
        for (String key : this.properties) {
            PropertyKey propertyKey = this.transaction.getPropertyKey(key);
            E.checkArgumentNotNull(propertyKey,
                                   "Undefined property key '%s'", key);
        }
    }

    @SuppressWarnings("unchecked")
    private void checkNullableKeys(boolean append) {
        VertexLabel vertexLabel = this.transaction.getVertexLabel(this.name);
        // The originProps is empty when firstly create vertex label
        List<String> originProps = vertexLabel == null ?
                                   ImmutableList.of() :
                                   this.transaction.graph()
                                       .mapPkId2Name(vertexLabel.properties());
        Set<String> appendProps = this.properties;

        E.checkArgument(CollectionUtils.union(originProps, appendProps)
                        .containsAll(this.nullableKeys),
                        "The nullableKeys: %s to be created or appended " +
                        "must belong to the origin/new properties: %s/%s",
                        this.nullableKeys, originProps, appendProps);

        Collection<String> intersecKeys = CollectionUtils.intersection(
                                          this.primaryKeys, this.nullableKeys);
        E.checkArgument(intersecKeys.isEmpty(),
                        "The nullableKeys: %s are not allowed to " +
                        "belong to primaryKeys: %s of vertex label '%s'",
                        this.nullableKeys, this.primaryKeys, this.name);

        if (append) {
            Collection<String> newAddedProps = CollectionUtils.subtract(
                                               appendProps, originProps);
            E.checkArgument(this.nullableKeys.containsAll(newAddedProps),
                            "The new added properties: %s must be nullable",
                            newAddedProps);
        }
    }

    private void checkIdStrategy() {
        IdStrategy strategy = this.idStrategy;
        boolean hasPrimaryKey = this.primaryKeys.size() > 0;
        switch (strategy) {
            case DEFAULT:
                if (hasPrimaryKey) {
                    this.idStrategy = IdStrategy.PRIMARY_KEY;
                } else {
                    this.idStrategy = IdStrategy.AUTOMATIC;
                }
                break;
            case AUTOMATIC:
            case CUSTOMIZE_STRING:
            case CUSTOMIZE_NUMBER:
                E.checkArgument(!hasPrimaryKey,
                                "Not allowed to assign primary keys " +
                                "when using '%s' id strategy", strategy);
                break;
            case PRIMARY_KEY:
                E.checkArgument(hasPrimaryKey,
                                "Must assign some primary keys " +
                                "when using '%s' id strategy", strategy);
                break;
            default:
                throw new AssertionError(String.format(
                          "Unknown id strategy '%s'", strategy));
        }
        if (this.idStrategy == IdStrategy.PRIMARY_KEY) {
            this.checkPrimaryKeys();
        }
    }

    private void checkPrimaryKeys() {
        E.checkArgument(this.idStrategy == IdStrategy.DEFAULT ||
                        this.idStrategy == IdStrategy.PRIMARY_KEY,
                        "Not allowed to use id strategy '%s' and assign " +
                        "primary keys at the same time for vertex label '%s'",
                        this.idStrategy, this.name);

        E.checkArgument(!this.properties.isEmpty(),
                        "The properties of vertex label '%s' " +
                        "can't be empty when id strategy is '%s'",
                        this.name, IdStrategy.PRIMARY_KEY);

        E.checkNotEmpty(this.primaryKeys, "primary keys", this.name);
        // Use loop instead containAll for more detailed exception info.
        for (String key : this.primaryKeys) {
            E.checkArgument(this.properties.contains(key),
                            "The primary key '%s' of vertex label '%s' " +
                            "must be contained in properties: %s",
                            key, this.name, this.properties);
        }
    }

    private void checkStableVars() {
        // Don't allow to append primary keys.
        if (!this.primaryKeys.isEmpty()) {
            throw new NotAllowException(
                      "Not allowed to update primary keys " +
                      "for vertex label '%s'", this.name);
        }
        if (this.idStrategy != IdStrategy.DEFAULT) {
            throw new NotAllowException(
                      "Not allowed to update id strategy " +
                      "for vertex label '%s'", this.name);
        }
    }
}
