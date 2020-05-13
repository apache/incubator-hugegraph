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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.id.IdGenerator;
import com.baidu.hugegraph.backend.tx.SchemaTransaction;
import com.baidu.hugegraph.exception.ExistedException;
import com.baidu.hugegraph.exception.NotAllowException;
import com.baidu.hugegraph.exception.NotFoundException;
import com.baidu.hugegraph.schema.PropertyKey;
import com.baidu.hugegraph.schema.Userdata;
import com.baidu.hugegraph.schema.VertexLabel;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.define.Action;
import com.baidu.hugegraph.type.define.IdStrategy;
import com.baidu.hugegraph.util.CollectionUtil;
import com.baidu.hugegraph.util.E;
import com.google.common.collect.ImmutableList;

public class VertexLabelBuilder extends AbstractBuilder
                                implements VertexLabel.Builder {

    private Id id;
    private String name;
    private IdStrategy idStrategy;
    private Set<String> properties;
    private List<String> primaryKeys;
    private Set<String> nullableKeys;
    private long ttl;
    private String ttlStartTime;
    private Boolean enableLabelIndex;
    private Userdata userdata;
    private boolean checkExist;

    public VertexLabelBuilder(SchemaTransaction transaction,
                              HugeGraph graph, String name) {
        super(transaction, graph);
        E.checkNotNull(name, "name");
        this.id = null;
        this.name = name;
        this.idStrategy = IdStrategy.DEFAULT;
        this.properties = new HashSet<>();
        this.primaryKeys = new ArrayList<>();
        this.nullableKeys = new HashSet<>();
        this.ttl = 0L;
        this.ttlStartTime = null;
        this.enableLabelIndex = null;
        this.userdata = new Userdata();
        this.checkExist = true;
    }

    @Override
    public VertexLabel build() {
        Id id = this.validOrGenerateId(HugeType.VERTEX_LABEL,
                                       this.id, this.name);
        VertexLabel vertexLabel = new VertexLabel(this.graph(), id, this.name);
        vertexLabel.idStrategy(this.idStrategy);
        vertexLabel.enableLabelIndex(this.enableLabelIndex == null ||
                                     this.enableLabelIndex);
        vertexLabel.ttl(this.ttl);
        if (this.ttlStartTime != null) {
            vertexLabel.ttlStartTime(this.graph().propertyKey(
                                     this.ttlStartTime).id());
        }
        // Assign properties
        for (String key : this.properties) {
            PropertyKey propertyKey = this.graph().propertyKey(key);
            vertexLabel.property(propertyKey.id());
        }
        for (String key : this.primaryKeys) {
            PropertyKey propertyKey = this.graph().propertyKey(key);
            vertexLabel.primaryKey(propertyKey.id());
        }
        for (String key : this.nullableKeys) {
            PropertyKey propertyKey = this.graph().propertyKey(key);
            vertexLabel.nullableKey(propertyKey.id());
        }
        vertexLabel.userdata(this.userdata);
        return vertexLabel;
    }

    @Override
    public VertexLabel create() {
        HugeType type = HugeType.VERTEX_LABEL;
        this.checkSchemaName(this.name);

        return this.lockCheckAndCreateSchema(type, this.name, name -> {
            VertexLabel vertexLabel = this.vertexLabelOrNull(name);
            if (vertexLabel != null) {
                if (this.checkExist) {
                    throw new ExistedException(type, name);
                }
                return vertexLabel;
            }
            this.checkSchemaIdIfRestoringMode(type, this.id);

            this.checkProperties(Action.INSERT);
            this.checkIdStrategy();
            this.checkNullableKeys(Action.INSERT);
            Userdata.check(this.userdata, Action.INSERT);
            this.checkTtl();
            this.checkUserdata(Action.INSERT);

            vertexLabel = this.build();
            assert vertexLabel.name().equals(name);
            this.graph().addVertexLabel(vertexLabel);
            return vertexLabel;
        });
    }

    @Override
    public VertexLabel append() {
        VertexLabel vertexLabel = this.vertexLabelOrNull(this.name);
        if (vertexLabel == null) {
            throw new NotFoundException("Can't update vertex label '%s' " +
                                        "since it doesn't exist", this.name);
        }

        this.checkStableVars();
        this.checkProperties(Action.APPEND);
        this.checkNullableKeys(Action.APPEND);
        Userdata.check(this.userdata, Action.APPEND);

        for (String key : this.properties) {
            PropertyKey propertyKey = this.graph().propertyKey(key);
            vertexLabel.property(propertyKey.id());
        }
        for (String key : this.nullableKeys) {
            PropertyKey propertyKey = this.graph().propertyKey(key);
            vertexLabel.nullableKey(propertyKey.id());
        }
        vertexLabel.userdata(this.userdata);
        this.graph().addVertexLabel(vertexLabel);
        return vertexLabel;
    }

    @Override
    public VertexLabel eliminate() {
        VertexLabel vertexLabel = this.vertexLabelOrNull(this.name);
        if (vertexLabel == null) {
            throw new NotFoundException("Can't update vertex label '%s' " +
                                        "since it doesn't exist", this.name);
        }
        // Only allowed to eliminate user data
        this.checkStableVars();
        this.checkProperties(Action.ELIMINATE);
        this.checkNullableKeys(Action.ELIMINATE);
        Userdata.check(this.userdata, Action.ELIMINATE);

        vertexLabel.removeUserdata(this.userdata);
        this.graph().addVertexLabel(vertexLabel);
        return vertexLabel;
    }

    @Override
    public Id remove() {
        VertexLabel vertexLabel = this.vertexLabelOrNull(this.name);
        if (vertexLabel == null) {
            return null;
        }
        return this.graph().removeVertexLabel(vertexLabel.id());
    }

    @Override
    public Id rebuildIndex() {
        VertexLabel vertexLabel = this.vertexLabelOrNull(this.name);
        if (vertexLabel == null) {
            return null;
        }
        return this.graph().rebuildIndex(vertexLabel);
    }

    @Override
    public VertexLabelBuilder id(long id) {
        E.checkArgument(id != 0L,
                        "Not allowed to assign 0 as vertex label id");
        this.id = IdGenerator.of(id);
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
    public VertexLabelBuilder useCustomizeUuidId() {
        E.checkArgument(this.idStrategy == IdStrategy.DEFAULT ||
                        this.idStrategy == IdStrategy.CUSTOMIZE_UUID,
                        "Not allowed to change id strategy for " +
                        "vertex label '%s'", this.name);
        this.idStrategy = IdStrategy.CUSTOMIZE_UUID;
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
    public VertexLabel.Builder ttl(long ttl) {
        this.ttl = ttl;
        return this;
    }

    @Override
    public VertexLabel.Builder ttlStartTime(String ttlStartTime) {
        this.ttlStartTime = ttlStartTime;
        return this;
    }

    @Override
    public VertexLabelBuilder enableLabelIndex(boolean enable) {
        this.enableLabelIndex = enable;
        return this;
    }

    @Override
    public VertexLabelBuilder userdata(String key, Object value) {
        this.userdata.put(key, value);
        return this;
    }

    @Override
    public VertexLabelBuilder userdata(Map<String, Object> userdata) {
        this.userdata.putAll(userdata);
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

    private void checkProperties(Action action) {
        switch (action) {
            case INSERT:
            case APPEND:
                for (String key : this.properties) {
                    this.graph().propertyKey(key);
                }
                break;
            case ELIMINATE:
                if (!this.properties.isEmpty()) {
                    throw new NotAllowException(
                              "Not support to eliminate properties " +
                              "for vertex label currently");
                }
                break;
            case DELETE:
                break;
            default:
                throw new AssertionError(String.format(
                          "Unknown schema action '%s'", action));
        }
    }

    @SuppressWarnings("unchecked")
    private void checkNullableKeys(Action action) {
        // Not using switch-case to avoid indent too much
        if (action == Action.ELIMINATE) {
            if (!this.nullableKeys.isEmpty()) {
                throw new NotAllowException(
                          "Not support to eliminate nullableKeys " +
                          "for vertex label currently");
            }
            return;
        }

        VertexLabel vertexLabel = this.vertexLabelOrNull(this.name);
        // The originProps is empty when firstly create vertex label
        List<String> originProps = vertexLabel == null ?
                                   ImmutableList.of() :
                                   this.graph()
                                       .mapPkId2Name(vertexLabel.properties());
        Set<String> appendProps = this.properties;

        E.checkArgument(CollectionUtil.union(originProps, appendProps)
                                      .containsAll(this.nullableKeys),
                        "The nullableKeys: %s to be created or appended " +
                        "must belong to the origin/new properties: %s/%s",
                        this.nullableKeys, originProps, appendProps);

        E.checkArgument(!CollectionUtil.hasIntersection(this.primaryKeys,
                                                        this.nullableKeys),
                        "The nullableKeys: %s are not allowed to " +
                        "belong to primaryKeys: %s of vertex label '%s'",
                        this.nullableKeys, this.primaryKeys, this.name);

        if (action == Action.APPEND) {
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
            case CUSTOMIZE_UUID:
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
        if (this.enableLabelIndex != null) {
            throw new NotAllowException(
                      "Not allowed to update enable_label_index " +
                      "for vertex label '%s'", this.name);
        }
    }

    private void checkTtl() {
        E.checkArgument(this.ttl >= 0,
                        "The ttl must be >= 0, but got: %s", this.ttl);
        if (this.ttl == 0L) {
            E.checkArgument(this.ttlStartTime == null,
                            "Can't set ttl start time if ttl is not set");
            return;
        }
        if (this.ttlStartTime == null) {
            return;
        }
        // Check whether the properties contains the specified keys
        E.checkArgument(!this.properties.isEmpty(),
                        "The properties can't be empty when exist " +
                        "ttl start time for edge label '%s'", this.name);
        E.checkArgument(this.properties.contains(this.ttlStartTime),
                        "The ttl start time '%s' must be contained in " +
                        "properties '%s' for vertex label '%s'",
                        this.ttlStartTime, this.name, this.properties);
        PropertyKey pkey = this.graph().propertyKey(this.ttlStartTime);
        E.checkArgument(pkey.dataType().isDate(),
                        "The ttl start time property must be date type," +
                        "but got '%s(%s)'", this.ttlStartTime, pkey.dataType());
    }

    private void checkUserdata(Action action) {
        switch (action) {
            case INSERT:
            case APPEND:
                for (Map.Entry<String, Object> e : this.userdata.entrySet()) {
                    if (e.getValue() == null) {
                        throw new NotAllowException(
                                  "Not allowed pass null userdata value when " +
                                  "create or append edge label");
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
