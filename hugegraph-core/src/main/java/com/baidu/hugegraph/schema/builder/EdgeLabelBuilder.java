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
import com.baidu.hugegraph.backend.id.IdGenerator;
import com.baidu.hugegraph.backend.tx.SchemaTransaction;
import com.baidu.hugegraph.exception.ExistedException;
import com.baidu.hugegraph.exception.NotAllowException;
import com.baidu.hugegraph.exception.NotFoundException;
import com.baidu.hugegraph.schema.EdgeLabel;
import com.baidu.hugegraph.schema.PropertyKey;
import com.baidu.hugegraph.schema.SchemaElement;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.define.Action;
import com.baidu.hugegraph.type.define.Frequency;
import com.baidu.hugegraph.util.CollectionUtil;
import com.baidu.hugegraph.util.E;
import com.google.common.collect.ImmutableList;

public class EdgeLabelBuilder implements EdgeLabel.Builder {

    private Id id;
    private String name;
    private String sourceLabel;
    private String targetLabel;
    private Frequency frequency;
    private Set<String> properties;
    private List<String> sortKeys;
    private Set<String> nullableKeys;
    private Boolean enableLabelIndex;
    private Map<String, Object> userdata;
    private boolean checkExist;

    private SchemaTransaction transaction;

    public EdgeLabelBuilder(String name, SchemaTransaction transaction) {
        E.checkNotNull(name, "name");
        E.checkNotNull(transaction, "transaction");
        this.id = null;
        this.name = name;
        this.sourceLabel = null;
        this.targetLabel = null;
        this.frequency = Frequency.DEFAULT;
        this.properties = new HashSet<>();
        this.sortKeys = new ArrayList<>();
        this.nullableKeys = new HashSet<>();
        this.enableLabelIndex = null;
        this.userdata = new HashMap<>();
        this.checkExist = true;
        this.transaction = transaction;
    }

    @Override
    public EdgeLabel build() {
        Id id = this.transaction.validOrGenerateId(HugeType.EDGE_LABEL,
                                                   this.id, this.name);
        HugeGraph graph = this.transaction.graph();
        EdgeLabel edgeLabel = new EdgeLabel(graph, id, this.name);
        edgeLabel.sourceLabel(this.transaction.getVertexLabel(
                              this.sourceLabel).id());
        edgeLabel.targetLabel(this.transaction.getVertexLabel(
                              this.targetLabel).id());
        edgeLabel.frequency(this.frequency);
        edgeLabel.enableLabelIndex(this.enableLabelIndex == null ||
                                   this.enableLabelIndex);
        for (String key : this.properties) {
            PropertyKey propertyKey = this.transaction.getPropertyKey(key);
            edgeLabel.property(propertyKey.id());
        }
        for (String key : this.sortKeys) {
            PropertyKey propertyKey = this.transaction.getPropertyKey(key);
            edgeLabel.sortKey(propertyKey.id());
        }
        for (String key : this.nullableKeys) {
            PropertyKey propertyKey = this.transaction.getPropertyKey(key);
            edgeLabel.nullableKey(propertyKey.id());
        }
        for (Map.Entry<String, Object> entry : this.userdata.entrySet()) {
            edgeLabel.userdata(entry.getKey(), entry.getValue());
        }
        return edgeLabel;
    }

    @Override
    public EdgeLabel create() {
        HugeType type = HugeType.EDGE_LABEL;
        SchemaTransaction tx = this.transaction;
        SchemaElement.checkName(this.name, tx.graph().configuration());
        EdgeLabel edgeLabel = tx.getEdgeLabel(this.name);
        if (edgeLabel != null) {
            if (this.checkExist) {
                throw new ExistedException(type, this.name);
            }
            return edgeLabel;
        }
        tx.checkIdIfRestoringMode(type, this.id);

        if (this.frequency == Frequency.DEFAULT) {
            this.frequency = Frequency.SINGLE;
        }
        // These methods will check params and fill to member variables
        this.checkRelation();
        this.checkProperties(Action.INSERT);
        this.checkSortKeys();
        this.checkNullableKeys(Action.INSERT);
        this.checkUserdata(Action.INSERT);

        edgeLabel = this.build();
        tx.addEdgeLabel(edgeLabel);
        return edgeLabel;
    }

    @Override
    public EdgeLabel append() {
        EdgeLabel edgeLabel = this.transaction.getEdgeLabel(this.name);
        if (edgeLabel == null) {
            throw new NotFoundException("Can't update edge label '%s' " +
                                        "since it doesn't exist", this.name);
        }
        // These methods will check params and fill to member variables
        this.checkStableVars();
        this.checkProperties(Action.APPEND);
        this.checkNullableKeys(Action.APPEND);
        this.checkUserdata(Action.APPEND);

        for (String key : this.properties) {
            PropertyKey propertyKey = this.transaction.getPropertyKey(key);
            edgeLabel.property(propertyKey.id());
        }
        for (String key : this.nullableKeys) {
            PropertyKey propertyKey = this.transaction.getPropertyKey(key);
            edgeLabel.nullableKey(propertyKey.id());
        }
        for (Map.Entry<String, Object> entry : this.userdata.entrySet()) {
            edgeLabel.userdata(entry.getKey(), entry.getValue());
        }
        this.transaction.addEdgeLabel(edgeLabel);
        return edgeLabel;
    }

    @Override
    public EdgeLabel eliminate() {
        EdgeLabel edgeLabel = this.transaction.getEdgeLabel(this.name);
        if (edgeLabel == null) {
            throw new NotFoundException("Can't update edge label '%s' " +
                                        "since it doesn't exist", this.name);
        }
        // Only allowed to eliminate user data
        this.checkStableVars();
        this.checkProperties(Action.ELIMINATE);
        this.checkNullableKeys(Action.ELIMINATE);
        this.checkUserdata(Action.ELIMINATE);

        for (String key : this.userdata.keySet()) {
            edgeLabel.removeUserdata(key);
        }
        this.transaction.addEdgeLabel(edgeLabel);
        return edgeLabel;
    }

    @Override
    public Id remove() {
        EdgeLabel edgeLabel = this.transaction.getEdgeLabel(this.name);
        if (edgeLabel == null) {
            return null;
        }
        return this.transaction.removeEdgeLabel(edgeLabel.id());
    }

    @Override
    public Id rebuildIndex() {
        EdgeLabel edgeLabel = this.transaction.graph().edgeLabel(this.name);
        if (edgeLabel == null) {
            return null;
        }
        return this.transaction.rebuildIndex(edgeLabel);
    }

    @Override
    public EdgeLabelBuilder id(long id) {
        E.checkArgument(id != 0L, "Not allowed to assign 0 as edge label id");
        this.id = IdGenerator.of(id);
        return this;
    }

    @Override
    public EdgeLabelBuilder properties(String... properties) {
        this.properties.addAll(Arrays.asList(properties));
        return this;
    }

    @Override
    public EdgeLabelBuilder nullableKeys(String... keys) {
        this.nullableKeys.addAll(Arrays.asList(keys));
        return this;
    }

    @Override
    public EdgeLabelBuilder sortKeys(String... keys) {
        if (keys.length == 0) {
            return this;
        }

        E.checkArgument(this.sortKeys.isEmpty(),
                        "Not allowed to assign sort keys multitimes");

        List<String> sortKeys = Arrays.asList(keys);
        E.checkArgument(CollectionUtil.allUnique(sortKeys),
                        "Invalid sort keys %s, which contains some " +
                        "duplicate properties", sortKeys);
        this.sortKeys.addAll(sortKeys);
        return this;
    }

    @Override
    public EdgeLabelBuilder link(String sourceLabel, String targetLabel) {
        this.sourceLabel(sourceLabel);
        this.targetLabel(targetLabel);
        return this;
    }

    @Override
    public EdgeLabelBuilder sourceLabel(String label) {
        this.sourceLabel = label;
        return this;
    }

    @Override
    public EdgeLabelBuilder targetLabel(String label) {
        this.targetLabel = label;
        return this;
    }

    @Override
    public EdgeLabelBuilder singleTime() {
        this.frequency = Frequency.SINGLE;
        return this;
    }

    @Override
    public EdgeLabelBuilder multiTimes() {
        this.frequency = Frequency.MULTIPLE;
        return this;
    }

    @Override
    public EdgeLabelBuilder ifNotExist() {
        this.checkExist = false;
        return this;
    }

    @Override
    public EdgeLabelBuilder frequency(Frequency frequency) {
        this.frequency = frequency;
        return this;
    }

    @Override
    public EdgeLabelBuilder enableLabelIndex(boolean enable) {
        this.enableLabelIndex = enable;
        return this;
    }

    @Override
    public EdgeLabelBuilder userdata(String key, Object value) {
        this.userdata.put(key, value);
        return this;
    }

    @Override
    public EdgeLabelBuilder userdata(Map<String, Object> userdata) {
        this.userdata.putAll(userdata);
        return this;
    }

    @Override
    public EdgeLabelBuilder checkExist(boolean checkExist) {
        this.checkExist = checkExist;
        return this;
    }

    private void checkProperties(Action action) {
        switch (action) {
            case INSERT:
            case APPEND:
                for (String key : this.properties) {
                    PropertyKey pkey = this.transaction.getPropertyKey(key);
                    E.checkArgumentNotNull(pkey,
                                           "Undefined property key '%s'", key);
                }
                break;
            case ELIMINATE:
                if (!this.properties.isEmpty()) {
                    throw new NotAllowException(
                              "Not support to eliminate properties " +
                              "for edge label currently");
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
                          "for edge label currently");
            }
            return;
        }

        EdgeLabel edgeLabel = this.transaction.getEdgeLabel(this.name);
        // The originProps is empty when firstly create edge label
        List<String> originProps = edgeLabel == null ?
                                   ImmutableList.of() :
                                   this.transaction.graph()
                                       .mapPkId2Name(edgeLabel.properties());
        Set<String> appendProps = this.properties;

        E.checkArgument(CollectionUtil.union(originProps, appendProps)
                        .containsAll(this.nullableKeys),
                        "The nullableKeys: %s to be created or appended " +
                        "must belong to the origin/new properties: %s/%s ",
                        this.nullableKeys, originProps, appendProps);

        E.checkArgument(!CollectionUtil.hasIntersection(this.sortKeys,
                                                        this.nullableKeys),
                        "The nullableKeys: %s are not allowed to " +
                        "belong to sortKeys: %s of edge label '%s'",
                        this.nullableKeys, this.sortKeys, this.name);

        if (action == Action.APPEND) {
            Collection<String> newAddedProps = CollectionUtils.subtract(
                                               appendProps, originProps);
            E.checkArgument(this.nullableKeys.containsAll(newAddedProps),
                            "The new added properties: %s must be nullable",
                            newAddedProps);
        }
    }

    private void checkSortKeys() {
        if (this.frequency == Frequency.SINGLE) {
            E.checkArgument(this.sortKeys.isEmpty(),
                            "EdgeLabel can't contain sortKeys " +
                            "when the cardinality property is single");
        } else {
            E.checkState(this.sortKeys != null,
                         "The sortKeys can't be null when the " +
                         "cardinality property is multiple");
            E.checkArgument(!this.sortKeys.isEmpty(),
                            "EdgeLabel must contain sortKeys " +
                            "when the cardinality property is multiple");
        }

        if (this.sortKeys.isEmpty()) {
            return;
        }

        // Check whether the properties contains the specified keys
        E.checkArgument(!this.properties.isEmpty(),
                        "The properties can't be empty when exist " +
                        "sort keys for edge label '%s'", this.name);

        for (String key : this.sortKeys) {
            E.checkArgument(this.properties.contains(key),
                            "The sort key '%s' must be contained in " +
                            "properties '%s' for edge label '%s'",
                            key, this.name, this.properties);
        }
    }

    private void checkRelation() {
        String srcLabel = this.sourceLabel;
        String tgtLabel = this.targetLabel;

        E.checkArgument(srcLabel != null && tgtLabel != null,
                        "Must set source and target label " +
                        "for edge label '%s'", this.name);

        E.checkArgumentNotNull(this.transaction.getVertexLabel(srcLabel),
                               "Undefined source vertex label '%s' " +
                               "in edge label '%s'", srcLabel, this.name);
        E.checkArgumentNotNull(this.transaction.getVertexLabel(tgtLabel),
                               "Undefined target vertex label '%s' " +
                               "in edge label '%s'", tgtLabel, this.name);
    }

    private void checkStableVars() {
        if (this.sourceLabel != null) {
            throw new NotAllowException(
                      "Not allowed to update source label " +
                      "for edge label '%s', it must be null", this.name);
        }
        if (this.targetLabel != null) {
            throw new NotAllowException(
                      "Not allowed to update target label " +
                      "for edge label '%s', it must be null", this.name);
        }
        if (this.frequency != Frequency.DEFAULT) {
            throw new NotAllowException(
                      "Not allowed to update frequency " +
                      "for edge label '%s'", this.name);
        }
        if (!this.sortKeys.isEmpty()) {
            throw new NotAllowException(
                      "Not allowed to update sort keys " +
                      "for edge label '%s'", this.name);
        }
        if (this.enableLabelIndex != null) {
            throw new NotAllowException(
                      "Not allowed to update enable_label_index " +
                      "for edge label '%s'", this.name);
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
