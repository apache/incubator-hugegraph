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

package com.baidu.hugegraph.schema;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import com.baidu.hugegraph.HugeException;
import com.baidu.hugegraph.backend.tx.SchemaTransaction;
import com.baidu.hugegraph.config.CoreOptions;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.exception.ExistedException;
import com.baidu.hugegraph.exception.NotAllowException;
import com.baidu.hugegraph.exception.NotSupportException;
import com.baidu.hugegraph.schema.builder.VertexLabelBuilder;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.define.IdStrategy;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.StringUtil;

public class VertexLabel extends SchemaLabel {

    private IdStrategy idStrategy;
    private List<String> primaryKeys;

    public VertexLabel(String name) {
        super(name);
        this.idStrategy = IdStrategy.DEFAULT;
        this.primaryKeys = new ArrayList<>();
    }

    @Override
    public HugeType type() {
        return HugeType.VERTEX_LABEL;
    }

    public IdStrategy idStrategy() {
        return this.idStrategy;
    }

    public void idStrategy(IdStrategy idStrategy) {
        E.checkArgument(this.idStrategy == IdStrategy.DEFAULT ||
                        this.idStrategy == idStrategy,
                        "Not allowed to change id strategy for " +
                        "vertex label '%s'", this.name);
        this.idStrategy = idStrategy;
    }

    public List<String> primaryKeys() {
        return this.primaryKeys;
    }

    public VertexLabel primaryKeys(String... keys) {
        if (keys.length == 0) {
            return this;
        }
        E.checkArgument(this.idStrategy == IdStrategy.DEFAULT ||
                        this.idStrategy == IdStrategy.PRIMARY_KEY,
                        "Not allowed to use id strategy '%s' and call " +
                        "primaryKeys() at the same time for vertex label '%s'",
                        this.idStrategy, this.name);

        this.idStrategy = IdStrategy.PRIMARY_KEY;

        for (String key : keys) {
            if (!this.primaryKeys.contains(key)) {
                this.primaryKeys.add(key);
            }
        }
        return this;
    }

    @Override
    public VertexLabel indexNames(String... names) {
        this.indexNames.addAll(Arrays.asList(names));
        return this;
    }

    public VertexLabel properties(String... propertyNames) {
        this.properties.addAll(Arrays.asList(propertyNames));
        return this;
    }

    @Override
    public String schema() {
        StringBuilder sb = new StringBuilder();
        sb.append("schema.vertexLabel(\"").append(this.name).append("\")");
        sb.append(this.idStrategy.schema());
        sb.append(this.propertiesSchema());
        sb.append(this.primaryKeysSchema());
        sb.append(".ifNotExist()");
        sb.append(".create();");
        return sb.toString();
    }

    private String primaryKeysSchema() {
        return StringUtil.desc("primaryKeys", this.primaryKeys);
    }

    public static class Builder implements VertexLabelBuilder {

        private VertexLabel vertexLabel;
        private SchemaTransaction transaction;

        public Builder(String name, SchemaTransaction transaction) {
            this(new VertexLabel(name), transaction);
        }

        public Builder(VertexLabel vertexLabel,
                       SchemaTransaction transaction) {
            E.checkNotNull(vertexLabel, "vertexLabel");
            E.checkNotNull(transaction, "transaction");
            this.vertexLabel = vertexLabel;
            this.transaction = transaction;
        }

        @Override
        public VertexLabel create() {
            String name = this.vertexLabel.name();
            HugeConfig config = this.transaction.graph().configuration();
            checkName(name, config.get(CoreOptions.SCHEMA_ILLEGAL_NAME_REGEX));

            VertexLabel vertexLabel = this.transaction.getVertexLabel(name);
            if (vertexLabel != null) {
                if (this.vertexLabel.checkExist) {
                    throw new ExistedException("vertex label", name);
                }
                return vertexLabel;
            }

            this.checkProperties();
            this.checkIdStrategy();

            this.transaction.addVertexLabel(this.vertexLabel);
            return this.vertexLabel;
        }

        @Override
        public VertexLabel append() {
            String name = this.vertexLabel.name();
            // Don't allow user to modify some stable properties.
            this.checkStableVars();
            this.checkProperties();

            VertexLabel vertexLabel = this.transaction.getVertexLabel(name);
            if (vertexLabel == null) {
                throw new HugeException("Can't append the vertex label '%s' " +
                                        "since it doesn't exist", name);
            }

            vertexLabel.properties().addAll(this.vertexLabel.properties);

            this.transaction.addVertexLabel(vertexLabel);
            return this.vertexLabel;
        }

        @Override
        public VertexLabel eliminate() {
            throw new NotSupportException("action eliminate on vertex label");
        }

        @Override
        public void remove() {
            this.transaction.removeVertexLabel(this.vertexLabel.name());
        }

        public void rebuildIndex() {
            this.transaction.rebuildIndex(this.vertexLabel);
        }

        @Override
        public Builder useAutomaticId() {
            this.vertexLabel.idStrategy(IdStrategy.AUTOMATIC);
            return this;
        }

        @Override
        public Builder useCustomizeId() {
            this.vertexLabel.idStrategy(IdStrategy.CUSTOMIZE);
            return this;
        }

        @Override
        public Builder usePrimaryKeyId() {
            this.vertexLabel.idStrategy(IdStrategy.PRIMARY_KEY);
            return this;
        }

        @Override
        public Builder properties(String... propertyNames) {
            this.vertexLabel.properties(propertyNames);
            return this;
        }

        @Override
        public Builder primaryKeys(String... keys) {
            this.vertexLabel.primaryKeys(keys);
            return this;
        }

        public Builder ifNotExist() {
            this.vertexLabel.checkExist = false;
            return this;
        }

        private void checkProperties() {
            String name = this.vertexLabel.name();
            Set<String> properties = this.vertexLabel.properties();
            // If properties is not empty, check all property.
            for (String pk : properties) {
                E.checkArgumentNotNull(this.transaction.getPropertyKey(pk),
                                       "Undefined property key '%s' in the " +
                                       "vertex label '%s'", pk, name);
            }
        }

        private void checkIdStrategy() {
            IdStrategy strategy = this.vertexLabel.idStrategy;
            boolean hasPrimaryKey = this.vertexLabel.primaryKeys().size() > 0;
            switch (strategy) {
                case DEFAULT:
                    if (hasPrimaryKey) {
                        this.vertexLabel.idStrategy(IdStrategy.PRIMARY_KEY);
                    } else {
                        this.vertexLabel.idStrategy(IdStrategy.AUTOMATIC);
                    }
                    break;
                case AUTOMATIC:
                case CUSTOMIZE:
                    E.checkArgument(!hasPrimaryKey,
                                    "Not allowed to pass primary keys " +
                                    "when using '%s' id strategy", strategy);
                    break;
                case PRIMARY_KEY:
                    E.checkArgument(hasPrimaryKey,
                                    "Must pass primary keys when " +
                                    "using '%s' id strategy", strategy);
                    break;
                default:
                    throw new AssertionError(String.format(
                              "Unknown id strategy '%s'", strategy));
            }
            if (this.vertexLabel.idStrategy == IdStrategy.PRIMARY_KEY) {
                this.checkPrimaryKeys();
            }
        }

        private void checkPrimaryKeys() {
            String name = this.vertexLabel.name();

            Set<String> properties = this.vertexLabel.properties();
            E.checkArgument(!properties.isEmpty(),
                            "The properties of vertex label '%s' " +
                            "can't be empty whose id strategy is '%s'",
                            name, IdStrategy.PRIMARY_KEY);

            List<String> primaryKeys = this.vertexLabel.primaryKeys();
            E.checkNotEmpty(primaryKeys, "primary keys", name);

            // Use loop instead containAll for more detailed exception info.
            for (String key : primaryKeys) {
                E.checkArgument(properties.contains(key),
                                "The primary key '%s' of vertex label '%s' " +
                                "must be contained in properties: %s",
                                key, name, properties);
            }
        }

        private void checkStableVars() {
            String name = this.vertexLabel.name();
            List<String> primaryKeys = this.vertexLabel.primaryKeys;
            IdStrategy idStrategy = this.vertexLabel.idStrategy;

            // Don't allow to append sort keys.
            if (!primaryKeys.isEmpty()) {
                throw new NotAllowException(
                          "Not allowed to append primary keys " +
                          "for existed vertex label '%s'", name);
            }
            if (idStrategy != IdStrategy.DEFAULT) {
                throw new NotAllowException(
                          "Not allowed to change id strategy " +
                          "for existed vertex label '%s'", name);
            }
        }
    }
}
