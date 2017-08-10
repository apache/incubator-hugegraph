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

package com.baidu.hugegraph.schema;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import com.baidu.hugegraph.HugeException;
import com.baidu.hugegraph.backend.tx.SchemaTransaction;
import com.baidu.hugegraph.exception.ExistedException;
import com.baidu.hugegraph.exception.NotAllowException;
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
        this.idStrategy = IdStrategy.DAFAULT;
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
        E.checkArgument(this.idStrategy == IdStrategy.DAFAULT ||
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
        E.checkArgument(this.idStrategy == IdStrategy.DAFAULT ||
                        this.idStrategy == IdStrategy.PRIMARY_KEY,
                        "Not allowed to use id strategy '%s' and call " +
                        "method 'primaryKeys(...)' at the same time for " +
                        "vertex label '%s'", this.idStrategy, this.name);

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
            this.vertexLabel = new VertexLabel(name);
            this.transaction = transaction;
        }

        public Builder(VertexLabel vertexLabel, SchemaTransaction transaction) {
            this.vertexLabel = vertexLabel;
            this.transaction = transaction;
        }

        public VertexLabel create() {
            String name = this.vertexLabel.name();

            StringUtil.checkName(name);
            // Try to read
            VertexLabel vertexLabel = this.transaction.getVertexLabel(name);
            // if vertexLabel exist and checkExist
            if (vertexLabel != null) {
                if (this.vertexLabel.checkExist) {
                    throw new ExistedException("vertex label", name);
                } else {
                    return vertexLabel;
                }
            }

            this.checkProperties();
            this.checkIdStrategy();

            this.transaction.addVertexLabel(this.vertexLabel);
            return this.vertexLabel;
        }

        public VertexLabel append() {
            String name = this.vertexLabel.name();

            StringUtil.checkName(name);
            // Don't allow user to modify some stable properties.
            this.checkStableVars();

            this.checkProperties();

            // Try to read
            VertexLabel vertexLabel = this.transaction.getVertexLabel(name);
            if (vertexLabel == null) {
                throw new HugeException("Can't append the vertex label '%s' " +
                                        "since it doesn't exist", name);
            }

            vertexLabel.properties().addAll(this.vertexLabel.properties);

            this.transaction.addVertexLabel(vertexLabel);
            return this.vertexLabel;
        }

        public VertexLabel eliminate() {
            throw new HugeException("Not support eliminate action on " +
                                    "vertex label");
        }

        public void remove() {
            this.transaction.removeVertexLabel(this.vertexLabel.name());
        }

        public void rebuildIndex() {
            this.transaction.rebuildIndex(this.vertexLabel);
        }

        public Builder useAutomaticId() {
            this.vertexLabel.idStrategy(IdStrategy.AUTOMATIC);
            return this;
        }

        public Builder useCustomizeId() {
            this.vertexLabel.idStrategy(IdStrategy.CUSTOMIZE);
            return this;
        }

        public Builder usePrimaryKeyId() {
            this.vertexLabel.idStrategy(IdStrategy.PRIMARY_KEY);
            return this;
        }

        public Builder properties(String... propertyNames) {
            this.vertexLabel.properties(propertyNames);
            return this;
        }

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

            E.checkNotNull(properties, "properties", name);
            E.checkNotEmpty(properties, "properties", name);
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
                case DAFAULT:
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
            List<String> primaryKeys = this.vertexLabel.primaryKeys();
            Set<String> properties = this.vertexLabel.properties();

            E.checkNotNull(primaryKeys, "primary keys");
            E.checkNotEmpty(primaryKeys, "primary keys");

            // Use loop instead containAll for more detailed exception info.
            for (String key : primaryKeys) {
                E.checkArgument(properties.contains(key),
                                "The primary key '%s' of vertex label '%s' " +
                                "must be contained in properties", key, name);
            }
        }

        private void checkStableVars() {
            String name = this.vertexLabel.name();
            List<String> primaryKeys = this.vertexLabel.primaryKeys();
            Set<String> indexNames = this.vertexLabel.indexNames();

            // Don't allow to append sort keys.
            if (!primaryKeys.isEmpty()) {
                throw new NotAllowException("Not allowed to append primary " +
                                            "keys for existed vertex label " +
                                            "'%s'", name);
            }
            if (!indexNames.isEmpty()) {
                throw new NotAllowException("Not allowed to append indexes " +
                                            "for existed vertex label '%s'",
                                            name);
            }
        }
    }
}
