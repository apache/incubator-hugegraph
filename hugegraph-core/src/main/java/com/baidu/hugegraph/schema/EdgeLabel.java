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
import com.baidu.hugegraph.schema.builder.EdgeLabelBuilder;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.define.Frequency;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.StringUtil;


public class EdgeLabel extends SchemaLabel {

    private String sourceLabel;
    private String targetLabel;
    private Frequency frequency;
    private List<String> sortKeys;

    public EdgeLabel(String name) {
        super(name);
        this.sourceLabel = null;
        this.targetLabel = null;
        this.frequency = Frequency.SINGLE;
        this.sortKeys = new ArrayList<>();
    }

    @Override
    public HugeType type() {
        return HugeType.EDGE_LABEL;
    }

    @Override
    public EdgeLabel indexNames(String... names) {
        this.indexNames.addAll(Arrays.asList(names));
        return this;
    }

    public Frequency frequency() {
        return this.frequency;
    }

    public void frequency(Frequency frequency) {
        this.frequency = frequency;
    }

    public boolean isDirected() {
        // TODO: implement (do we need this method?)
        return true;
    }

    public String sourceLabel() {
        return this.sourceLabel;
    }

    public EdgeLabel sourceLabel(String label) {
        E.checkArgument(this.sourceLabel == null,
                        "Not allowed to set source label multi times " +
                        "of edge label '%s'", this.name);
        this.sourceLabel = label;
        return this;
    }

    public String targetLabel() {
        return this.targetLabel;
    }

    public EdgeLabel targetLabel(String label) {
        E.checkArgument(this.targetLabel == null,
                        "Not allowed to set target label multi times " +
                        "of edge label '%s'", this.name);
        this.targetLabel = label;
        return this;
    }

    public boolean linkWithLabel(String name) {
        return this.sourceLabel.equals(name) || this.targetLabel.equals(name);
    }

    public boolean checkLinkEqual(String sourceLabel, String targetLabel) {
        return this.sourceLabel.equals(sourceLabel) &&
               this.targetLabel.equals(targetLabel);
    }

    public List<String> sortKeys() {
        return this.sortKeys;
    }

    public EdgeLabel sortKeys(String... keys) {
        for (String key : keys) {
            if (!this.sortKeys.contains(key)) {
                this.sortKeys.add(key);
            }
        }
        return this;
    }

    public EdgeLabel properties(String... propertyNames) {
        this.properties.addAll(Arrays.asList(propertyNames));
        return this;
    }

    @Override
    public String schema() {
        StringBuilder sb = new StringBuilder();
        sb.append("schema.edgeLabel(\"").append(this.name).append("\")");
        sb.append(".sourceLabel(\"").append(this.sourceLabel).append("\")");
        sb.append(".targetLabel(\"").append(this.targetLabel).append("\")");
        sb.append(this.frequency.schema());
        sb.append(this.propertiesSchema());
        sb.append(this.sortKeysSchema());
        sb.append(".ifNotExist()");
        sb.append(".create();");
        return sb.toString();
    }

    private String sortKeysSchema() {
        return StringUtil.desc("sortKeys", this.sortKeys);
    }

    public static class Builder implements EdgeLabelBuilder {

        private EdgeLabel edgeLabel;
        private SchemaTransaction transaction;

        public Builder(String name, SchemaTransaction transaction) {
            this.edgeLabel = new EdgeLabel(name);
            this.transaction = transaction;
        }

        public Builder(EdgeLabel edgeLabel, SchemaTransaction transaction) {
            this.edgeLabel = edgeLabel;
            this.transaction = transaction;
        }

        public EdgeLabel create() {
            String name = this.edgeLabel.name();

            StringUtil.checkName(name);
            // Try to read
            EdgeLabel edgeLabel = this.transaction.getEdgeLabel(name);
            // If edgeLabel exist and checkExist
            if (edgeLabel != null) {
                if (this.edgeLabel.checkExist) {
                    throw new ExistedException("edge label", name);
                } else {
                    return edgeLabel;
                }
            }

            this.checkLinks();
            this.checkProperties();
            this.checkSortKeys();

            this.transaction.addEdgeLabel(this.edgeLabel);
            return this.edgeLabel;
        }

        public EdgeLabel append() {
            String name = this.edgeLabel.name();

            StringUtil.checkName(name);
            // Don't allow user to modify some stable properties.
            this.checkStableVars();
            this.checkProperties();

            // Try to read
            EdgeLabel edgeLabel = this.transaction.getEdgeLabel(name);
            if (edgeLabel == null) {
                throw new HugeException("Can't append the edge label '%s' " +
                                        "since it doesn't exist", name);
            }

            this.checkFrequency(edgeLabel.frequency());

            edgeLabel.properties().addAll(this.edgeLabel.properties);

            this.transaction.addEdgeLabel(edgeLabel);
            return this.edgeLabel;
        }

        public EdgeLabel eliminate() {
            throw new HugeException("Not support eliminate action on " +
                                    "edge label");
        }

        public void remove() {
            this.transaction.removeEdgeLabel(this.edgeLabel.name());
        }

        public void rebuildIndex() {
            this.transaction.rebuildIndex(this.edgeLabel);
        }

        public Builder properties(String... propertyNames) {
            this.edgeLabel.properties(propertyNames);
            return this;
        }

        public Builder sortKeys(String... keys) {
            this.edgeLabel.sortKeys(keys);
            return this;
        }

        public Builder indexNames(String... indexNames) {
            this.edgeLabel.indexNames(indexNames);
            return this;
        }

        public Builder link(String sourceLabel, String targetLabel) {
            this.edgeLabel.sourceLabel(sourceLabel);
            this.edgeLabel.targetLabel(targetLabel);
            return this;
        }

        public Builder sourceLabel(String label) {
            this.edgeLabel.sourceLabel(label);
            return this;
        }

        public Builder targetLabel(String label) {
            this.edgeLabel.targetLabel(label);
            return this;
        }

        public Builder singleTime() {
            this.edgeLabel.frequency(Frequency.SINGLE);
            return this;
        }

        public Builder multiTimes() {
            this.edgeLabel.frequency(Frequency.MULTIPLE);
            return this;
        }

        public Builder ifNotExist() {
            this.edgeLabel.checkExist = false;
            return this;
        }

        private void checkProperties() {
            String name = this.edgeLabel.name();
            Set<String> properties = this.edgeLabel.properties();

            E.checkNotNull(properties, "properties", name);
            // The properties of edge label allowded be empty.
            // If properties is not empty, check all property.
            for (String pk : properties) {
                E.checkArgumentNotNull(
                        this.transaction.getPropertyKey(pk),
                        "Undefined property key '%s' in the edge label '%s'",
                        pk, name);
            }
        }

        private void checkSortKeys() {
            String name = this.edgeLabel.name();
            Set<String> properties = this.edgeLabel.properties();
            List<String> sortKeys = this.edgeLabel.sortKeys();
            Frequency frequency = this.edgeLabel.frequency();

            if (frequency == Frequency.SINGLE) {
                E.checkArgument(sortKeys.isEmpty(),
                                "EdgeLabelBuilder can't contain sortKeys when the " +
                                "cardinality property is single");
            } else {
                E.checkState(sortKeys != null,
                             "The sortKeys can't be null when the " +
                             "cardinality property is multiple");
                E.checkArgument(!sortKeys.isEmpty(),
                                "EdgeLabelBuilder must contain sortKeys when the " +
                                "cardinality property is multiple");
            }

            if (sortKeys != null && !sortKeys.isEmpty()) {
                // Check whether the properties contains the specified keys
                E.checkArgument(!properties.isEmpty(),
                                "The properties can't be empty when exist " +
                                "sort keys for edge label '%s'", name);
                for (String key : sortKeys) {
                    E.checkArgument(properties.contains(key),
                                    "The sort key '%s' must be contained in " +
                                    "properties '%s' for edge label '%s'",
                                    key, name, properties);
                }
            }
        }

        private void checkFrequency(Frequency frequency) {
            // Don't allow to modify frequency.
            if (this.edgeLabel.frequency() != frequency) {
                throw new NotAllowException("Not allowed to modify frequency " +
                                            "for existed edge label '%s'",
                                            this.edgeLabel.name());
            }
        }

        private void checkLinks() {
            String name = this.edgeLabel.name();
            String srcLabel = this.edgeLabel.sourceLabel();
            String tgtLabel = this.edgeLabel.targetLabel();

            E.checkArgument(srcLabel != null && tgtLabel != null,
                            "Must set source and target label for edge " +
                            "label '%s'", name);

            E.checkArgumentNotNull(this.transaction.getVertexLabel(srcLabel),
                                   "Undefined source vertex label '%s' " +
                                   "in edge label '%s'", srcLabel, name);
            E.checkArgumentNotNull(this.transaction.getVertexLabel(tgtLabel),
                                   "Undefined target vertex label '%s' " +
                                   "in edge label '%s'", tgtLabel, name);
        }

        private void checkStableVars() {
            String name = this.edgeLabel.name();
            String sourceLabel = this.edgeLabel.sourceLabel();
            String targetLabel = this.edgeLabel.targetLabel();
            List<String> sortKeys = this.edgeLabel.sortKeys();
            Set<String> indexNames = this.edgeLabel.indexNames();

            if (sourceLabel != null || targetLabel != null) {
                throw new NotAllowException("Not allowd to append source " +
                                            "label or target label for " +
                                            "existed edge label '%s'", name);
            }
            // Don't allow to append sort keys.
            if (!sortKeys.isEmpty()) {
                throw new NotAllowException("Not allowed to append sort " +
                                            "keys for existed edge label " +
                                            "'%s'", name);
            }
            if (!indexNames.isEmpty()) {
                throw new NotAllowException("Not allowed to append indexes " +
                                            "for existed edge label '%s'",
                                            name);
            }
        }
    }
}
