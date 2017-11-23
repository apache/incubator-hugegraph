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
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.query.ConditionQuery;
import com.baidu.hugegraph.backend.tx.SchemaTransaction;
import com.baidu.hugegraph.config.CoreOptions;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.exception.ExistedException;
import com.baidu.hugegraph.exception.NotSupportException;
import com.baidu.hugegraph.schema.builder.IndexLabelBuilder;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.define.Cardinality;
import com.baidu.hugegraph.type.define.HugeKeys;
import com.baidu.hugegraph.type.define.IndexType;
import com.baidu.hugegraph.util.CollectionUtil;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.NumericUtil;

public class IndexLabel extends SchemaElement {

    private HugeType baseType;
    private String baseValue;
    private IndexType indexType;
    private List<String> indexFields;

    public IndexLabel(String name) {
        this(name, null, null);
    }

    public IndexLabel(String name, HugeType baseType, String baseValue) {
        super(name);
        this.baseType = baseType;
        this.baseValue = baseValue;
        this.indexType = IndexType.SECONDARY;
        this.indexFields = new ArrayList<>();
    }

    @Override
    public HugeType type() {
        return HugeType.INDEX_LABEL;
    }

    public HugeType baseType() {
        return this.baseType;
    }

    public void baseType(HugeType baseType) {
        this.baseType = baseType;
    }

    public String baseValue() {
        return this.baseValue;
    }

    public void baseValue(String baseValue) {
        this.baseValue = baseValue;
    }

    public IndexType indexType() {
        return this.indexType;
    }

    public void indexType(IndexType indexType) {
        this.indexType = indexType;
    }

    public HugeType queryType() {
        switch (this.baseType) {
            case VERTEX_LABEL:
                return HugeType.VERTEX;
            case EDGE_LABEL:
                return HugeType.EDGE;
            default:
                throw new AssertionError(String.format(
                          "Query type of index label is either '%s' or '%s', " +
                          "but '%s' is used",
                          HugeType.VERTEX_LABEL,
                          HugeType.EDGE_LABEL,
                          this.baseType));
        }
    }

    public List<String> indexFields() {
        return Collections.unmodifiableList(this.indexFields);
    }

    public void indexFields(String... fields) {
        if (fields.length == 0) {
            return;
        }
        E.checkArgument(this.indexFields.isEmpty(),
                        "Not allowed to assign index fields multitimes");
        List<String> indexFields = Arrays.asList(fields);
        E.checkArgument(CollectionUtil.allUnique(indexFields),
                        "Invalid index fields %s, which contains some " +
                        "duplicate properties", indexFields);
        this.indexFields.addAll(indexFields);
    }

    @Override
    public String schema() {
        StringBuilder sb = new StringBuilder();
        sb.append("schema.indexLabel(\"").append(this.name).append("\")");
        sb.append(this.baseLabelSchema());
        sb.append(this.indexFieldsSchema());
        sb.append(this.indexType.schema());
        sb.append(".ifNotExist()");
        sb.append(".create();");
        return sb.toString();
    }

    // TODO: Print the element name instead of object may lead custom confused.
    private String baseLabelSchema() {
        if (this.baseType == HugeType.VERTEX_LABEL) {
            return String.format(".onV(\"%s\")", this.baseValue);
        } else {
            assert this.baseType == HugeType.EDGE_LABEL;
            return String.format(".onE(\"%s\")", this.baseValue);
        }
    }

    private String indexFieldsSchema() {
        StringBuilder sb = new StringBuilder();
        for (String indexField : this.indexFields) {
            sb.append("\"").append(indexField).append("\",");
        }
        int endIdx = sb.lastIndexOf(",") > 0 ? sb.length() - 1 : sb.length();
        return String.format(".by(%s)", sb.substring(0, endIdx));
    }

    static class PrimitiveIndexLabel extends IndexLabel {

        public PrimitiveIndexLabel(String name) {
            super(name);
            // TODO: add indexFields and id(from -1)
        }

        @Override
        public boolean primitive() {
            return true;
        }
    }

    public static final IndexLabel VL_IL = new PrimitiveIndexLabel("~vli");
    public static final IndexLabel EL_IL = new PrimitiveIndexLabel("~eli");

    public static IndexLabel label(HugeType type) {
        if (type == HugeType.VERTEX) {
            return VL_IL;
        } else if (type == HugeType.EDGE || // TODO: just EDGE when separate e-p
                   type == HugeType.EDGE_OUT || type == HugeType.EDGE_IN) {
            return EL_IL;
        }
        throw new AssertionError("No index label for " + type);
    }

    public static IndexLabel indexLabel(HugeGraph graph, String il) {
        // Primitive IndexLabel first
        if (VL_IL.name().equals(il)) {
            return VL_IL;
        }
        if (EL_IL.name().equals(il)) {
            return EL_IL;
        }

        return graph.indexLabel(il);
    }

    public static class Builder implements IndexLabelBuilder {

        private IndexLabel indexLabel;
        private SchemaTransaction transaction;

        public Builder(String name, SchemaTransaction transaction) {
            this(new IndexLabel(name), transaction);
        }

        public Builder(IndexLabel indexLabel, SchemaTransaction transaction) {
            E.checkNotNull(indexLabel, "indexLabel");
            E.checkNotNull(transaction, "transaction");
            this.indexLabel = indexLabel;
            this.transaction = transaction;
        }

        @Override
        public IndexLabel create() {
            String name = this.indexLabel.name();
            HugeConfig config = this.transaction.graph().configuration();
            checkName(name, config.get(CoreOptions.SCHEMA_ILLEGAL_NAME_REGEX));

            IndexLabel indexLabel = this.transaction.getIndexLabel(name);
            if (indexLabel != null) {
                if (this.indexLabel.checkExist) {
                    throw new ExistedException("index label", name);
                }
                return indexLabel;
            }

            SchemaLabel schemaLabel = this.loadElement();
            E.checkArgumentNotNull(schemaLabel,
                                   "Can't find the %s with name '%s'",
                                   this.indexLabel.baseType,
                                   this.indexLabel.baseValue);

            this.checkFields(schemaLabel.properties);

            /*
             * If new index label is prefix of existed index label, or has
             * the same fields, fail to create new index label.
             */
            this.checkRepeatIndex(schemaLabel);

            // Delete index label which is prefix of the new index label
            // TODO: use event to replace direct call
            this.removeSubIndex(schemaLabel);

            // TODO: should wrap update and add operation in one transaction.
            this.updateSchemaIndexName(schemaLabel);

            this.transaction.addIndexLabel(this.indexLabel);

            // TODO: use event to replace direct call
            this.rebuildIndexIfNeeded();

            return this.indexLabel;
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
            this.transaction.removeIndexLabel(this.indexLabel.name);
        }

        public void rebuildIndexIfNeeded() {
            if (this.indexLabel.baseType() == HugeType.VERTEX_LABEL) {
                ConditionQuery query = new ConditionQuery(HugeType.VERTEX);
                query.eq(HugeKeys.LABEL, this.indexLabel.baseValue);
                query.limit(1L);
                if (this.transaction.graph().graphTransaction()
                        .queryVertices(query).iterator().hasNext()) {
                    this.transaction.rebuildIndex(this.indexLabel);
                }
            } else {
                assert this.indexLabel.baseType() == HugeType.EDGE_LABEL;
                ConditionQuery query = new ConditionQuery(HugeType.EDGE);
                query.eq(HugeKeys.LABEL, this.indexLabel.baseValue);
                query.limit(1L);
                if (this.transaction.graph().graphTransaction()
                        .queryEdges(query).iterator().hasNext()) {
                    this.transaction.rebuildIndex(this.indexLabel);
                }
            }
        }

        public void rebuild() {
            this.transaction.rebuildIndex(this.indexLabel);
        }

        @Override
        public Builder onV(String baseValue) {
            this.indexLabel.baseType = HugeType.VERTEX_LABEL;
            this.indexLabel.baseValue = baseValue;
            return this;
        }

        @Override
        public Builder onE(String baseValue) {
            this.indexLabel.baseType = HugeType.EDGE_LABEL;
            this.indexLabel.baseValue = baseValue;
            return this;
        }

        @Override
        public Builder by(String... fields) {
            this.indexLabel.indexFields(fields);
            return this;
        }

        @Override
        public Builder secondary() {
            this.indexLabel.indexType(IndexType.SECONDARY);
            return this;
        }

        @Override
        public Builder search() {
            this.indexLabel.indexType(IndexType.SEARCH);
            return this;
        }

        public Builder ifNotExist() {
            this.indexLabel.checkExist = false;
            return this;
        }

        private SchemaLabel loadElement() {
            HugeType baseType = this.indexLabel.baseType;
            E.checkNotNull(baseType, "base type", "index label");
            String baseValue = this.indexLabel.baseValue;
            E.checkNotNull(baseValue, "base value", "index label");
            switch (baseType) {
                case VERTEX_LABEL:
                    return this.transaction.getVertexLabel(baseValue);
                case EDGE_LABEL:
                    return this.transaction.getEdgeLabel(baseValue);
                default:
                    throw new AssertionError(String.format(
                              "Unsupported base type '%s' of index label '%s'",
                              baseType, this.indexLabel.name));
            }
        }

        private void checkFields(Set<String> properties) {
            List<String> fields = this.indexLabel.indexFields;
            E.checkNotEmpty(fields, "index fields", this.indexLabel.name);

            E.checkArgument(CollectionUtil.containsAll(properties, fields),
                            "Not all index fields '%s' are contained in " +
                            "'%s' properties %s",
                            fields, this.indexLabel.baseValue, properties);

            for (String field : fields) {
                PropertyKey pk = this.transaction.getPropertyKey(field);
                // In general this will not happen
                E.checkArgumentNotNull(pk, "Can't build index on undefined " +
                                       "property key '%s' for '%s': '%s'",
                                       field, this.indexLabel.baseType,
                                       this.indexLabel.baseValue);
                E.checkArgument(pk.cardinality() == Cardinality.SINGLE,
                                "Not allowed to build index on property key " +
                                "'%s' whose cardinality is list or set.",
                                pk.name());
            }

            // Search index must build on single numeric column
            if (this.indexLabel.indexType == IndexType.SEARCH) {
                E.checkArgument(fields.size() == 1,
                                "Search index can only build on " +
                                "one property, but got %s properties: '%s'",
                                fields.size(), fields);
                String field = fields.iterator().next();
                PropertyKey pk = this.transaction.getPropertyKey(field);
                E.checkArgument(NumericUtil.isNumber(pk.dataType().clazz()),
                                "Search index can only build on " +
                                "numeric property, but got %s(%s)",
                                pk.dataType(), pk.name());
            }
        }

        protected void updateSchemaIndexName(SchemaLabel schemaLabel) {
            schemaLabel.indexNames(this.indexLabel.name());
            switch (this.indexLabel.baseType) {
                case VERTEX_LABEL:
                    this.transaction.addVertexLabel((VertexLabel) schemaLabel);
                    break;
                case EDGE_LABEL:
                    this.transaction.addEdgeLabel((EdgeLabel) schemaLabel);
                    break;
                default:
                    throw new AssertionError(String.format(
                              "Can't update index name of schema type: %s",
                              this.indexLabel.baseType));
            }
        }

        protected void checkRepeatIndex(SchemaLabel schemaLabel) {
            for (String indexName : schemaLabel.indexNames) {
                IndexLabel existed = this.transaction.getIndexLabel(indexName);
                IndexLabel newOne = this.indexLabel;
                if (newOne.indexType != existed.indexType) {
                    continue;
                }
                // New created indexLabel can't be prefix of existed indexLabel
                E.checkArgument(!CollectionUtil.prefixOf(newOne.indexFields,
                                                         existed.indexFields),
                                "Fields %s of new index label '%s' is prefix" +
                                " of existed index label '%s'",
                                newOne.indexFields, newOne.name, existed.name);
            }
        }

        protected void removeSubIndex(SchemaLabel schemaLabel) {
            HashSet<String> overrideIndexLabels = new HashSet<>();
            for (String indexName : schemaLabel.indexNames) {
                IndexLabel existed = this.transaction.getIndexLabel(indexName);
                IndexLabel newOne = this.indexLabel;
                if (newOne.indexType != existed.indexType) {
                    continue;
                }
                /*
                 * If existed indexLabel is prefix of new created indexLabel,
                 * remove the existed indexLabel.
                 */
                if (CollectionUtil.prefixOf(existed.indexFields,
                                            newOne.indexFields)) {
                    overrideIndexLabels.add(indexName);
                }
            }
            for (String indexName : overrideIndexLabels) {
                schemaLabel.removeIndexName(indexName);
                this.transaction.removeIndexLabel(indexName);
            }
        }
    }
}
