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
import java.util.List;

import com.baidu.hugegraph.HugeException;
import com.baidu.hugegraph.backend.query.ConditionQuery;
import com.baidu.hugegraph.backend.tx.SchemaTransaction;
import com.baidu.hugegraph.exception.ExistedException;
import com.baidu.hugegraph.schema.builder.IndexLabelBuilder;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.define.Cardinality;
import com.baidu.hugegraph.type.define.HugeKeys;
import com.baidu.hugegraph.type.define.IndexType;
import com.baidu.hugegraph.util.CollectionUtil;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.NumericUtil;
import com.baidu.hugegraph.util.StringUtil;


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
        return this.indexFields;
    }

    public void indexFields(String... indexFields) {
        for (String field : indexFields) {
            if (!this.indexFields.contains(field)) {
                this.indexFields.add(field);
            }
        }
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
        return String.format(".on(%s)", this.baseValue);
    }

    private String indexFieldsSchema() {
        StringBuilder sb = new StringBuilder();
        for (String indexField : this.indexFields) {
            sb.append("\"").append(indexField).append("\",");
        }
        int endIdx = sb.lastIndexOf(",") > 0 ? sb.length() - 1 : sb.length();
        return String.format(".by(%s)", sb.substring(0, endIdx));
    }

    public static class Builder implements IndexLabelBuilder {

        private IndexLabel indexLabel;
        private SchemaTransaction transaction;

        public Builder(String name, SchemaTransaction transaction) {
            this.indexLabel = new IndexLabel(name);
            this.transaction = transaction;
        }

        public Builder(IndexLabel indexLabel, SchemaTransaction transaction) {
            this.indexLabel = indexLabel;
            this.transaction = transaction;
        }

        public IndexLabel create() {
            String name = this.indexLabel.name();

            StringUtil.checkName(name);
            IndexLabel indexLabel = this.transaction.getIndexLabel(name);
            if (indexLabel != null) {
                if (this.indexLabel.checkExist) {
                    throw new ExistedException("index label", name);
                } else {
                    return indexLabel;
                }
            }

            // Check field
            this.checkFields();

            SchemaLabel schemaLabel = this.loadElement();
            E.checkArgumentNotNull(schemaLabel,
                                   "Can't build index for %s which is not " +
                                   "existed", this.indexLabel.baseType);

            E.checkArgument(CollectionUtil.containsAll(
                            schemaLabel.properties,
                            this.indexLabel.indexFields),
                            "Not all index fields '%s' " +
                            "are in schema properties '%s'",
                            this.indexLabel.indexFields,
                            schemaLabel.properties);

            // TODO: should wrap update and add operation in one transaction.
            this.updateSchemaIndexName(schemaLabel);

            this.transaction.addIndexLabel(this.indexLabel);

            // TODO: use event to replace direct call
            this.rebuildIndexIfNeeded();

            return this.indexLabel;
        }

        public IndexLabel append() {
            throw new HugeException("Not support append action on index label");
        }

        public IndexLabel eliminate() {
            throw new HugeException("Not support eliminate action on " +
                                    "index label");
        }

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

        public Builder onV(String baseValue) {
            this.indexLabel.baseType = HugeType.VERTEX_LABEL;
            this.indexLabel.baseValue = baseValue;
            return this;
        }

        public Builder onE(String baseValue) {
            this.indexLabel.baseType = HugeType.EDGE_LABEL;
            this.indexLabel.baseValue = baseValue;
            return this;
        }

        public Builder by(String... fields) {
            this.indexLabel.indexFields(fields);
            return this;
        }

        public Builder secondary() {
            this.indexLabel.indexType(IndexType.SECONDARY);
            return this;
        }

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

        private void checkFields() {
            E.checkNotEmpty(this.indexLabel.indexFields,
                            "index fields", "index label");
            // search index must build on single numeric column
            if (this.indexLabel.indexType == IndexType.SEARCH) {
                E.checkArgument(this.indexLabel.indexFields.size() == 1,
                                "Search index can only build on one " +
                                "property, but got %s properties: '%s'",
                                this.indexLabel.indexFields.size(),
                                this.indexLabel.indexFields);
                String field = this.indexLabel.indexFields.iterator().next();
                PropertyKey pk = this.transaction.getPropertyKey(field);
                E.checkArgument(NumericUtil.isNumber(pk.dataType().clazz()),
                                "Search index can only build on numeric " +
                                "property, but got %s(%s)",
                                pk.dataType(), pk.name());
            }

            for (String field : this.indexLabel.indexFields) {
                PropertyKey pk = this.transaction.getPropertyKey(field);
                E.checkArgument(pk.cardinality() == Cardinality.SINGLE,
                                "Not allowed to build index on property " +
                                "key: '%s' that cardinality is list or set.",
                                pk.name());
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
    }

}
