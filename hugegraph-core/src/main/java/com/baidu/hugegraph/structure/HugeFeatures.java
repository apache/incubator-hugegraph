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

package com.baidu.hugegraph.structure;

import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.FeatureDescriptor;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.config.CoreOptions;
import com.baidu.hugegraph.config.HugeConfig;


public class HugeFeatures implements Graph.Features {

    protected final HugeGraph graph;
    protected final boolean supportsPersistence;
    protected final HugeGraphFeatures graphFeatures;
    protected final HugeVertexFeatures vertexFeatures;
    protected final HugeEdgeFeatures edgeFeatures;

    public HugeFeatures(HugeGraph graph, boolean supportsPersistence) {
        this.graph = graph;
        this.supportsPersistence = supportsPersistence;

        this.graphFeatures = new HugeGraphFeatures();
        this.vertexFeatures = new HugeVertexFeatures();
        this.edgeFeatures = new HugeEdgeFeatures();
    }

    @Override
    public HugeGraphFeatures graph() {
        return this.graphFeatures;
    }

    @Override
    public HugeVertexFeatures vertex() {
        return this.vertexFeatures;
    }

    @Override
    public HugeEdgeFeatures edge() {
        return this.edgeFeatures;
    }

    @Override
    public String toString() {
        return StringFactory.featureString(this);
    }

    public class HugeGraphFeatures implements GraphFeatures {

        private final VariableFeatures variableFeatures =
                new HugeVariableFeatures();

        @Override
        public boolean supportsConcurrentAccess() {
            return false;
        }

        @Override
        public boolean supportsComputer() {
            return false;
        }

        @Override
        public boolean supportsPersistence() {
            return HugeFeatures.this.supportsPersistence;
        }

        @Override
        public VariableFeatures variables() {
            return this.variableFeatures;
        }

        @Override
        public boolean supportsTransactions() {
            return true;
        }

        @Override
        public boolean supportsThreadedTransactions() {
            return false;
        }
    }

    public class HugeElementFeatures implements ElementFeatures {

        @Override
        public boolean supportsNumericIds() {
            return false;
        }

        @Override
        public boolean supportsStringIds() {
            return true;
        }

        @Override
        public boolean supportsUuidIds() {
            return false;
        }

        @Override
        public boolean supportsAnyIds() {
            return false;
        }

        @Override
        public boolean supportsCustomIds() {
            return false;
        }

        @Override
        public boolean supportsRemoveProperty() {
            return true;
        }
    }

    public class HugeVariableFeatures implements VariableFeatures {

        @Override
        public boolean supportsVariables() {
            return false;
        }

        @Override
        public boolean supportsBooleanValues() {
            return false;
        }

        @Override
        public boolean supportsDoubleValues() {
            return false;
        }

        @Override
        public boolean supportsFloatValues() {
            return false;
        }

        @Override
        public boolean supportsIntegerValues() {
            return false;
        }

        @Override
        public boolean supportsLongValues() {
            return false;
        }

        @Override
        public boolean supportsMapValues() {
            return false;
        }

        @Override
        public boolean supportsMixedListValues() {
            return false;
        }

        @Override
        public boolean supportsByteValues() {
            return false;
        }

        @Override
        public boolean supportsBooleanArrayValues() {
            return false;
        }

        @Override
        public boolean supportsByteArrayValues() {
            return false;
        }

        @Override
        public boolean supportsDoubleArrayValues() {
            return false;
        }

        @Override
        public boolean supportsFloatArrayValues() {
            return false;
        }

        @Override
        public boolean supportsIntegerArrayValues() {
            return false;
        }

        @Override
        public boolean supportsLongArrayValues() {
            return false;
        }

        @Override
        public boolean supportsStringArrayValues() {
            return false;
        }

        @Override
        public boolean supportsSerializableValues() {
            return false;
        }

        @Override
        public boolean supportsStringValues() {
            return false;
        }

        @Override
        public boolean supportsUniformListValues() {
            return false;
        }
    }

    public class HugeVertexPropertyFeatures extends HugeDataTypeFeatures
                                            implements VertexPropertyFeatures {

        @Override
        public boolean supportsAddProperty() {
            return true;
        }

        @Override
        public boolean supportsRemoveProperty() {
            return true;
        }

        @Override
        public boolean supportsMapValues() {
            return false;
        }

        @Override
        public boolean supportsMixedListValues() {
            return false;
        }

        @Override
        public boolean supportsSerializableValues() {
            return false;
        }

        @Override
        public boolean supportsUniformListValues() {
            return true;
        }

        @Override
        public boolean supportsUserSuppliedIds() {
            return false;
        }

        @Override
        public boolean supportsAnyIds() {
            return false;
        }

        @Override
        public boolean supportsCustomIds() {
            return false;
        }
    }

    public class HugeEdgePropertyFeatures extends HugeDataTypeFeatures
                                          implements EdgePropertyFeatures {

        @Override
        public boolean supportsMapValues() {
            return false;
        }

        @Override
        public boolean supportsMixedListValues() {
            return false;
        }

        @Override
        public boolean supportsSerializableValues() {
            return false;
        }

        @Override
        public boolean supportsUniformListValues() {
            return true;
        }

    }

    public class HugeVertexFeatures extends HugeElementFeatures
                                    implements VertexFeatures {

        private final VertexPropertyFeatures vertexPropertyFeatures =
                                             new HugeVertexPropertyFeatures();

        @Override
        public VertexPropertyFeatures properties() {
            return this.vertexPropertyFeatures;
        }

        @Override
        public boolean supportsUserSuppliedIds() {
            return true;
        }

        @Override
        public boolean supportsMultiProperties() {
            // Regard as a set (actually can also be a list)
            return true;
        }

        @Override
        public boolean supportsDuplicateMultiProperties() {
            // Regard as a list
            return true;
        }

        @Override
        public boolean supportsMetaProperties() {
            // Nested property
            return false;
        }

        @Override
        public VertexProperty.Cardinality getCardinality(final String key) {
            return VertexProperty.Cardinality.single;
        }

        public boolean supportsDefaultLabel() {
            return true;
        }

        public String defaultLabel() {
            Configuration conf = HugeFeatures.this.graph.configuration();
            return ((HugeConfig) conf).get(CoreOptions.DEFAULT_VERTEX_LABEL);
        }
    }

    public class HugeEdgeFeatures extends HugeElementFeatures
                                  implements EdgeFeatures {

        private final EdgePropertyFeatures edgePropertyFeatures =
                                           new HugeEdgePropertyFeatures();

        @Override
        public boolean supportsUserSuppliedIds() {
            return false;
        }

        @Override
        public EdgePropertyFeatures properties() {
            return this.edgePropertyFeatures;
        }
    }

    public class HugeDataTypeFeatures implements DataTypeFeatures {

        @Override
        @FeatureDescriptor(name = FEATURE_STRING_VALUES)
        public boolean supportsStringValues() {
            return true;
        }

        @Override
        @FeatureDescriptor(name = FEATURE_BOOLEAN_VALUES)
        public boolean supportsBooleanValues() {
            return true;
        }

        @Override
        @FeatureDescriptor(name = FEATURE_BYTE_VALUES)
        public boolean supportsByteValues() {
            return true;
        }

        @Override
        @FeatureDescriptor(name = FEATURE_FLOAT_VALUES)
        public boolean supportsFloatValues() {
            return true;
        }

        @Override
        @FeatureDescriptor(name = FEATURE_DOUBLE_VALUES)
        public boolean supportsDoubleValues() {
            return true;
        }

        @Override
        @FeatureDescriptor(name = FEATURE_INTEGER_VALUES)
        public boolean supportsIntegerValues() {
            return true;
        }

        @Override
        @FeatureDescriptor(name = FEATURE_LONG_VALUES)
        public boolean supportsLongValues() {
            return true;
        }

        @Override
        @FeatureDescriptor(name = FEATURE_UNIFORM_LIST_VALUES)
        public boolean supportsUniformListValues() {
            /*
             * NOTE: must use cardinality list if use LIST property value,
             * can't support a LIST property value with cardinality single
             */
            return true;
        }

        @Override
        @FeatureDescriptor(name = FEATURE_MIXED_LIST_VALUES)
        public boolean supportsMixedListValues() {
            return false;
        }

        @Override
        @FeatureDescriptor(name = FEATURE_MAP_VALUES)
        public boolean supportsMapValues() {
            return false;
        }

        @Override
        @FeatureDescriptor(name = FEATURE_SERIALIZABLE_VALUES)
        public boolean supportsSerializableValues() {
            return false;
        }

        /**
         * All these supportsXxArrayValues() must be used with cardinality list
         * we can't support array values with cardinality single like tinkerpop
         */
        @Override
        @FeatureDescriptor(name = FEATURE_BYTE_ARRAY_VALUES)
        public boolean supportsByteArrayValues() {
            // Regard as blob
            return true;
        }

        @Override
        @FeatureDescriptor(name = FEATURE_BOOLEAN_ARRAY_VALUES)
        public boolean supportsBooleanArrayValues() {
            return false;
        }

        @Override
        @FeatureDescriptor(name = FEATURE_FLOAT_ARRAY_VALUES)
        public boolean supportsFloatArrayValues() {
            return false;
        }

        @Override
        @FeatureDescriptor(name = FEATURE_DOUBLE_ARRAY_VALUES)
        public boolean supportsDoubleArrayValues() {
            return false;
        }

        @Override
        @FeatureDescriptor(name = FEATURE_INTEGER_ARRAY_VALUES)
        public boolean supportsIntegerArrayValues() {
            return false;
        }

        @Override
        @FeatureDescriptor(name = FEATURE_LONG_ARRAY_VALUES)
        public boolean supportsLongArrayValues() {
            return false;
        }

        @Override
        @FeatureDescriptor(name = FEATURE_STRING_ARRAY_VALUES)
        public boolean supportsStringArrayValues() {
            return false;
        }
    }
}
