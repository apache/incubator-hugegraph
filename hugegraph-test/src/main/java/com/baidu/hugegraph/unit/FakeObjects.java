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

package com.baidu.hugegraph.unit;

import java.util.Collections;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.mockito.Mockito;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.schema.EdgeLabel;
import com.baidu.hugegraph.schema.IndexLabel;
import com.baidu.hugegraph.schema.PropertyKey;
import com.baidu.hugegraph.schema.VertexLabel;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.define.Cardinality;
import com.baidu.hugegraph.type.define.DataType;
import com.baidu.hugegraph.type.define.Frequency;
import com.baidu.hugegraph.type.define.IdStrategy;
import com.baidu.hugegraph.type.define.IndexType;

public final class FakeObjects {

    private final HugeGraph graph;

    public FakeObjects() {
        this.graph = Mockito.mock(HugeGraph.class);
        Mockito.doReturn(newConfig()).when(this.graph).configuration();
    }

    public FakeObjects(String name) {
        this();
        Mockito.doReturn(name).when(this.graph).name();
    }

    public static HugeConfig newConfig() {
        Configuration conf = Mockito.mock(PropertiesConfiguration.class);
        Mockito.when(conf.getKeys()).thenReturn(Collections.emptyIterator());
        return new HugeConfig(conf);
    }

    public HugeGraph graph() {
        return this.graph;
    }

    public PropertyKey newPropertyKey(Id id, String name) {
        return newPropertyKey(id, name, DataType.TEXT, Cardinality.SINGLE);
    }

    public PropertyKey newPropertyKey(Id id, String name,
                                      DataType dataType) {
        return newPropertyKey(id, name, dataType, Cardinality.SINGLE);
    }

    public PropertyKey newPropertyKey(Id id, String name,
                                      DataType dataType,
                                      Cardinality cardinality) {
        PropertyKey schema = new PropertyKey(this.graph, id, name);
        schema.dataType(dataType);
        schema.cardinality(cardinality);
        return schema;
    }

    public VertexLabel newVertexLabel(Id id, String name,
                                      IdStrategy idStrategy,
                                      Id... properties) {
        VertexLabel schema = new VertexLabel(this.graph, id, name);
        schema.idStrategy(idStrategy);
        schema.properties(properties);
        return schema;
    }

    public EdgeLabel newEdgeLabel(Id id, String name, Frequency frequency,
                                  Id sourceLabel, Id targetLabel,
                                  Id... properties) {
        EdgeLabel schema = new EdgeLabel(this.graph, id, name);
        schema.frequency(frequency);
        schema.sourceLabel(sourceLabel);
        schema.targetLabel(targetLabel);
        schema.properties(properties);
        return schema;
    }

    public IndexLabel newIndexLabel(Id id, String name, HugeType baseType,
                                    Id baseValue, IndexType indexType,
                                    Id... fields) {
        IndexLabel schema = new IndexLabel(this.graph, id, name);
        schema.baseType(baseType);
        schema.baseValue(baseValue);
        schema.indexType(indexType);
        schema.indexFields(fields);
        return schema;
    }
}
