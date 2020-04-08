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
import java.util.Date;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.mockito.Mockito;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.id.EdgeId;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.id.IdGenerator;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.schema.EdgeLabel;
import com.baidu.hugegraph.schema.IndexLabel;
import com.baidu.hugegraph.schema.PropertyKey;
import com.baidu.hugegraph.schema.VertexLabel;
import com.baidu.hugegraph.structure.HugeEdge;
import com.baidu.hugegraph.structure.HugeVertex;
import com.baidu.hugegraph.testutil.Whitebox;
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

        Mockito.when(this.graph.propertyKey(id)).thenReturn(schema);
        return schema;
    }

    public VertexLabel newVertexLabel(Id id, String name,
                                      IdStrategy idStrategy,
                                      Id... properties) {
        VertexLabel schema = new VertexLabel(this.graph, id, name);
        schema.idStrategy(idStrategy);
        schema.properties(properties);

        Mockito.when(this.graph.vertexLabel(id)).thenReturn(schema);
        Mockito.when(this.graph.vertexLabelOrNone(id)).thenReturn(schema);
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

        Mockito.when(this.graph.edgeLabel(id)).thenReturn(schema);
        Mockito.when(this.graph.edgeLabelOrNone(id)).thenReturn(schema);
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

        Mockito.when(this.graph.indexLabel(id)).thenReturn(schema);
        return schema;
    }

    public HugeEdge newEdge(long sourceVertexId, long targetVertexId) {
        PropertyKey name = this.newPropertyKey(IdGenerator.of(1), "name");
        PropertyKey age = this.newPropertyKey(IdGenerator.of(2), "age",
                                              DataType.INT,
                                              Cardinality.SINGLE);
        PropertyKey city = this.newPropertyKey(IdGenerator.of(3), "city");
        PropertyKey date = this.newPropertyKey(IdGenerator.of(4), "date",
                                               DataType.DATE);
        PropertyKey weight = this.newPropertyKey(IdGenerator.of(5),
                                                 "weight", DataType.DOUBLE);

        VertexLabel vl = this.newVertexLabel(IdGenerator.of(1), "person",
                                             IdStrategy.CUSTOMIZE_NUMBER,
                                             name.id(), age.id(), city.id());

        EdgeLabel el = this.newEdgeLabel(IdGenerator.of(1), "knows",
                                         Frequency.SINGLE,  vl.id(), vl.id(),
                                         date.id(), weight.id());

        HugeVertex source = new HugeVertex(this.graph(),
                                           IdGenerator.of(sourceVertexId), vl);
        source.addProperty(name, "tom");
        source.addProperty(age, 18);
        source.addProperty(city, "Beijing");

        HugeVertex target = new HugeVertex(this.graph(),
                                           IdGenerator.of(targetVertexId), vl);
        target.addProperty(name, "cat");
        target.addProperty(age, 20);
        target.addProperty(city, "Shanghai");

        Id id = EdgeId.parse("L123456>1>>L987654");
        HugeEdge edge = new HugeEdge(this.graph(), id, el);

        Whitebox.setInternalState(edge, "sourceVertex", source);
        Whitebox.setInternalState(edge, "targetVertex", target);
        edge.assignId();
        edge.addProperty(date, new Date());
        edge.addProperty(weight, 0.75);

        return edge;
    }
}
