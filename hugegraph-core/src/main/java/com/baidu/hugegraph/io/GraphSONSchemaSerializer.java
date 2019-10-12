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

package com.baidu.hugegraph.io;

import java.util.LinkedHashMap;
import java.util.Map;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.schema.EdgeLabel;
import com.baidu.hugegraph.schema.IndexLabel;
import com.baidu.hugegraph.schema.PropertyKey;
import com.baidu.hugegraph.schema.VertexLabel;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.define.HugeKeys;

public class GraphSONSchemaSerializer {

    public Map<HugeKeys, Object> writeVertexLabel(VertexLabel vertexLabel) {
        HugeGraph graph = vertexLabel.graph();
        assert graph != null;

        Map<HugeKeys, Object> map = new LinkedHashMap<>();
        map.put(HugeKeys.ID, vertexLabel.id().asLong());
        map.put(HugeKeys.NAME, vertexLabel.name());
        map.put(HugeKeys.ID_STRATEGY, vertexLabel.idStrategy());
        map.put(HugeKeys.PRIMARY_KEYS,
                graph.mapPkId2Name(vertexLabel.primaryKeys()));
        map.put(HugeKeys.NULLABLE_KEYS,
                graph.mapPkId2Name(vertexLabel.nullableKeys()));
        map.put(HugeKeys.INDEX_LABELS,
                graph.mapIlId2Name(vertexLabel.indexLabels()));
        map.put(HugeKeys.PROPERTIES,
                graph.mapPkId2Name(vertexLabel.properties()));
        map.put(HugeKeys.ENABLE_LABEL_INDEX, vertexLabel.enableLabelIndex());
        map.put(HugeKeys.USER_DATA, vertexLabel.userdata());
        return map;
    }
    
    public Map<HugeKeys, Object> writeEdgeLabel(EdgeLabel edgeLabel) {
        HugeGraph graph = edgeLabel.graph();
        assert graph != null;

        Map<HugeKeys, Object> map = new LinkedHashMap<>();
        map.put(HugeKeys.ID, edgeLabel.id().asLong());
        map.put(HugeKeys.NAME, edgeLabel.name());
        map.put(HugeKeys.SOURCE_LABEL,
                graph.vertexLabel(edgeLabel.sourceLabel()).name());
        map.put(HugeKeys.TARGET_LABEL,
                graph.vertexLabel(edgeLabel.targetLabel()).name());
        map.put(HugeKeys.FREQUENCY, edgeLabel.frequency());
        map.put(HugeKeys.SORT_KEYS,
                graph.mapPkId2Name(edgeLabel.sortKeys()));
        map.put(HugeKeys.NULLABLE_KEYS,
                graph.mapPkId2Name(edgeLabel.nullableKeys()));
        map.put(HugeKeys.INDEX_LABELS,
                graph.mapIlId2Name(edgeLabel.indexLabels()));
        map.put(HugeKeys.PROPERTIES,
                graph.mapPkId2Name(edgeLabel.properties()));
        map.put(HugeKeys.ENABLE_LABEL_INDEX, edgeLabel.enableLabelIndex());
        map.put(HugeKeys.USER_DATA, edgeLabel.userdata());
        return map;
    }
    
    public Map<HugeKeys, Object> writePropertyKey(PropertyKey propertyKey) {
        HugeGraph graph = propertyKey.graph();
        assert graph != null;

        Map<HugeKeys, Object> map = new LinkedHashMap<>();
        map.put(HugeKeys.ID, propertyKey.id().asLong());
        map.put(HugeKeys.NAME, propertyKey.name());
        map.put(HugeKeys.DATA_TYPE, propertyKey.dataType());
        map.put(HugeKeys.CARDINALITY, propertyKey.cardinality());
        map.put(HugeKeys.AGGREGATE_TYPE, propertyKey.aggregateType());
        map.put(HugeKeys.PROPERTIES,
                graph.mapPkId2Name(propertyKey.properties()));
        map.put(HugeKeys.USER_DATA, propertyKey.userdata());
        return map;
    }

    public Map<HugeKeys, Object> writeIndexLabel(IndexLabel indexLabel) {
        HugeGraph graph = indexLabel.graph();
        assert graph != null;

        Map<HugeKeys, Object> map = new LinkedHashMap<>();
        map.put(HugeKeys.ID, indexLabel.id().asLong());
        map.put(HugeKeys.NAME, indexLabel.name());
        map.put(HugeKeys.BASE_TYPE, indexLabel.baseType());
        if (indexLabel.baseType() == HugeType.VERTEX_LABEL) {
            map.put(HugeKeys.BASE_VALUE,
                    graph.vertexLabel(indexLabel.baseValue()).name());
        } else {
            assert indexLabel.baseType() == HugeType.EDGE_LABEL;
            map.put(HugeKeys.BASE_VALUE,
                    graph.edgeLabel(indexLabel.baseValue()).name());
        }
        map.put(HugeKeys.INDEX_TYPE, indexLabel.indexType());
        map.put(HugeKeys.FIELDS, graph.mapPkId2Name(indexLabel.indexFields()));
        return map;
    }
}
