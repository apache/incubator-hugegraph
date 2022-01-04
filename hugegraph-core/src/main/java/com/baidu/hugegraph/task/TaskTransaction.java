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

 /**
  * Splitted from StandardTaskScheduler on 2022-1-4 by Scorpiour
  * Provide general transaction for queries;
  * @since 2022-01-04
  */
package com.baidu.hugegraph.task;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.tinkerpop.gremlin.structure.Graph.Hidden;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import com.baidu.hugegraph.HugeException;
import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.HugeGraphParams;
import com.baidu.hugegraph.backend.query.QueryResults;
import com.baidu.hugegraph.backend.store.BackendStore;

import com.baidu.hugegraph.backend.tx.GraphTransaction;
import com.baidu.hugegraph.schema.IndexLabel;
import com.baidu.hugegraph.schema.PropertyKey;
import com.baidu.hugegraph.schema.SchemaManager;
import com.baidu.hugegraph.schema.VertexLabel;
import com.baidu.hugegraph.structure.HugeVertex;
import com.baidu.hugegraph.task.HugeTask.P;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.define.Cardinality;
import com.baidu.hugegraph.type.define.DataType;

public class TaskTransaction extends GraphTransaction {

    public static final String TASK = P.TASK;

    public TaskTransaction(HugeGraphParams graph, BackendStore store) {
        super(graph, store);
        this.autoCommit(true);
    }

    public HugeVertex constructVertex(HugeTask<?> task) {
        if (!this.graph().existsVertexLabel(TASK)) {
            throw new HugeException("Schema is missing for task(%s) '%s'",
                                    task.id(), task.name());
        }
        return this.constructVertex(false, task.asArray());
    }

    public void deleteIndex(HugeVertex vertex) {
        // Delete the old record if exist
        Iterator<Vertex> old = this.queryVertices(vertex.id());
        HugeVertex oldV = (HugeVertex) QueryResults.one(old);
        if (oldV == null) {
            return;
        }
        this.deleteIndexIfNeeded(oldV, vertex);
    }

    private boolean deleteIndexIfNeeded(HugeVertex oldV, HugeVertex newV) {
        if (!oldV.value(P.STATUS).equals(newV.value(P.STATUS))) {
            // Only delete vertex if index value changed else override it
            this.updateIndex(this.indexLabel(P.STATUS).id(), oldV, true);
            return true;
        }
        return false;
    }

    public void initSchema() {
        if (this.existVertexLabel(TASK)) {
            return;
        }

        HugeGraph graph = this.graph();
        String[] properties = this.initProperties();

        // Create vertex label '~task'
        VertexLabel label = graph.schema().vertexLabel(TASK)
                                 .properties(properties)
                                 .useCustomizeNumberId()
                                 .nullableKeys(P.DESCRIPTION, P.CONTEXT,
                                               P.UPDATE, P.INPUT, P.RESULT,
                                               P.DEPENDENCIES, P.SERVER)
                                 .enableLabelIndex(true)
                                 .build();
        this.params().schemaTransaction().addVertexLabel(label);

        // Create index
        this.createIndexLabel(label, P.STATUS);
    }

    private boolean existVertexLabel(String label) {
        return this.params().schemaTransaction()
                            .getVertexLabel(label) != null;
    }

    private String[] initProperties() {
        List<String> props = new ArrayList<>();

        props.add(createPropertyKey(P.TYPE));
        props.add(createPropertyKey(P.NAME));
        props.add(createPropertyKey(P.CALLABLE));
        props.add(createPropertyKey(P.DESCRIPTION));
        props.add(createPropertyKey(P.CONTEXT));
        props.add(createPropertyKey(P.STATUS, DataType.BYTE));
        props.add(createPropertyKey(P.PROGRESS, DataType.INT));
        props.add(createPropertyKey(P.CREATE, DataType.DATE));
        props.add(createPropertyKey(P.UPDATE, DataType.DATE));
        props.add(createPropertyKey(P.RETRIES, DataType.INT));
        props.add(createPropertyKey(P.INPUT, DataType.BLOB));
        props.add(createPropertyKey(P.RESULT, DataType.BLOB));
        props.add(createPropertyKey(P.DEPENDENCIES, DataType.LONG,
                                    Cardinality.SET));
        props.add(createPropertyKey(P.SERVER));

        return props.toArray(new String[0]);
    }

    private String createPropertyKey(String name) {
        return this.createPropertyKey(name, DataType.TEXT);
    }

    private String createPropertyKey(String name, DataType dataType) {
        return this.createPropertyKey(name, dataType, Cardinality.SINGLE);
    }

    private String createPropertyKey(String name, DataType dataType,
                                     Cardinality cardinality) {
        HugeGraph graph = this.graph();
        SchemaManager schema = graph.schema();
        PropertyKey propertyKey = schema.propertyKey(name)
                                        .dataType(dataType)
                                        .cardinality(cardinality)
                                        .build();
        this.params().schemaTransaction().addPropertyKey(propertyKey);
        return name;
    }

    private IndexLabel createIndexLabel(VertexLabel label, String field) {
        HugeGraph graph = this.graph();
        SchemaManager schema = graph.schema();
        String name = Hidden.hide("task-index-by-" + field);
        IndexLabel indexLabel = schema.indexLabel(name)
                                      .on(HugeType.VERTEX_LABEL, TASK)
                                      .by(field)
                                      .build();
        this.params().schemaTransaction().addIndexLabel(label, indexLabel);
        return indexLabel;
    }

    private IndexLabel indexLabel(String field) {
        String name = Hidden.hide("task-index-by-" + field);
        HugeGraph graph = this.graph();
        return graph.indexLabel(name);
    }
}