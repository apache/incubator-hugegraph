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

package com.baidu.hugegraph.variables;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.id.SplicingIdGenerator;
import com.baidu.hugegraph.backend.query.ConditionQuery;
import com.baidu.hugegraph.schema.SchemaManager;
import com.baidu.hugegraph.schema.VertexLabel;
import com.baidu.hugegraph.structure.HugeVertex;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.define.HugeKeys;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.apache.tinkerpop.gremlin.structure.Graph.Hidden;

public class HugeVariables implements Graph.Variables {

    private static final String VARIABLES = "variables";
    private static final String VARIABLES_KEY = "variablesKey";
    private static final String VARIABLES_TYPE = "variablesType";

    private static final String BYTE_VALUE = "byteValue";
    private static final String BOOLEAN_VALUE = "booleanValue";
    private static final String INTEGER_VALUE = "integerValue";
    private static final String LONG_VALUE = "longValue";
    private static final String FLOAT_VALUE = "floatValue";
    private static final String DOUBLE_VALUE = "doubleValue";
    private static final String STRING_VALUE = "stringValue";

    private static final String UNIFORM_LIST = "UniformList";
    private static final String SET = "Set";

    private List<String> types = Arrays.asList(
            Hidden.hide(BYTE_VALUE),
            Hidden.hide(BOOLEAN_VALUE),
            Hidden.hide(INTEGER_VALUE),
            Hidden.hide(LONG_VALUE),
            Hidden.hide(FLOAT_VALUE),
            Hidden.hide(DOUBLE_VALUE),
            Hidden.hide(STRING_VALUE),
            Hidden.hide(BYTE_VALUE + UNIFORM_LIST),
            Hidden.hide(BOOLEAN_VALUE + UNIFORM_LIST),
            Hidden.hide(INTEGER_VALUE + UNIFORM_LIST),
            Hidden.hide(LONG_VALUE + UNIFORM_LIST),
            Hidden.hide(FLOAT_VALUE + UNIFORM_LIST),
            Hidden.hide(DOUBLE_VALUE + UNIFORM_LIST),
            Hidden.hide(STRING_VALUE + UNIFORM_LIST),
            Hidden.hide(BYTE_VALUE + SET),
            Hidden.hide(BOOLEAN_VALUE + SET),
            Hidden.hide(INTEGER_VALUE + SET),
            Hidden.hide(LONG_VALUE + SET),
            Hidden.hide(FLOAT_VALUE + SET),
            Hidden.hide(DOUBLE_VALUE + SET),
            Hidden.hide(STRING_VALUE + SET)
    );

    private Map<String, Object> variables;
    private HugeGraph graph;

    public HugeVariables(HugeGraph graph) {
        this.graph = graph;
        this.variables = new HashMap<>();

        this.init(graph);
    }

    private void init(HugeGraph graph) {
        // Create schema if needed
        this.initSchema();

        // Get all variable vertices
        ConditionQuery query = new ConditionQuery(HugeType.VERTEX);
        query.eq(HugeKeys.LABEL, Hidden.hide(VARIABLES));
        Iterator<Vertex> vertices = graph.vertices(query);

        // Init key/value Map using variable vertices
        vertices.forEachRemaining(vertex -> initVariables(vertex));
    }

    private void initSchema() {
        SchemaManager schema = this.graph.schema();
        schema.propertyKey(Hidden.hide(VARIABLES_KEY))
              .ifNotExist().create();
        schema.propertyKey(Hidden.hide(VARIABLES_TYPE))
              .ifNotExist().create();
        schema.propertyKey(Hidden.hide(BYTE_VALUE))
              .asByte().ifNotExist().create();
        schema.propertyKey(Hidden.hide(BOOLEAN_VALUE))
              .asBoolean().ifNotExist().create();
        schema.propertyKey(Hidden.hide(INTEGER_VALUE))
              .asInt().ifNotExist().create();
        schema.propertyKey(Hidden.hide(LONG_VALUE))
              .asLong().ifNotExist().create();
        schema.propertyKey(Hidden.hide(FLOAT_VALUE))
              .asFloat().ifNotExist().create();
        schema.propertyKey(Hidden.hide(DOUBLE_VALUE))
              .asDouble().ifNotExist().create();
        schema.propertyKey(Hidden.hide(STRING_VALUE))
              .ifNotExist().create();
        schema.propertyKey(Hidden.hide(BYTE_VALUE + UNIFORM_LIST))
              .asByte().valueList().ifNotExist().create();
        schema.propertyKey(Hidden.hide(BOOLEAN_VALUE + UNIFORM_LIST))
              .asBoolean().valueList().ifNotExist().create();
        schema.propertyKey(Hidden.hide(INTEGER_VALUE + UNIFORM_LIST))
              .asInt().valueList().ifNotExist().create();
        schema.propertyKey(Hidden.hide(LONG_VALUE + UNIFORM_LIST))
              .asLong().valueList().ifNotExist().create();
        schema.propertyKey(Hidden.hide(FLOAT_VALUE + UNIFORM_LIST))
              .asFloat().valueList().ifNotExist().create();
        schema.propertyKey(Hidden.hide(DOUBLE_VALUE + UNIFORM_LIST))
              .asDouble().valueList().ifNotExist().create();
        schema.propertyKey(Hidden.hide(STRING_VALUE + UNIFORM_LIST))
              .valueList().ifNotExist().create();
        schema.propertyKey(Hidden.hide(BYTE_VALUE + SET))
              .asByte().valueSet().ifNotExist().create();
        schema.propertyKey(Hidden.hide(BOOLEAN_VALUE + SET))
              .asBoolean().valueSet().ifNotExist().create();
        schema.propertyKey(Hidden.hide(INTEGER_VALUE + SET))
              .asInt().valueSet().ifNotExist().create();
        schema.propertyKey(Hidden.hide(LONG_VALUE + SET))
              .asLong().valueSet().ifNotExist().create();
        schema.propertyKey(Hidden.hide(FLOAT_VALUE + SET))
              .asFloat().valueSet().ifNotExist().create();
        schema.propertyKey(Hidden.hide(DOUBLE_VALUE + SET))
              .asDouble().valueSet().ifNotExist().create();
        schema.propertyKey(Hidden.hide(STRING_VALUE + SET))
              .valueSet().ifNotExist().create();

        String[] properties = this.types.toArray(
                              new String[this.types.size() + 2]);
        properties[this.types.size()] = Hidden.hide(VARIABLES_KEY);
        properties[this.types.size() + 1] = Hidden.hide(VARIABLES_TYPE);

        schema.vertexLabel(Hidden.hide(VARIABLES))
              .properties(properties)
              .primaryKeys(Hidden.hide(VARIABLES_KEY))
              .nullableKeys(this.types.toArray(new String[this.types.size()]))
              .ifNotExist().create();
    }

    private void initVariables(Vertex vertex) {
        String key = vertex.value(Hidden.hide(VARIABLES_KEY));
        String type = vertex.value(Hidden.hide(VARIABLES_TYPE));
        Object value;
        if (!this.types.contains(type)) {
            throw Graph.Variables.Exceptions
                       .dataTypeOfVariableValueNotSupported(type);
        }
        value = vertex.value(Hidden.hide(type));
        this.variables.put(key, value);
    }

    @Override
    public Set<String> keys() {
        return this.variables.keySet();
    }

    @Override
    public <R> Optional<R> get(String key) {
        if (key == null) {
            throw Graph.Variables.Exceptions.variableKeyCanNotBeNull();
        }
        if (key.isEmpty()) {
            throw Graph.Variables.Exceptions.variableKeyCanNotBeEmpty();
        }
        return Optional.of((R) this.variables.get(key));
    }

    @Override
    public void set(String key, Object value) {
        if (key == null) {
            throw Graph.Variables.Exceptions.variableKeyCanNotBeNull();
        }
        if (key.isEmpty()) {
            throw Graph.Variables.Exceptions.variableKeyCanNotBeEmpty();
        }
        if (value == null) {
            throw Graph.Variables.Exceptions.variableValueCanNotBeNull();
        }

        this.variables.put(key, value);

        HugeVertex vertex = this.constructVariableVertex(key);
        this.setVariablesValue(vertex, value);
        this.graph.graphTransaction().addVertex(vertex);
    }

    private void setVariablesValue(HugeVertex vertex, Object value) {
        try {
            if (value instanceof Byte) {
                vertex.property(Hidden.hide(BYTE_VALUE), value);
                vertex.property(Hidden.hide(VARIABLES_TYPE), BYTE_VALUE);
            } else if (value instanceof Boolean) {
                vertex.property(Hidden.hide(BOOLEAN_VALUE), value);
                vertex.property(Hidden.hide(VARIABLES_TYPE), BOOLEAN_VALUE);
            } else if (value instanceof Integer) {
                vertex.property(Hidden.hide(INTEGER_VALUE), value);
                vertex.property(Hidden.hide(VARIABLES_TYPE), INTEGER_VALUE);
            } else if (value instanceof Long) {
                vertex.property(Hidden.hide(LONG_VALUE), value);
                vertex.property(Hidden.hide(VARIABLES_TYPE), LONG_VALUE);
            } else if (value instanceof Float) {
                vertex.property(Hidden.hide(FLOAT_VALUE), value);
                vertex.property(Hidden.hide(VARIABLES_TYPE), FLOAT_VALUE);
            } else if (value instanceof Double) {
                vertex.property(Hidden.hide(DOUBLE_VALUE), value);
                vertex.property(Hidden.hide(VARIABLES_TYPE), DOUBLE_VALUE);
            } else if (value instanceof String) {
                vertex.property(Hidden.hide(STRING_VALUE), value);
                vertex.property(Hidden.hide(VARIABLES_TYPE), STRING_VALUE);
            } else if (value instanceof List || value instanceof Set) {
                String suffix = value instanceof List ? UNIFORM_LIST : SET;
                Collection<?> collection = (Collection<?>) value;
                if (collection.isEmpty()) {
                    vertex.property(Hidden.hide(STRING_VALUE + suffix), value);
                    vertex.property(Hidden.hide(VARIABLES_TYPE),
                                    STRING_VALUE + suffix);
                    return;
                }
                Object object1 = collection.toArray()[0];
                if (object1 instanceof Byte) {
                    vertex.property(Hidden.hide(BYTE_VALUE + suffix), value);
                    vertex.property(Hidden.hide(VARIABLES_TYPE),
                                    BYTE_VALUE + suffix);
                } else if (object1 instanceof Boolean) {
                    vertex.property(Hidden.hide(BOOLEAN_VALUE + suffix),
                                    value);
                    vertex.property(Hidden.hide(VARIABLES_TYPE),
                                    BOOLEAN_VALUE + suffix);
                } else if (object1 instanceof Integer) {
                    vertex.property(Hidden.hide(INTEGER_VALUE + suffix),
                                    value);
                    vertex.property(Hidden.hide(VARIABLES_TYPE),
                                    INTEGER_VALUE + suffix);
                } else if (object1 instanceof Long) {
                    vertex.property(Hidden.hide(LONG_VALUE + suffix), value);
                    vertex.property(Hidden.hide(VARIABLES_TYPE),
                                    LONG_VALUE + suffix);
                } else if (object1 instanceof Float) {
                    vertex.property(Hidden.hide(FLOAT_VALUE + suffix), value);
                    vertex.property(Hidden.hide(VARIABLES_TYPE),
                                    FLOAT_VALUE + suffix);
                } else if (object1 instanceof Double) {
                    vertex.property(Hidden.hide(DOUBLE_VALUE + suffix), value);
                    vertex.property(Hidden.hide(VARIABLES_TYPE),
                                    DOUBLE_VALUE + suffix);
                } else if (object1 instanceof String) {
                    vertex.property(Hidden.hide(STRING_VALUE + suffix), value);
                    vertex.property(Hidden.hide(VARIABLES_TYPE),
                                    STRING_VALUE + suffix);
                } else {
                    throw Graph.Variables.Exceptions
                               .dataTypeOfVariableValueNotSupported(value);
                }
            } else {
                throw Graph.Variables.Exceptions
                           .dataTypeOfVariableValueNotSupported(value);
            }
        } catch (IllegalArgumentException e) {
            throw Graph.Variables.Exceptions
                       .dataTypeOfVariableValueNotSupported(value);
        }
    }

    @Override
    public void remove(String key) {
        if (key == null) {
            throw Graph.Variables.Exceptions.variableKeyCanNotBeNull();
        }
        if (key.isEmpty()) {
            throw Graph.Variables.Exceptions.variableKeyCanNotBeEmpty();
        }
        if (this.variables.keySet().contains(key)) {
            this.variables.remove(key);
            HugeVertex vertex = this.constructVariableVertex(key);
            this.graph.graphTransaction().removeVertex(vertex);
        }
    }

    private HugeVertex constructVariableVertex(String key) {
        Id id = SplicingIdGenerator.splicing(Hidden.hide(VARIABLES), key);
        VertexLabel vl = this.graph.vertexLabel(Hidden.hide(VARIABLES));
        return new HugeVertex(this.graph, id, vl);
    }

    @Override
    public String toString() {
        return StringFactory.graphVariablesString(this);
    }
}
