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

package com.baidu.hugegraph.auth;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.apache.tinkerpop.gremlin.structure.Vertex;

import com.baidu.hugegraph.HugeException;
import com.baidu.hugegraph.HugeGraphParams;
import com.baidu.hugegraph.auth.SchemaDefine.Entity;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.query.Condition;
import com.baidu.hugegraph.backend.query.ConditionQuery;
import com.baidu.hugegraph.backend.query.QueryResults;
import com.baidu.hugegraph.backend.tx.GraphTransaction;
import com.baidu.hugegraph.exception.NotFoundException;
import com.baidu.hugegraph.iterator.MapperIterator;
import com.baidu.hugegraph.schema.PropertyKey;
import com.baidu.hugegraph.schema.VertexLabel;
import com.baidu.hugegraph.structure.HugeVertex;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.define.HugeKeys;
import com.baidu.hugegraph.util.E;
import com.google.common.collect.ImmutableMap;

public class EntityManager<T extends Entity> {

    private final HugeGraphParams graph;
    private final String label;
    private final Function<Vertex, T> deser;

    private static final long NO_LIMIT = -1L;

    public EntityManager(HugeGraphParams graph, String label,
                         Function<Vertex, T> dser) {
        E.checkNotNull(graph, "graph");

        this.graph = graph;
        this.label = label;
        this.deser = dser;
    }

    private GraphTransaction tx() {
        return this.graph.systemTransaction();
    }

    public Id add(T entity) {
        E.checkArgumentNotNull(entity, "Entity can't be null");
        return this.save(entity);
    }

    public Id update(T entity) {
        E.checkArgumentNotNull(entity, "Entity can't be null");
        entity.onUpdate();
        return this.save(entity);
    }

    public T delete(Id id) {
        T entity = null;
        Iterator<Vertex> vertices = this.tx().queryVertices(id);
        if (vertices.hasNext()) {
            HugeVertex vertex = (HugeVertex) vertices.next();
            entity = this.deser.apply(vertex);
            this.tx().removeVertex(vertex);
            this.commitOrRollback();
            assert !vertices.hasNext();
        }
        return entity;
    }

    public T get(Id id) {
        T entity = null;
        Iterator<Vertex> vertices = this.tx().queryVertices(id);
        if (vertices.hasNext()) {
            entity = this.deser.apply(vertices.next());
            assert !vertices.hasNext();
        }
        if (entity == null) {
            throw new NotFoundException("Can't find %s with id '%s'",
                                        this.label, id);
        }
        return entity;
    }

    public List<T> list(List<Id> ids) {
        return toList(this.queryById(ids));
    }

    public List<T> list(long limit) {
        return toList(this.queryEntity(this.label, ImmutableMap.of(), limit));
    }

    protected List<T> query(String key, Object value, long limit) {
        Map<String, Object> conditions = ImmutableMap.of(key, value);
        return toList(this.queryEntity(this.label, conditions, limit));
    }

    protected List<T> toList(Iterator<Vertex> vertices) {
        Iterator<T> iter = new MapperIterator<>(vertices, this.deser);
        // Convert iterator to list to avoid across thread tx accessed
        return (List<T>) QueryResults.toList(iter).list();
    }

    private Iterator<Vertex> queryById(List<Id> ids) {
        Object[] idArray = ids.toArray(new Id[ids.size()]);
        Iterator<Vertex> vertices = this.tx().queryVertices(idArray);
        return vertices;
    }

    private Iterator<Vertex> queryEntity(String label,
                                         Map<String, Object> conditions,
                                         long limit) {
        ConditionQuery query = new ConditionQuery(HugeType.VERTEX);
        VertexLabel vl = SchemaDefine.vertexLabel(this.graph, label);
        query.eq(HugeKeys.LABEL, vl.id());
        for (Map.Entry<String, Object> entry : conditions.entrySet()) {
            PropertyKey pkey = SchemaDefine.propertyKey(this.graph,
                                                        entry.getKey());
            query.query(Condition.eq(pkey.id(), entry.getValue()));
        }
        query.showHidden(true);
        if (limit != NO_LIMIT) {
            query.limit(limit);
        }
        return this.tx().queryVertices(query);
    }

    private Id save(T entity) {
        // Construct vertex from task
        HugeVertex vertex = this.constructVertex(entity);
        // Add or update user in backend store, stale index might exist
        vertex = this.tx().addVertex(vertex);
        this.commitOrRollback();
        return vertex.id();
    }

    private HugeVertex constructVertex(Entity entity) {
        if (!SchemaDefine.existVertexLabel(this.graph, entity.label())) {
            throw new HugeException("Schema is missing for %s '%s'",
                                    entity.label(), entity.id());
        }
        return this.tx().constructVertex(false, entity.asArray());
    }

    private void commitOrRollback() {
        this.tx().commitOrRollback();
    }
}
