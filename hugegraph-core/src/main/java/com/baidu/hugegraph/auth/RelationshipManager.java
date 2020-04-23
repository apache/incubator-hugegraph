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

import org.apache.tinkerpop.gremlin.structure.Edge;

import com.baidu.hugegraph.HugeException;
import com.baidu.hugegraph.HugeGraphParams;
import com.baidu.hugegraph.auth.SchemaDefine.Relationship;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.query.Condition;
import com.baidu.hugegraph.backend.query.ConditionQuery;
import com.baidu.hugegraph.backend.query.QueryResults;
import com.baidu.hugegraph.backend.tx.GraphTransaction;
import com.baidu.hugegraph.exception.NotFoundException;
import com.baidu.hugegraph.iterator.MapperIterator;
import com.baidu.hugegraph.schema.EdgeLabel;
import com.baidu.hugegraph.schema.PropertyKey;
import com.baidu.hugegraph.schema.VertexLabel;
import com.baidu.hugegraph.structure.HugeEdge;
import com.baidu.hugegraph.structure.HugeVertex;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.define.Directions;
import com.baidu.hugegraph.type.define.HugeKeys;
import com.baidu.hugegraph.util.E;
import com.google.common.collect.ImmutableMap;

public class RelationshipManager<T extends Relationship> {

    private final HugeGraphParams graph;
    private final String label;
    private final Function<Edge, T> deser;

    private static final long NO_LIMIT = -1L;

    public RelationshipManager(HugeGraphParams graph, String label,
                               Function<Edge, T> dser) {
        E.checkNotNull(graph, "graph");

        this.graph = graph;
        this.label = label;
        this.deser = dser;
    }

    private GraphTransaction tx() {
        return this.graph.systemTransaction();
    }

    public Id add(T relationship) {
        E.checkArgumentNotNull(relationship, "Relationship can't be null");
        return this.save(relationship);
    }

    public Id update(T relationship) {
        E.checkArgumentNotNull(relationship, "Relationship can't be null");
        relationship.onUpdate();
        return this.save(relationship);
    }

    public T delete(Id id) {
        T relationship = null;
        Iterator<Edge> edges = this.tx().queryEdges(id);
        if (edges.hasNext()) {
            HugeEdge edge = (HugeEdge) edges.next();
            relationship = this.deser.apply(edge);
            this.tx().removeEdge(edge);
            this.commitOrRollback();
            assert !edges.hasNext();
        }
        return relationship;
    }

    public T get(Id id) {
        T relationship = null;
        Iterator<Edge> edges = this.tx().queryEdges(id);
        if (edges.hasNext()) {
            relationship = this.deser.apply(edges.next());
            assert !edges.hasNext();
        }
        if (relationship == null) {
            throw new NotFoundException("Can't find %s with id '%s'",
                                        this.label, id);
        }
        return relationship;
    }

    public List<T> list(List<Id> ids) {
        return toList(this.queryById(ids));
    }

    public List<T> list(long limit) {
        Iterator<Edge> edges = this.queryRelationship(null, null, this.label,
                                                      ImmutableMap.of(), limit);
        return toList(edges);
    }

    public List<T> list(Id source, Directions direction,
                        String label, long limit) {
        Iterator<Edge> edges = this.queryRelationship(source, direction, label,
                                                      ImmutableMap.of(), limit);
        return toList(edges);
    }

    public List<T> list(Id source, Directions direction, String label,
                        String key, Object value, long limit) {
        Map<String, Object> conditions = ImmutableMap.of(key, value);
        Iterator<Edge> edges = this.queryRelationship(source, direction, label,
                                                      conditions, limit);
        return toList(edges);
    }

    protected List<T> toList(Iterator<Edge> edges) {
        Iterator<T> iter = new MapperIterator<>(edges, this.deser);
        // Convert iterator to list to avoid across thread tx accessed
        return (List<T>) QueryResults.toList(iter).list();
    }

    private Iterator<Edge> queryById(List<Id> ids) {
        Object[] idArray = ids.toArray(new Id[ids.size()]);
        Iterator<Edge> edges = this.tx().queryEdges(idArray);
        return edges;
    }

    private Iterator<Edge> queryRelationship(Id source,
                                             Directions direction,
                                             String label,
                                             Map<String, Object> conditions,
                                             long limit) {
        ConditionQuery query = new ConditionQuery(HugeType.EDGE);
        EdgeLabel el = this.graph.graph().edgeLabel(label);
        if (direction == null) {
            direction = Directions.OUT;
        }
        if (source != null) {
            query.eq(HugeKeys.OWNER_VERTEX, source);
            query.eq(HugeKeys.DIRECTION, direction);
        }
        if (label != null) {
            query.eq(HugeKeys.LABEL, el.id());
        }
        for (Map.Entry<String, Object> entry : conditions.entrySet()) {
            PropertyKey pk = this.graph.graph().propertyKey(entry.getKey());
            query.query(Condition.eq(pk.id(), entry.getValue()));
        }
        query.showHidden(true);
        if (limit != NO_LIMIT) {
            query.limit(limit);
        }
        Iterator<Edge> edges = this.tx().queryEdges(query);
        if (limit == NO_LIMIT) {
            return edges;
        }
        long[] size = new long[1];
        return new MapperIterator<>(edges, edge -> {
            if (++size[0] > limit) {
                return null;
            }
            return edge;
        });
    }

    private Id save(T relationship) {
        if (!SchemaDefine.existEdgeLabel(this.graph, relationship.label())) {
            throw new HugeException("Schema is missing for %s '%s'",
                                    relationship.label(),
                                    relationship.source());
        }
        HugeVertex source = this.newVertex(relationship.source(),
                                           relationship.sourceLabel());
        HugeVertex target = this.newVertex(relationship.target(),
                                           relationship.targetLabel());
        HugeEdge edge = source.constructEdge(relationship.label(), target,
                                             relationship.asArray());
        this.tx().addEdge(edge);
        this.commitOrRollback();
        return edge.id();
    }

    private HugeVertex newVertex(Object id, String label) {
        VertexLabel vl = SchemaDefine.vertexLabel(this.graph, label);
        Id idValue = HugeVertex.getIdValue(id);
        return new HugeVertex(this.tx(), idValue, vl);
    }

    private void commitOrRollback() {
        this.tx().commitOrRollback();
    }
}
