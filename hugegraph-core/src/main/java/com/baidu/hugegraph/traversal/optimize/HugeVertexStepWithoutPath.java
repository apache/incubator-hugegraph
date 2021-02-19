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

package com.baidu.hugegraph.traversal.optimize;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;
import org.apache.tinkerpop.gremlin.util.iterator.EmptyIterator;

import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.backend.query.ConditionQuery;
import com.baidu.hugegraph.backend.query.BatchQuery;
import com.baidu.hugegraph.structure.HugeEdge;
import com.baidu.hugegraph.structure.HugeVertex;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.define.HugeKeys;
import com.google.common.base.Function;

public class HugeVertexStepWithoutPath<E extends Element>
       extends HugeVertexStep<E> {

    private static final long serialVersionUID = -3609787815053052222L;

    private List<Traverser.Admin<Vertex>> parents = null;
    private Iterator<E> iterator = EmptyIterator.instance();

    public HugeVertexStepWithoutPath(final VertexStep<E> originalVertexStep) {
        super(originalVertexStep);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Traverser.Admin<E> processNextStart() {
        /* Override super.processNextStart() */
        if (this.parents == null) {
            Function<Object, Traverser.Admin<?>> next = null;
            Step<?, Vertex> prev = this.getPreviousStep();
            if (prev.getClass() == HugeVertexStepWithoutPath.class) {
                next = (arg) ->
                     ((HugeVertexStepWithoutPath<?>) prev).processNextStart();
            } else {
                next = (arg) -> this.starts.next();
            }

            List<Traverser.Admin<Vertex>> vertexes = new LinkedList<>();
            while (true) {
                try {
                    vertexes.add((Traverser.Admin<Vertex>) next.apply(null));
                } catch (NoSuchElementException e) {
                    break;
                }
            }

            this.parents = vertexes;
            if (!vertexes.isEmpty()) {
                this.iterator = this.flatMap(vertexes);
            }
        }

        if (this.iterator.hasNext()) {
            // TODO: use this.parent(item) instead
            Traverser.Admin<Vertex> parent = this.parents.get(0);
            E item = this.iterator.next();
            return parent.split(item, this);
        }

        throw FastNoSuchElementException.instance();
    }

    @SuppressWarnings("unused")
    private Traverser.Admin<Vertex> parent(E item) {
        assert this.parents != null;
        Vertex parent = null;
        if (item instanceof HugeVertex) {
            HugeVertex vertex = (HugeVertex) item;
            // TODO: set the edge connected with the parent
            assert vertex.getEdges().size() == 1;
            parent = vertex.getEdges().iterator().next().otherVertex();
        } else {
            assert item instanceof HugeEdge;
            parent = ((HugeEdge) item).otherVertex();
        }

        for (Traverser.Admin<Vertex> p : this.parents) {
            if (p.equals(parent)) {
                return p;
            }
        }

        throw new BackendException(
                  "Unexpected item(without parent): '%s'", item);
    }

    @Override
    public void reset() {
        this.parents = null;
        super.reset();
        closeIterator();
        this.iterator = EmptyIterator.instance();
    }

    @Override
    protected void closeIterator() {
        CloseableIterator.closeIterator(this.iterator);
    }

    @SuppressWarnings("unchecked")
    private Iterator<E> flatMap(List<Traverser.Admin<Vertex>> traversers) {
        boolean queryVertex = Vertex.class.isAssignableFrom(getReturnClass());
        boolean queryEdge = Edge.class.isAssignableFrom(getReturnClass());
        assert queryVertex || queryEdge;
        if (queryVertex) {
            return (Iterator<E>) this.vertices(traversers);
        } else {
            assert queryEdge;
            return (Iterator<E>) this.edges(traversers);
        }
    }

    private Iterator<Vertex> vertices(
                             List<Traverser.Admin<Vertex>> traversers) {
        assert traversers.size() > 0;

        Iterator<Edge> edges = this.edges(traversers);
        return this.queryAdjacentVertices(edges);
    }

    private Iterator<Edge> edges(List<Traverser.Admin<Vertex>> traversers) {
        assert traversers.size() > 0;

        BatchQuery batchQuery = new BatchQuery(HugeType.EDGE);

        for (Traverser.Admin<Vertex> t : traversers) {
            ConditionQuery query = this.constructEdgesQuery(t);
            // Merge each query into batch query
            batchQuery.mergeWithIn(query, HugeKeys.OWNER_VERTEX);
        }

        return this.queryEdges(batchQuery);
    }
}
