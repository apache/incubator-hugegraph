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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.HasContainerHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.query.ConditionQuery;
import com.baidu.hugegraph.backend.tx.GraphTransaction;
import com.baidu.hugegraph.type.ExtendableIterator;
import com.google.common.collect.ImmutableSet;

public final class HugeVertexStep<E extends Element>
             extends VertexStep<E> implements HasContainerHolder {

    private static final long serialVersionUID = -7850636388424382454L;

    private static final Logger logger =
                         LoggerFactory.getLogger(HugeVertexStep.class);

    private final List<HasContainer> hasContainers = new ArrayList<>();

    public HugeVertexStep(final VertexStep<E> originVertexStep) {
        super(originVertexStep.getTraversal(),
              originVertexStep.getReturnClass(),
              originVertexStep.getDirection(),
              originVertexStep.getEdgeLabels());
        originVertexStep.getLabels().forEach(this::addLabel);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Iterator<E> flatMap(final Traverser.Admin<Vertex> traverser) {
        boolean queryVertex = Vertex.class.isAssignableFrom(getReturnClass());
        boolean queryEdge = Edge.class.isAssignableFrom(getReturnClass());
        assert queryVertex || queryEdge;
        if (queryVertex) {
            return (Iterator<E>) this.vertices(traverser);
        } else {
            assert queryEdge;
            return (Iterator<E>) this.edges(traverser);
        }
    }

    private Iterator<Vertex> vertices(Traverser.Admin<Vertex> traverser) {
        HugeGraph graph = (HugeGraph) traverser.get().graph();
        Vertex vertex = traverser.get();

        Iterator<Edge> edges = this.edges(traverser);
        Iterator<Vertex> vertices = graph.adjacentVertices(edges);
        if (logger.isDebugEnabled()) {
            logger.debug("HugeVertexStep.vertices(): is there adjacent " +
                         "vertices of {}: {}, has={}",
                         vertex.id(), vertices.hasNext(), this.hasContainers);
        }

        // TODO: query by vertex index to optimize
        return HugeGraphStep.filterResult(this.hasContainers, vertices);
    }

    private Iterator<Edge> edges(Traverser.Admin<Vertex> traverser) {
        HugeGraph graph = (HugeGraph) traverser.get().graph();

        Vertex vertex = traverser.get();
        Direction direction = this.getDirection();
        String[] edgeLabels = this.getEdgeLabels();

        logger.debug("HugeVertexStep.edges(): vertex={}, direction={}, " +
                     "edgeLabels={}, has={}",
                     vertex.id(), direction, edgeLabels, this.hasContainers);

        ImmutableSet<Direction> directions = ImmutableSet.of(direction);
        // Deal with direction is BOTH
        if (direction == Direction.BOTH) {
            directions = ImmutableSet.of(Direction.OUT, Direction.IN);
        }

        ExtendableIterator<Edge> results = new ExtendableIterator<>();
        for (Direction dir : directions) {
            ConditionQuery query = GraphTransaction.constructEdgesQuery(
                                   (Id) vertex.id(), dir, edgeLabels);

            // Enable conditions if query for edge else conditions for vertex
            if (Edge.class.isAssignableFrom(getReturnClass())) {
                HugeGraphStep.fillConditionQuery(this.hasContainers, query);
            }

            if (!query.ids().isEmpty()) {
                // TODO: should check the edge id match this vertex
                // ignore conditions if query by edge id in has-containers
                query.resetConditions();
                logger.warn("It's not recommended to query by has(id)");
            }

            // Do query
            results.extend(graph.edges(query));
        }
        return results;
    }

    @Override
    public String toString() {
        if (this.hasContainers.isEmpty()) {
            return super.toString();
        }

        return StringFactory.stepString(
               this,
               getDirection(),
               Arrays.asList(getEdgeLabels()),
               getReturnClass().getSimpleName(),
               this.hasContainers);
    }

    @Override
    public List<HasContainer> getHasContainers() {
        return Collections.unmodifiableList(this.hasContainers);
    }

    @Override
    public void addHasContainer(final HasContainer hasContainer) {
        this.hasContainers.add(hasContainer);
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ this.hasContainers.hashCode();
    }
}
