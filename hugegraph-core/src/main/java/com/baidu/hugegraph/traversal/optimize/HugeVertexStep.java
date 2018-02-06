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
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.slf4j.Logger;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.query.ConditionQuery;
import com.baidu.hugegraph.backend.query.Query;
import com.baidu.hugegraph.backend.tx.GraphTransaction;
import com.baidu.hugegraph.iterator.ExtendableIterator;
import com.baidu.hugegraph.type.define.Directions;
import com.baidu.hugegraph.util.Log;
import com.google.common.collect.ImmutableSet;

public final class HugeVertexStep<E extends Element>
             extends VertexStep<E> implements QueryHolder {

    private static final long serialVersionUID = -7850636388424382454L;

    private static final Logger LOG = Log.logger(HugeVertexStep.class);

    private final List<HasContainer> hasContainers = new ArrayList<>();;

    // Store limit/order-by
    private final Query queryInfo = new Query(null);

    private Iterator<E> lastTimeResults = null;

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
        Iterator<E> results;
        boolean queryVertex = this.returnsVertex();
        boolean queryEdge = this.returnsEdge();
        assert queryVertex || queryEdge;
        if (queryVertex) {
            results = (Iterator<E>) this.vertices(traverser);
        } else {
            assert queryEdge;
            results = (Iterator<E>) this.edges(traverser);
        }
        this.lastTimeResults = results;
        return results;
    }

    private Iterator<Vertex> vertices(Traverser.Admin<Vertex> traverser) {
        HugeGraph graph = (HugeGraph) traverser.get().graph();
        Vertex vertex = traverser.get();

        Iterator<Edge> edges = this.edges(traverser);
        Iterator<Vertex> vertices = graph.adjacentVertices(edges);

        if (LOG.isDebugEnabled()) {
            LOG.debug("HugeVertexStep.vertices(): is there adjacent " +
                      "vertices of {}: {}, has={}",
                      vertex.id(), vertices.hasNext(), this.hasContainers);
        }

        if (this.hasContainers.isEmpty()) {
            return vertices;
        }

        // TODO: query by vertex index to optimize
        return TraversalUtil.filterResult(this.hasContainers, vertices);
    }

    private Iterator<Edge> edges(Traverser.Admin<Vertex> traverser) {
        HugeGraph graph = (HugeGraph) traverser.get().graph();
        List<HasContainer> conditions = this.hasContainers;

        // Query for edge with conditions(else conditions for vertex)
        boolean withEdgeCond = Edge.class.isAssignableFrom(getReturnClass()) &&
                               !conditions.isEmpty();

        Vertex vertex = traverser.get();
        Directions direction = Directions.convert(this.getDirection());
        String[] edgeLabels = this.getEdgeLabels();

        LOG.debug("HugeVertexStep.edges(): vertex={}, direction={}, " +
                  "edgeLabels={}, has={}",
                  vertex.id(), direction, edgeLabels, this.hasContainers);

        ImmutableSet<Directions> directions = ImmutableSet.of(direction);
        // Deal with direction is BOTH
        if (direction == Directions.BOTH) {
            directions = ImmutableSet.of(Directions.OUT, Directions.IN);
        }

        Id[] edgeLabelIds = graph.mapElName2Id(edgeLabels);

        ExtendableIterator<Edge> results = new ExtendableIterator<>();
        for (Directions dir : directions) {
            ConditionQuery query = GraphTransaction.constructEdgesQuery(
                                   (Id) vertex.id(), dir, edgeLabelIds);

            // Query by sort-keys
            boolean bySortKeys = false;
            if (withEdgeCond && edgeLabels.length > 0) {
                TraversalUtil.fillConditionQuery(conditions, query, graph);
                if (GraphTransaction.matchEdgeSortKeys(query, graph)) {
                    bySortKeys = true;
                } else {
                    // Can't query by sysprop and by index(HugeGraph-749)
                    query.resetUserpropConditions();
                }
            }

            // Query by has(id)
            if (!query.ids().isEmpty()) {
                // Ignore conditions if query by edge id in has-containers
                // FIXME: should check that the edge id matches the `vertex`
                query.resetConditions();
                LOG.warn("It's not recommended to query by has(id)");
            }

            query = this.injectQueryInfo(query);

            // Do query
            Iterator<Edge> edges = graph.edges(query);

            // Do filter by edge conditions
            if (withEdgeCond && !bySortKeys) {
                edges = TraversalUtil.filterResult(this.hasContainers, edges);
            }

            results.extend(edges);
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
    public void addHasContainer(final HasContainer has) {
        if (SYSPROP_PAGE.equals(has.getKey())) {
            this.setPage((String) has.getValue());
            return;
        }
        this.hasContainers.add(has);
    }

    @Override
    public Query queryInfo() {
        return this.queryInfo;
    }

    @Override
    public Iterator<?> lastTimeResults() {
        return this.lastTimeResults;
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^
               this.queryInfo.hashCode() ^
               this.hasContainers.hashCode();
    }
}
