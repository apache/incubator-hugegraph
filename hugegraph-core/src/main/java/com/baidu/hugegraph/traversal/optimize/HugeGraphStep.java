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

import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.slf4j.Logger;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.query.ConditionQuery;
import com.baidu.hugegraph.backend.query.Query;
import com.baidu.hugegraph.backend.query.QueryResults;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.util.Log;

public final class HugeGraphStep<S, E extends Element>
             extends GraphStep<S, E> implements QueryHolder {

    private static final long serialVersionUID = -679873894532085972L;

    private static final Logger LOG = Log.logger(HugeGraphStep.class);

    private final List<HasContainer> hasContainers = new ArrayList<>();

    // Store limit/order-by
    private final Query queryInfo = new Query(HugeType.UNKNOWN);

    private Iterator<E> lastTimeResults = QueryResults.emptyIterator();

    public HugeGraphStep(final GraphStep<S, E> originGraphStep) {
        super(originGraphStep.getTraversal(),
              originGraphStep.getReturnClass(),
              originGraphStep.isStartStep(),
              originGraphStep.getIds());

        originGraphStep.getLabels().forEach(this::addLabel);

        boolean queryVertex = this.returnsVertex();
        boolean queryEdge = this.returnsEdge();
        assert queryVertex || queryEdge;
        this.setIteratorSupplier(() -> {
            Iterator<E> results = queryVertex ? this.vertices() : this.edges();
            this.lastTimeResults = results;
            return results;
        });
    }

    private Iterator<E> vertices() {
        LOG.debug("HugeGraphStep.vertices(): {}", this);

        HugeGraph graph = (HugeGraph) this.getTraversal().getGraph().get();
        // g.V().hasId(EMPTY_LIST) will set ids to null
        if (this.ids == null) {
            return QueryResults.emptyIterator();
        }
        if (this.ids.length > 0) {
            return TraversalUtil.filterResult(this.hasContainers,
                                              graph.vertices(this.ids));
        }

        Query query = null;
        if (this.hasContainers.isEmpty()) {
            // Query all
            query = new Query(HugeType.VERTEX);
        } else {
            ConditionQuery q = new ConditionQuery(HugeType.VERTEX);
            query = TraversalUtil.fillConditionQuery(this.hasContainers,
                                                     q, graph);
        }

        query = this.injectQueryInfo(query);

        @SuppressWarnings("unchecked")
        Iterator<E> result = (Iterator<E>) graph.vertices(query);
        return result;
    }

    private Iterator<E> edges() {
        LOG.debug("HugeGraphStep.edges(): {}", this);

        HugeGraph graph = (HugeGraph) this.getTraversal().getGraph().get();

        if (this.ids != null && this.ids.length > 0) {
            return TraversalUtil.filterResult(this.hasContainers,
                                              graph.edges(this.ids));
        }

        Query query = null;

        if (this.hasContainers.isEmpty()) {
            /* Query all */
            query = new Query(HugeType.EDGE);
        } else {
            ConditionQuery q = new ConditionQuery(HugeType.EDGE);
            query = TraversalUtil.fillConditionQuery(this.hasContainers,
                                                     q, graph);
        }

        query = this.injectQueryInfo(query);

        @SuppressWarnings("unchecked")
        Iterator<E> result = (Iterator<E>) graph.edges(query);
        return result;
    }

    @Override
    public String toString() {
        if (this.hasContainers.isEmpty()) {
            return super.toString();
        }

        return this.ids.length == 0 ?
               StringFactory.stepString(this,
                                        this.returnClass.getSimpleName(),
                                        this.hasContainers) :
               StringFactory.stepString(this,
                                        this.returnClass.getSimpleName(),
                                        Arrays.toString(this.ids),
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
