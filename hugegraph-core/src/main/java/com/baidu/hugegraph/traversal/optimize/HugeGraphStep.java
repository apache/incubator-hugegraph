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
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.baidu.hugegraph.traversal.optimize;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.function.BiPredicate;

import org.apache.tinkerpop.gremlin.process.traversal.Compare;
import org.apache.tinkerpop.gremlin.process.traversal.Contains;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.step.HasContainerHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.util.AndP;
import org.apache.tinkerpop.gremlin.process.traversal.util.OrP;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.backend.query.Condition;
import com.baidu.hugegraph.backend.query.Condition.Relation;
import com.baidu.hugegraph.backend.query.ConditionQuery;
import com.baidu.hugegraph.backend.query.Query;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.define.HugeKeys;

public final class HugeGraphStep<S, E extends Element>
             extends GraphStep<S, E> implements HasContainerHolder {

    private static final long serialVersionUID = -679873894532085972L;

    private static final Logger logger =
                         LoggerFactory.getLogger(HugeGraphStep.class);

    private final List<HasContainer> hasContainers = new ArrayList<>();
    private long limit = Query.NO_LIMIT;
    private long offset = 0;

    public HugeGraphStep(final GraphStep<S, E> originGraphStep) {
        super(originGraphStep.getTraversal(),
              originGraphStep.getReturnClass(),
              originGraphStep.isStartStep(),
              originGraphStep.getIds());

        originGraphStep.getLabels().forEach(this::addLabel);

        boolean queryVertex = Vertex.class.isAssignableFrom(this.returnClass);
        boolean queryEdge = Edge.class.isAssignableFrom(this.returnClass);
        assert queryVertex || queryEdge;
        this.setIteratorSupplier(() -> {
            return queryVertex ? this.vertices() : this.edges();
        });
    }

    private Iterator<E> vertices() {
        logger.debug("HugeGraphStep.vertices(): {}", this);

        HugeGraph graph = (HugeGraph) this.getTraversal().getGraph().get();
        if (this.ids != null && this.ids.length > 0) {
            return filterResult(this.hasContainers, graph.vertices(this.ids));
        }

        Query query = null;
        if (this.hasContainers.isEmpty()) {
            /* Query all */
            query = new Query(HugeType.VERTEX);
        } else {
            ConditionQuery q = new ConditionQuery(HugeType.VERTEX);
            query = HugeGraphStep.fillConditionQuery(this.hasContainers, q);
        }

        query.offset(this.offset);
        query.limit(this.limit);

        @SuppressWarnings("unchecked")
        Iterator<E> result = (Iterator<E>) graph.vertices(query);
        return  result;
    }

    private Iterator<E> edges() {
        logger.debug("HugeGraphStep.edges(): {}", this);

        HugeGraph graph = (HugeGraph) this.getTraversal().getGraph().get();

        if (this.ids != null && this.ids.length > 0) {
            return filterResult(this.hasContainers, graph.edges(this.ids));
        }

        Query query = null;

        if (this.hasContainers.isEmpty()) {
            /* Query all */
            query = new Query(HugeType.EDGE);
        } else {
            ConditionQuery q = new ConditionQuery(HugeType.EDGE);
            query = HugeGraphStep.fillConditionQuery(this.hasContainers, q);
        }

        query.offset(this.offset);
        /*
         * NOTE: double limit because of duplicate edges(when BOTH Direction)
         * TODO: the `this.limit * 2` maybe will overflow.
         */
        query.limit(this.limit == Query.NO_LIMIT ?
                    Query.NO_LIMIT :
                    this.limit << 1);

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
    public void addHasContainer(final HasContainer hasContainer) {
        this.hasContainers.add(hasContainer);
    }

    public void setRange(long start, long end) {
        if (end >= start) {
            this.offset = start;
            this.limit = end - start;
        } else {
            this.offset = 0;
            this.limit = Query.NO_LIMIT;
        }
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ this.hasContainers.hashCode();
    }

    public static ConditionQuery fillConditionQuery(
            List<HasContainer> hasContainers,
            ConditionQuery query) {
        for (HasContainer has : hasContainers) {
            BiPredicate<?, ?> bp = has.getPredicate().getBiPredicate();
            if (bp instanceof Compare) {
                query.query(HugeGraphStep.convCompare2Relation(has));
            } else if (bp instanceof Contains) {
                query.query(HugeGraphStep.convContains2Relation(has));
            } else if (has.getPredicate() instanceof AndP) {
                query.query(HugeGraphStep.convAnd(has));
            } else if (has.getPredicate() instanceof OrP) {
                query.query(HugeGraphStep.convOr(has));
            } else {
                // TODO: deal with other Predicate
                throw newUnsupportedPredicate(has.getPredicate());
            }
        }

        return query;
    }

    public static Condition convAnd(HasContainer has) {
        P<?> p = has.getPredicate();
        assert p instanceof AndP;
        @SuppressWarnings("unchecked")
        List<P<Object>> predicates = ((AndP<Object>) p).getPredicates();
        if (predicates.size() != 2) {
            throw newUnsupportedPredicate(p);
        }

        /* Just for supporting P.inside() / P.between() */
        return Condition.and(
                HugeGraphStep.convCompare2Relation(
                        new HasContainer(has.getKey(), predicates.get(0))),
                HugeGraphStep.convCompare2Relation(
                        new HasContainer(has.getKey(), predicates.get(1))));
    }

    public static Condition convOr(HasContainer has) {
        P<?> p = has.getPredicate();
        assert p instanceof OrP;
        // TODO: support P.outside() which is implemented by OR
        throw newUnsupportedPredicate(p);
    }

    public static Relation convCompare2Relation(HasContainer has) {
        BiPredicate<?, ?> bp = has.getPredicate().getBiPredicate();
        assert bp instanceof Compare;

        try {
            HugeKeys key = string2HugeKey(has.getKey());
            Object value = has.getValue();

            switch ((Compare) bp) {
                case eq:
                    return Condition.eq(key, value);
                case gt:
                    return Condition.gt(key, value);
                case gte:
                    return Condition.gte(key, value);
                case lt:
                    return Condition.lt(key, value);
                case lte:
                    return Condition.lte(key, value);
                case neq:
                    return Condition.neq(key, value);
            }
        } catch (IllegalArgumentException e) {
            String key = has.getKey();
            Object value = has.getValue();

            switch ((Compare) bp) {
                case eq:
                    return Condition.eq(key, value);
                case gt:
                    return Condition.gt(key, value);
                case gte:
                    return Condition.gte(key, value);
                case lt:
                    return Condition.lt(key, value);
                case lte:
                    return Condition.lte(key, value);
                case neq:
                    return Condition.neq(key, value);
            }
        }

        throw newUnsupportedPredicate(has.getPredicate());
    }

    public static Condition convContains2Relation(HasContainer has) {
        BiPredicate<?, ?> bp = has.getPredicate().getBiPredicate();
        assert bp instanceof Contains;
        List<?> value = (List<?>) has.getValue();

        try {
            HugeKeys key = string2HugeKey(has.getKey());

            switch ((Contains) bp) {
                case within:
                    return Condition.in(key, value);
                case without:
                    return Condition.nin(key, value);
            }
        } catch (IllegalArgumentException e) {
            String key = has.getKey();

            switch ((Contains) bp) {
                case within:
                    return Condition.in(key, value);
                case without:
                    return Condition.nin(key, value);
            }
        }

        throw newUnsupportedPredicate(has.getPredicate());
    }

    private static BackendException newUnsupportedPredicate(P<?> predicate) {
        return new BackendException("Unsupported predicate: '%s'", predicate);
    }

    public static HugeKeys string2HugeKey(String key) {
        if (key.equals(T.label.getAccessor())) {
            return HugeKeys.LABEL;
        } else if (key.equals(T.id.getAccessor())) {
            return HugeKeys.ID;
        } else if (key.equals(T.key.getAccessor())) {
            return HugeKeys.PROPERTY_KEY;
        } else if (key.equals(T.value.getAccessor())) {
            return HugeKeys.PROPERTY_VALUE;
        }
        return HugeKeys.valueOf(key);
    }

    public static <E> Iterator<E> filterResult(
            List<HasContainer> hasContainers,
            Iterator<? extends Element> iterator) {
        final List<E> list = new ArrayList<>();

        while (iterator.hasNext()) {
            final Element elem = iterator.next();
            if (HasContainer.testAll(elem, hasContainers)) {
                @SuppressWarnings("unchecked")
                E e = (E) elem;
                list.add(e);
            }
        }
        return list.iterator();
    }
}
