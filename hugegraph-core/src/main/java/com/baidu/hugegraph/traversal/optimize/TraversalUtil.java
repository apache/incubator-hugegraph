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
import java.util.Iterator;
import java.util.List;
import java.util.function.BiPredicate;
import java.util.function.Function;

import org.apache.tinkerpop.gremlin.process.traversal.Compare;
import org.apache.tinkerpop.gremlin.process.traversal.Contains;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.HasContainerHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.FilterStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.RangeGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.CountGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.NoOpBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.OrderGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.IdentityStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ElementValueComparator;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.util.AndP;
import org.apache.tinkerpop.gremlin.process.traversal.util.ConnectiveP;
import org.apache.tinkerpop.gremlin.process.traversal.util.OrP;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.page.PageInfo;
import com.baidu.hugegraph.backend.query.Condition;
import com.baidu.hugegraph.backend.query.Condition.Relation;
import com.baidu.hugegraph.backend.query.Condition.RelationType;
import com.baidu.hugegraph.backend.query.ConditionQuery;
import com.baidu.hugegraph.backend.query.Query;
import com.baidu.hugegraph.iterator.FilterIterator;
import com.baidu.hugegraph.schema.PropertyKey;
import com.baidu.hugegraph.schema.SchemaLabel;
import com.baidu.hugegraph.structure.HugeEdge;
import com.baidu.hugegraph.structure.HugeVertex;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.define.Directions;
import com.baidu.hugegraph.type.define.HugeKeys;
import com.baidu.hugegraph.util.E;
import com.google.common.collect.ImmutableList;

public final class TraversalUtil {

    public static void extractHasContainer(HugeGraphStep<?, ?> newStep,
                                           Traversal.Admin<?, ?> traversal) {
        Step<?, ?> step = newStep;
        do {
            step = step.getNextStep();
            if (step instanceof HasStep) {
                HasContainerHolder holder = (HasContainerHolder) step;
                for (HasContainer has : holder.getHasContainers()) {
                    if (!GraphStep.processHasContainerIds(newStep, has)) {
                        newStep.addHasContainer(has);
                    }
                }
                TraversalHelper.copyLabels(step, step.getPreviousStep(), false);
                traversal.removeStep(step);
            }
        } while (step instanceof HasStep || step instanceof NoOpBarrierStep);
    }

    public static void extractHasContainer(HugeVertexStep<?> newStep,
                                           Traversal.Admin<?, ?> traversal) {
        Step<?, ?> step = newStep;
        do {
            if (step instanceof HasStep) {
                HasContainerHolder holder = (HasContainerHolder) step;
                for (HasContainer has : holder.getHasContainers()) {
                    newStep.addHasContainer(has);
                }
                TraversalHelper.copyLabels(step, step.getPreviousStep(), false);
                traversal.removeStep(step);
            }
            step = step.getNextStep();
        } while (step instanceof HasStep || step instanceof NoOpBarrierStep);
    }

    public static void extractOrder(Step<?, ?> newStep,
                                    Traversal.Admin<?, ?> traversal) {
        Step<?, ?> step = newStep;
        do {
            step = step.getNextStep();
            if (step instanceof OrderGlobalStep) {
                QueryHolder holder = (QueryHolder) newStep;
                @SuppressWarnings("resource")
                OrderGlobalStep<?, ?> orderStep = (OrderGlobalStep<?, ?>) step;
                orderStep.getComparators().forEach(comp -> {
                    ElementValueComparator<?> comparator =
                            (ElementValueComparator<?>) comp.getValue1();
                    holder.orderBy(comparator.getPropertyKey(),
                                   (Order) comparator.getValueComparator());
                });
                TraversalHelper.copyLabels(step, newStep, false);
                traversal.removeStep(step);
            }
            step = step.getNextStep();
        } while (step instanceof OrderGlobalStep ||
                 step instanceof IdentityStep);
    }

    public static void extractRange(Step<?, ?> newStep,
                                    Traversal.Admin<?, ?> traversal,
                                    boolean extractOnlyLimit) {
        QueryHolder holder = (QueryHolder) newStep;
        Step<?, ?> step = newStep;
        do {
            step = step.getNextStep();
            if (step instanceof RangeGlobalStep) {
                @SuppressWarnings("unchecked")
                RangeGlobalStep<Object> range = (RangeGlobalStep<Object>) step;
                /*
                 * NOTE: keep the step to limit results after query from DB
                 * due to `limit`(in DB) may not be implemented accurately.
                 * but the backend driver should ensure `offset` accurately.
                 */
                // TraversalHelper.copyLabels(step, newStep, false);
                // traversal.removeStep(step);
                if (extractOnlyLimit) {
                    // May need to retain offset for multiple sub-queries
                    holder.setRange(0, range.getHighRange());
                } else {
                    long limit = holder.setRange(range.getLowRange(),
                                                 range.getHighRange());
                    RangeGlobalStep<Object> newRange = new RangeGlobalStep<>(
                                                       traversal, 0, limit);
                    TraversalHelper.replaceStep(range, newRange, traversal);
                }
            }
        } while (step instanceof RangeGlobalStep ||
                 step instanceof IdentityStep ||
                 step instanceof NoOpBarrierStep);
    }

    public static void extractCount(Step<?, ?> newStep,
                                    Traversal.Admin<?, ?> traversal) {
        Step<?, ?> step = newStep;
        do {
            step = step.getNextStep();
            if (step instanceof CountGlobalStep) {
                QueryHolder holder = (QueryHolder) newStep;
                holder.setCount();
            }
        } while (step instanceof CountGlobalStep ||
                 step instanceof FilterStep ||
                 step instanceof IdentityStep ||
                 step instanceof NoOpBarrierStep);
    }

    public static ConditionQuery fillConditionQuery(
                                 List<HasContainer> hasContainers,
                                 ConditionQuery query,
                                 HugeGraph graph) {
        HugeType resultType = query.resultType();

        for (HasContainer has : hasContainers) {
            Condition condition = convHas2Condition(has, resultType, graph);
            query.query(condition);
        }
        return query;
    }

    public static Condition convHas2Condition(HasContainer has,
                                              HugeType type,
                                              HugeGraph graph) {
        P<?> p = has.getPredicate();
        E.checkArgument(p != null, "The predicate of has(%s) is null", has);
        BiPredicate<?, ?> bp = p.getBiPredicate();
        Condition condition;
        if (keyForContainsKeyOrValue(has.getKey())) {
            condition = convContains2Relation(graph, has);
        } else if (bp instanceof Compare) {
            condition = convCompare2Relation(graph, type, has);
        } else if (bp instanceof RelationType) {
            condition = convRelationType2Relation(graph, type, has);
        } else if (bp instanceof Contains) {
            condition = convIn2Relation(graph, type, has);
        } else if (p instanceof AndP) {
            condition = convAnd(graph, type, has);
        } else if (p instanceof OrP) {
            condition = convOr(graph, type, has);
        } else {
            // TODO: deal with other Predicate
            throw newUnsupportedPredicate(p);
        }
        return condition;
    }

    public static Condition convAnd(HugeGraph graph,
                                    HugeType type,
                                    HasContainer has) {
        P<?> p = has.getPredicate();
        assert p instanceof AndP;
        @SuppressWarnings("unchecked")
        List<P<Object>> predicates = ((AndP<Object>) p).getPredicates();
        if (predicates.size() < 2) {
            throw newUnsupportedPredicate(p);
        }

        Condition cond = null;
        for (P<Object> predicate : predicates) {
            HasContainer newHas = new HasContainer(has.getKey(), predicate);
            Condition newCond = convHas2Condition(newHas, type, graph);
            if (cond == null) {
                cond = newCond;
            } else {
                cond = Condition.and(newCond, cond);
            }
        }
        return cond;
    }

    public static Condition convOr(HugeGraph graph,
                                   HugeType type,
                                   HasContainer has) {
        P<?> p = has.getPredicate();
        assert p instanceof OrP;
        @SuppressWarnings("unchecked")
        List<P<Object>> predicates = ((OrP<Object>) p).getPredicates();
        if (predicates.size() < 2) {
            throw newUnsupportedPredicate(p);
        }

        Condition cond = null;
        for (P<Object> predicate : predicates) {
            HasContainer newHas = new HasContainer(has.getKey(), predicate);
            Condition newCond = convHas2Condition(newHas, type, graph);
            if (cond == null) {
                cond = newCond;
            } else {
                cond = Condition.or(newCond, cond);
            }
        }
        return cond;
    }

    private static Relation convCompare2Relation(HugeGraph graph,
                                                 HugeType type,
                                                 HasContainer has) {
        assert type.isGraph();
        BiPredicate<?, ?> bp = has.getPredicate().getBiPredicate();
        assert bp instanceof Compare;

        boolean isSyspropKey = true;
        try {
            string2HugeKey(has.getKey());
        } catch (IllegalArgumentException ignored) {
            isSyspropKey = false;
        }

        return isSyspropKey ?
               convCompare2SyspropRelation(graph, type, has) :
               convCompare2UserpropRelation(graph, type, has);
    }


    private static Relation convCompare2SyspropRelation(HugeGraph graph,
                                                        HugeType type,
                                                        HasContainer has) {
        BiPredicate<?, ?> bp = has.getPredicate().getBiPredicate();
        assert bp instanceof Compare;

        HugeKeys key = string2HugeKey(has.getKey());
        Object value = convSysValueIfNeeded(graph, type, key, has.getValue());

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

        throw newUnsupportedPredicate(has.getPredicate());
    }

    private static Relation convCompare2UserpropRelation(HugeGraph graph,
                                                         HugeType type,
                                                         HasContainer has) {
        BiPredicate<?, ?> bp = has.getPredicate().getBiPredicate();
        assert bp instanceof Compare;

        String key = has.getKey();
        PropertyKey pkey = graph.propertyKey(key);
        Id pkeyId = pkey.id();
        Object value = validPredicateValue(has.getValue(), pkey);

        switch ((Compare) bp) {
            case eq:
                return Condition.eq(pkeyId, value);
            case gt:
                return Condition.gt(pkeyId, value);
            case gte:
                return Condition.gte(pkeyId, value);
            case lt:
                return Condition.lt(pkeyId, value);
            case lte:
                return Condition.lte(pkeyId, value);
            case neq:
                return Condition.neq(pkeyId, value);
        }

        throw newUnsupportedPredicate(has.getPredicate());
    }

    private static Condition convRelationType2Relation(HugeGraph graph,
                                                       HugeType type,
                                                       HasContainer has) {
        assert type.isGraph();
        BiPredicate<?, ?> bp = has.getPredicate().getBiPredicate();
        assert bp instanceof RelationType;

        String key = has.getKey();
        PropertyKey pkey = graph.propertyKey(key);
        Id pkeyId = pkey.id();
        Object value = validPredicateValue(has.getValue(), pkey);
        return new Condition.UserpropRelation(pkeyId, (RelationType) bp, value);
    }

    public static Condition convIn2Relation(HugeGraph graph,
                                            HugeType type,
                                            HasContainer has) {
        BiPredicate<?, ?> bp = has.getPredicate().getBiPredicate();
        assert bp instanceof Contains;
        List<?> values = (List<?>) has.getValue();

        String originKey = has.getKey();
        if (values.size() > 1) {
            E.checkArgument(!originKey.equals(T.key) &&
                            !originKey.equals(T.value),
                            "Not support hasKey() or hasValue() with " +
                            "multiple values");
        }

        HugeKeys hugeKey = null;
        try {
            hugeKey = string2HugeKey(originKey);
        } catch (IllegalArgumentException ignored) {
            // Ignore
        }

        if (hugeKey != null) {
            values = convSysListValueIfNeeded(graph, type, hugeKey, values);

            switch ((Contains) bp) {
                case within:
                    return Condition.in(hugeKey, values);
                case without:
                    return Condition.nin(hugeKey, values);
            }
        } else {
            String key = has.getKey();
            PropertyKey pkey = graph.propertyKey(key);

            switch ((Contains) bp) {
                case within:
                    return Condition.in(pkey.id(), values);
                case without:
                    return Condition.nin(pkey.id(), values);
            }
        }

        throw newUnsupportedPredicate(has.getPredicate());
    }

    public static Condition convContains2Relation(HugeGraph graph,
                                                  HasContainer has) {
        // Convert contains-key or contains-value
        BiPredicate<?, ?> bp = has.getPredicate().getBiPredicate();
        E.checkArgument(bp == Compare.eq, "CONTAINS query with relation " +
                        "'%s' is not supported", bp);

        HugeKeys key = string2HugeKey(has.getKey());
        Object value = has.getValue();

        if (keyForContainsKey(has.getKey())) {
            if (value instanceof String) {
                value = graph.propertyKey((String) value).id();
            }
            return Condition.containsKey(key, value);
        } else {
            assert keyForContainsValue(has.getKey());
            return Condition.containsValue(key, value);
        }
    }

    public static BackendException newUnsupportedPredicate(P<?> predicate) {
        return new BackendException("Unsupported predicate: '%s'", predicate);
    }

    public static HugeKeys string2HugeKey(String key) {
        if (key.equals(T.label.getAccessor())) {
            return HugeKeys.LABEL;
        } else if (key.equals(T.id.getAccessor())) {
            return HugeKeys.ID;
        } else if (keyForContainsKeyOrValue(key)) {
            return HugeKeys.PROPERTIES;
        }
        return HugeKeys.valueOf(key);
    }

    public static boolean keyForContainsKeyOrValue(String key) {
        return key.equals(T.key.getAccessor()) ||
               key.equals(T.value.getAccessor());
    }

    public static boolean keyForContainsKey(String key) {
        return key.equals(T.key.getAccessor());
    }

    public static boolean keyForContainsValue(String key) {
        return key.equals(T.value.getAccessor());
    }

    @SuppressWarnings("unchecked")
    public static <V> Iterator<V> filterResult(
                                  List<HasContainer> hasContainers,
                                  Iterator<? extends Element> iterator) {
        Iterator<?> result = new FilterIterator<>(iterator, elem -> {
            return HasContainer.testAll(elem, hasContainers);
        });
        return (Iterator<V>) result;
    }

    public static void convAllHasSteps(Traversal.Admin<?, ?> traversal) {
        // Extract all has steps in traversal
        @SuppressWarnings("rawtypes")
        List<HasStep> steps = TraversalHelper
                              .getStepsOfAssignableClassRecursively(
                              HasStep.class, traversal);
        HugeGraph graph = (HugeGraph) traversal.getGraph().get();
        for (HasStep<?> step : steps) {
            TraversalUtil.convHasStep(graph, step);
        }
    }

    public static void convHasStep(HugeGraph graph, HasStep<?> step) {
        HasContainerHolder holder = step;
        for (HasContainer has : holder.getHasContainers()) {
            convPredicateValue(graph, has);
        }
    }

    private static void convPredicateValue(HugeGraph graph, HasContainer has) {
        // No need to convert if key is sysprop
        if (isSysProp(has.getKey())) {
            return;
        }
        PropertyKey pkey = graph.propertyKey(has.getKey());

        List<P<Object>> leafPredicates = new ArrayList<>();
        collectPredicates(leafPredicates, ImmutableList.of(has.getPredicate()));
        for (P<Object> predicate : leafPredicates) {
            Object value = validPredicateValue(predicate.getValue(), pkey);
            predicate.setValue(value);
        }
    }

    private static boolean isSysProp(String key) {
        if (QueryHolder.SYSPROP_PAGE.equals(key)) {
            return true;
        }
        try {
            string2HugeKey(key);
            // Come here if key is ~id, ~label, ~key and ~value
            return true;
        } catch (IllegalArgumentException e) {
            // Ignore
            return false;
        }
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private static void collectPredicates(List<P<Object>> results,
                                          List<P<?>> predicates) {
        for (P<?> p : predicates) {
            if (p instanceof ConnectiveP) {
                collectPredicates(results, ((ConnectiveP) p).getPredicates());
            } else {
                results.add((P<Object>) p);
            }
        }
    }

    private static Object convSysValueIfNeeded(HugeGraph graph,HugeType type,
                                               HugeKeys key, Object value) {
        if (key == HugeKeys.LABEL && !(value instanceof Id)) {
            value = SchemaLabel.getLabelId(graph, type, value);
        } else if (key == HugeKeys.ID && !(value instanceof Id)) {
            if (type.isVertex()) {
                value = HugeVertex.getIdValue(value);
            } else {
                value = HugeEdge.getIdValue(value);
            }
        }
        return value;
    }

    private static List<?> convSysListValueIfNeeded(HugeGraph graph,
                                                    HugeType type,
                                                    HugeKeys key,
                                                    List<?> values) {
        List<Object> newValues = new ArrayList<>(values.size());
        for (Object value : values) {
            newValues.add(convSysValueIfNeeded(graph, type, key, value));
        }
        return newValues;
    }

    public static Iterator<Edge> filterResult(Vertex vertex,
                                              Directions dir,
                                              Iterator<Edge> edges) {
        final List<Edge> list = new ArrayList<>();
        while (edges.hasNext()) {
            Edge edge = edges.next();
            if (dir == Directions.OUT && vertex.equals(edge.outVertex()) ||
                dir == Directions.IN && vertex.equals(edge.inVertex())) {
                list.add(edge);
            }
        }
        return list.iterator();
    }

    public static Query.Order convOrder(Order order) {
        return order == Order.decr ? Query.Order.DESC : Query.Order.ASC;
    }

    private static <V> V validPredicateValue(V value, PropertyKey pkey) {
        V validValue = pkey.convValue(value, false);
        E.checkArgumentNotNull(validValue,
                               "Invalid data type of query value, " +
                               "expect '%s', actual '%s'",
                               pkey.dataType().clazz(),
                               value.getClass());
        return validValue;
    }

    public static void retriveSysprop(List<HasContainer> hasContainers,
                                      Function<HasContainer, Boolean> func) {
        for (Iterator<HasContainer> iter = hasContainers.iterator();
             iter.hasNext();) {
            HasContainer container = iter.next();
            if (container.getKey().startsWith("~") && func.apply(container)) {
                iter.remove();
            }
        }
    }

    public static String page(GraphTraversal<?, ?> traversal) {
        QueryHolder holder = rootStep(traversal);
        E.checkState(holder != null,
                     "Invalid paging traversal: %s", traversal.getClass());
        return (String) holder.metadata(PageInfo.PAGE);
    }

    public static QueryHolder rootStep(GraphTraversal<?, ?> traversal) {
        for (final Step<?, ?> step : traversal.asAdmin().getSteps()) {
            if (step instanceof QueryHolder) {
                return (QueryHolder) step;
            }
        }
        return null;
    }
}
