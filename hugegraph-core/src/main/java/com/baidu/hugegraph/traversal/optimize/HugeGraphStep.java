package com.baidu.hugegraph.traversal.optimize;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.function.BiPredicate;

import org.apache.tinkerpop.gremlin.process.traversal.Compare;
import org.apache.tinkerpop.gremlin.process.traversal.step.HasContainerHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
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
import com.baidu.hugegraph.backend.query.ConditionQuery;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.define.HugeKeys;

public final class HugeGraphStep<S, E extends Element>
        extends GraphStep<S, E> implements HasContainerHolder {

    private static final long serialVersionUID = -679873894532085972L;

    private static final Logger logger = LoggerFactory.getLogger(HugeGraphStep.class);

    private final List<HasContainer> hasContainers = new LinkedList<>();

    private long offset = 0;

    private long limit = 1000;

    public HugeGraphStep(final GraphStep<S, E> originalGraphStep) {
        super(originalGraphStep.getTraversal(),
              originalGraphStep.getReturnClass(),
              originalGraphStep.isStartStep(),
              originalGraphStep.getIds());

        originalGraphStep.getLabels().forEach(this::addLabel);

        boolean queryVertex = Vertex.class.isAssignableFrom(this.returnClass);
        boolean queryEdge = Edge.class.isAssignableFrom(this.returnClass);
        assert queryVertex || queryEdge;
        this.setIteratorSupplier(() -> (
                queryVertex ? this.vertices() : this.edges()));
    }

    private Iterator<E> vertices() {
        logger.debug("HugeGraphStep.vertices(): {}", this);

        HugeGraph graph = (HugeGraph) this.getTraversal().getGraph().get();
        if (this.ids != null && this.ids.length > 0) {
            return filterResult(this.hasContainers,
                    graph.vertices(this.ids));
        } else {
            ConditionQuery query = new ConditionQuery(HugeType.VERTEX);
            query.offset(this.offset);
            query.limit(this.limit);
            for (HasContainer condition : this.hasContainers) {
                query.query(convHasContainer2Condition(condition));
            }
            return (Iterator<E>) graph.vertices(query);
        }
    }

    private Iterator<E> edges() {
        logger.debug("HugeGraphStep.edges(): {}", this);

        HugeGraph graph = (HugeGraph) this.getTraversal().getGraph().get();
        if (this.ids != null && this.ids.length > 0) {
            return filterResult(this.hasContainers,
                    graph.edges(this.ids));
        } else {
            ConditionQuery query = new ConditionQuery(HugeType.EDGE);
            query.offset(this.offset);
            query.limit(this.limit);
            for (HasContainer has : this.hasContainers) {
                query.query(convHasContainer2Condition(has));
            }
            return (Iterator<E>) graph.edges(query);
        }
    }

    @Override
    public String toString() {
        if (this.hasContainers.isEmpty()) {
            return super.toString();
        } else {
            return this.ids.length == 0 ?
                    StringFactory.stepString(this,
                            this.returnClass.getSimpleName(),
                            this.hasContainers) :
                    StringFactory.stepString(this,
                            this.returnClass.getSimpleName(),
                            Arrays.toString(this.ids),
                            this.hasContainers);
        }
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
        assert end > start;
        this.offset = start;
        this.limit = end - start;
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ this.hasContainers.hashCode();
    }

    public static Condition convHasContainer2Condition(HasContainer has) {
        try {
            HugeKeys key = string2HugeKey(has.getKey());
            Object value = has.getValue();

            BiPredicate<?, ?> bp = has.getPredicate().getBiPredicate();
            if (bp.equals(Compare.eq)) {
                return Condition.eq(key, value);
            } else if (bp.equals(Compare.gt)) {
                return Condition.gt(key, value);
            } else if (bp.equals(Compare.gte)) {
                return Condition.gte(key, value);
            } else if (bp.equals(Compare.lt)) {
                return Condition.lt(key, value);
            } else if (bp.equals(Compare.lte)) {
                return Condition.lte(key, value);
            } else if (bp.equals(Compare.neq)) {
                return Condition.neq(key, value);
            } else {
                // TODO: deal with other Predicate
                throw new BackendException("Not supported condition: " + bp);
            }
        } catch (IllegalArgumentException e) {
            String key = has.getKey();
            Object value = has.getValue();

            BiPredicate<?, ?> bp = has.getPredicate().getBiPredicate();
            if (bp.equals(Compare.eq)) {
                return Condition.eq(key, value);
            } else if (bp.equals(Compare.gt)) {
                return Condition.gt(key, value);
            } else if (bp.equals(Compare.gte)) {
                return Condition.gte(key, value);
            } else if (bp.equals(Compare.lt)) {
                return Condition.lt(key, value);
            } else if (bp.equals(Compare.lte)) {
                return Condition.lte(key, value);
            } else if (bp.equals(Compare.neq)) {
                return Condition.neq(key, value);
            } else {
                // TODO: deal with other Predicate
                throw new BackendException("Not supported condition: " + bp);
            }
        }
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
        final List<E> list = new LinkedList<>();

        while (iterator.hasNext()) {
            final Element e = iterator.next();
            if (HasContainer.testAll(e, hasContainers)) {
                list.add((E) e);
            }
        }
        return list.iterator();
    }
}
