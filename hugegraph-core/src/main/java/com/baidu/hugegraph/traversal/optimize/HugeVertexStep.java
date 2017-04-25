package com.baidu.hugegraph.traversal.optimize;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
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
import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.backend.query.ConditionQuery;
import com.baidu.hugegraph.backend.query.IdQuery;
import com.baidu.hugegraph.structure.HugeEdge;
import com.baidu.hugegraph.type.HugeTypes;
import com.baidu.hugegraph.type.define.HugeKeys;
import com.google.common.collect.ImmutableList;

public final class HugeVertexStep<E extends Element>
        extends VertexStep<E> implements HasContainerHolder {

    private static final long serialVersionUID = -7850636388424382454L;

    private static final Logger logger = LoggerFactory.getLogger(HugeVertexStep.class);

    private final List<HasContainer> hasContainers = new LinkedList<>();;

    public HugeVertexStep(final VertexStep<E> originalVertexStep) {
        super(originalVertexStep.getTraversal(),
              originalVertexStep.getReturnClass(),
              originalVertexStep.getDirection(),
              originalVertexStep.getEdgeLabels());
        originalVertexStep.getLabels().forEach(this::addLabel);
    }

    @Override
    protected Iterator<E> flatMap(final Traverser.Admin<Vertex> traverser) {
        boolean queryVertex = Vertex.class.isAssignableFrom(getReturnClass());
        boolean queryEdge = Edge.class.isAssignableFrom(getReturnClass());
        assert queryVertex || queryEdge;
        return queryVertex ? vertices(traverser) : edges(traverser);
    }

    private Iterator<E> vertices(final Traverser.Admin<Vertex> traverser) {
        HugeGraph graph = (HugeGraph) traverser.get().graph();
        Vertex vertex = traverser.get();

        IdQuery query = new IdQuery(HugeTypes.VERTEX);
        Iterator<E> edges = this.edges(traverser);
        while (edges.hasNext()) {
            HugeEdge edge = (HugeEdge) edges.next();
            query.query(edge.otherVertex().id());
        }

        Iterator<Vertex> vertices = graph.vertices(query);
        if (logger.isDebugEnabled()) {
            logger.debug("HugeVertexStep.vertices(): "
                    + "adjacent vertices of {}={}, has={}",
                    vertex.id(),
                    ImmutableList.copyOf(vertices),
                    this.hasContainers);
        }

        // TODO: query by vertex index to optimize
        return HugeGraphStep.filterResult(this.hasContainers, vertices);
    }

    private Iterator<E> edges(final Traverser.Admin<Vertex> traverser) {
        HugeGraph graph = (HugeGraph) traverser.get().graph();

        Vertex vertex = traverser.get();
        Direction direction = this.getDirection();
        String[] edgeLabels = this.getEdgeLabels();

        logger.debug("HugeVertexStep.edges(): "
                + "vertex={}, direction={}, edgeLabels={}, has={}",
                vertex.id(), direction, edgeLabels, this.hasContainers);

        ConditionQuery query = new ConditionQuery(HugeTypes.EDGE);

        // edge source vertex
        // TODO: id should be serialized(bytes/string) by back-end store
        query.eq(HugeKeys.SOURCE_VERTEX, vertex.id().toString());

        // edge direction
        // TODO: direction should be serialized(code/string) by back-end store
        query.eq(HugeKeys.DIRECTION, direction.name());

        // edge labels
        if (edgeLabels.length == 1) {
            query.eq(HugeKeys.LABEL, edgeLabels[0]);
        } else if (edgeLabels.length > 1){
            // TODO: support query by multi edge labels
            // query.query(Condition.in(HugeKeys.LABEL, edgeLabels));
            throw new BackendException("Not support query by multi edge labels");
        } else {
            assert edgeLabels.length == 0;
        }

        // conditions (enable if query for edge else conditions for vertex)
        if (Edge.class.isAssignableFrom(getReturnClass())) {
            for (HasContainer has : this.hasContainers) {
                query.query(HugeGraphStep.convHasContainer2Condition(has));
            }
        }

        // do query
        return (Iterator<E>) graph.edges(query);
    }

    @Override
    public String toString() {
        if (this.hasContainers.isEmpty()) {
            return super.toString();
        } else {
            return StringFactory.stepString(
                    this,
                    getDirection(),
                    Arrays.asList(getEdgeLabels()),
                    getReturnClass().getSimpleName(),
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

    @Override
    public int hashCode() {
        return super.hashCode() ^ this.hasContainers.hashCode();
    }
}
