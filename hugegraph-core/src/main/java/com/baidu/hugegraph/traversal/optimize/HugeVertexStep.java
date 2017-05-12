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
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.query.ConditionQuery;
import com.baidu.hugegraph.backend.tx.GraphTransaction;

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

    @SuppressWarnings("unchecked")
    @Override
    protected Iterator<E> flatMap(final Traverser.Admin<Vertex> traverser) {
        boolean queryVertex = Vertex.class.isAssignableFrom(getReturnClass());
        boolean queryEdge = Edge.class.isAssignableFrom(getReturnClass());
        assert queryVertex || queryEdge;
        if (queryVertex) {
            return (Iterator<E>) this.vertices(traverser);
        } else {
            return (Iterator<E>) this.edges(traverser);
        }
    }

    private Iterator<Vertex> vertices(final Traverser.Admin<Vertex> traverser) {
        HugeGraph graph = (HugeGraph) traverser.get().graph();
        Vertex vertex = traverser.get();

        Iterator<Edge> edges = this.edges(traverser);
        Iterator<Vertex> vertices = graph.adjacentVertices(edges);
        if (logger.isDebugEnabled()) {
            logger.debug("HugeVertexStep.vertices(): "
                    + "is there adjacent vertices of {}: {}, has={}",
                    vertex.id(),
                    vertices.hasNext(),
                    this.hasContainers);
        }

        // TODO: query by vertex index to optimize
        return HugeGraphStep.filterResult(this.hasContainers, vertices);
    }

    private Iterator<Edge> edges(final Traverser.Admin<Vertex> traverser) {
        HugeGraph graph = (HugeGraph) traverser.get().graph();

        Vertex vertex = traverser.get();
        Direction direction = this.getDirection();
        String[] edgeLabels = this.getEdgeLabels();

        logger.debug("HugeVertexStep.edges(): "
                + "vertex={}, direction={}, edgeLabels={}, has={}",
                vertex.id(), direction, edgeLabels, this.hasContainers);

        ConditionQuery query = GraphTransaction.constructEdgesQuery(
                (Id) vertex.id(), direction, edgeLabels);

        // conditions (enable if query for edge else conditions for vertex)
        if (Edge.class.isAssignableFrom(getReturnClass())) {
            for (HasContainer has : this.hasContainers) {
                query.query(HugeGraphStep.convHasContainer2Condition(has));
            }
        }

        // do query
        return graph.edges(query);
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
