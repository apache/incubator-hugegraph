package com.baidu.hugegraph.backend.tx;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.id.IdGeneratorFactory;
import com.baidu.hugegraph.backend.id.SplicingIdGenerator;
import com.baidu.hugegraph.backend.query.ConditionQuery;
import com.baidu.hugegraph.backend.query.IdQuery;
import com.baidu.hugegraph.backend.query.Query;
import com.baidu.hugegraph.backend.store.BackendEntry;
import com.baidu.hugegraph.backend.store.BackendStore;
import com.baidu.hugegraph.schema.SchemaManager;
import com.baidu.hugegraph.structure.HugeEdge;
import com.baidu.hugegraph.structure.HugeElement;
import com.baidu.hugegraph.structure.HugeFeatures;
import com.baidu.hugegraph.structure.HugeFeatures.HugeVertexFeatures;
import com.baidu.hugegraph.structure.HugeVertex;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.define.HugeKeys;
import com.baidu.hugegraph.type.schema.VertexLabel;
import com.baidu.hugegraph.util.CollectionUtil;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

public class GraphTransaction extends AbstractTransaction {

    private static final Logger logger = LoggerFactory.getLogger(GraphTransaction.class);

    private IndexTransaction indexTx;

    private Set<HugeVertex> addedVertexes;
    private Set<HugeVertex> removedVertexes;

    private Set<HugeEdge> addedEdges;
    private Set<HugeEdge> removedEdges;

    public GraphTransaction(final HugeGraph graph,
            BackendStore store, BackendStore indexStore) {
        super(graph, store);

        this.indexTx = new IndexTransaction(graph, indexStore);
        assert !this.indexTx.autoCommit();

        this.addedVertexes = new LinkedHashSet<>();
        this.removedVertexes = new LinkedHashSet<>();

        this.addedEdges = new LinkedHashSet<>();
        this.removedEdges = new LinkedHashSet<>();
    }

    @Override
    public boolean hasUpdates() {
        boolean empty = (this.addedVertexes.isEmpty()
                && this.removedVertexes.isEmpty()
                && this.addedEdges.isEmpty()
                && this.removedEdges.isEmpty());
        return !empty || super.hasUpdates();
    }

    @Override
    protected void prepareCommit() {
        // serialize and add updates into super.deletions
        this.prepareDeletions(this.removedVertexes, this.removedEdges);
        // serialize and add updates into super.additions
        this.prepareAdditions(this.addedVertexes, this.addedEdges);
    }

    protected void prepareAdditions(
            Set<HugeVertex> updatedVertexes,
            Set<HugeEdge> updatedEdges) {

        Map<Id, HugeVertex> vertexes = new HashMap<>();

        // copy updated vertexes(only with props, without edges)
        for (HugeVertex v : updatedVertexes) {
            vertexes.put(v.id(), v.prepareAdded());
        }

        // copy updated edges and merge into owner vertex
        for (HugeEdge edge : updatedEdges) {
            assert edge.type() == HugeType.EDGE_OUT;
            Id sourceId = edge.sourceVertex().id();

            if (!vertexes.containsKey(sourceId)) {
                vertexes.put(sourceId, edge.prepareAddedOut());
            } else {
                HugeVertex sourceVertex = vertexes.get(sourceId);
                sourceVertex.addOutEdge(edge.prepareAddedOut(sourceVertex));
            }
        }

        // ensure all the target vertexes (of out edges) are in vertexes
        for (HugeEdge edge : updatedEdges) {
            assert edge.type() == HugeType.EDGE_OUT;
            Id targetId = edge.targetVertex().id();

            if (!vertexes.containsKey(targetId)) {
                vertexes.put(targetId, edge.prepareAddedIn());
            } else {
                HugeVertex targetVertex = vertexes.get(targetId);
                targetVertex.addInEdge(edge.prepareAddedIn(targetVertex));
            }
        }

        // do update
        for (HugeVertex v : vertexes.values()) {
            // add vertex entry
            this.addEntry(this.serializer.writeVertex(v));

            if (v.existsProperties()) {
                // update index of vertex(include props and edges)
                this.indexTx.updateVertexIndex(v, false);
            } else {
                // update index of vertex edges
                this.indexTx.updateEdgesIndex(v, false);
            }
        }

        // clear updates
        updatedVertexes.clear();
        updatedEdges.clear();
    }

    protected void prepareDeletions(
            Set<HugeVertex> updatedVertexes,
            Set<HugeEdge> updatedEdges) {

        Set<HugeVertex> vertexes = new LinkedHashSet<>();
        vertexes.addAll(updatedVertexes);

        Set<HugeEdge> edges = new LinkedHashSet<>();
        edges.addAll(updatedEdges);

        // clear updates
        updatedVertexes.clear();
        updatedEdges.clear();

        // in order to remove edges of vertexes, query all edges first
        for (HugeVertex v : vertexes) {
            @SuppressWarnings({ "unchecked", "rawtypes" })
            Collection<HugeEdge> vedges = (Collection) ImmutableList.copyOf(
                    this.queryEdgesByVertex(v.id()));
            edges.addAll(vedges);
        }

        // remove vertexes
        for (HugeVertex v : vertexes) {
            // if the backend stores vertex together with edges,
            // its edges would be removed after removing vertex
            // if the backend stores vertex which is separated from edges,
            // its edges should be removed manually when removing vertex
            this.removeEntry(this.serializer.writeId(HugeType.VERTEX, v.id()));
            this.indexTx.updateVertexIndex(v, true);
        }

        // remove edges independently
        for (HugeEdge edge : edges) {
            // remove OUT
            HugeVertex vertex = edge.sourceVertex().prepareRemoved();
            vertex.addEdge(edge.prepareRemoved()); // only with edge id
            this.removeEntry(this.serializer.writeVertex(vertex));

            // remove IN
            vertex = edge.targetVertex().prepareRemoved();
            vertex.addEdge(edge.switchOwner().prepareRemoved());
            this.removeEntry(this.serializer.writeVertex(vertex));

            // update edge index
            this.indexTx.updateEdgeIndex(edge, true);
        }
    }

    @Override
    public void commit() throws BackendException {
        super.commit();
        this.indexTx.commit();
    }

    @Override
    public void rollback() throws BackendException {
        super.rollback();
        this.indexTx.rollback();
    }

    @Override
    public Iterable<BackendEntry> query(Query query) {
        if (query instanceof ConditionQuery) {
            query = this.optimizeQuery((ConditionQuery) query);
        }
        return super.query(query);
    }

    public Vertex addVertex(HugeVertex vertex) {
        this.beforeWrite();
        this.addedVertexes.add(vertex);
        this.afterWrite();
        return vertex;
    }

    public Vertex addVertex(Object... keyValues) {
        ElementHelper.legalPropertyKeyValueArray(keyValues);

        Id id = HugeVertex.getIdValue(keyValues);
        Object label = HugeVertex.getLabelValue(keyValues);

        HugeVertexFeatures features = this.graph.features().vertex();

        // Vertex id must be null now
        if (!features.supportsCustomIds() && id != null) {
            String msg = "User defined id of Vertex is not supported";
            throw new IllegalArgumentException(msg);
        }

        // check Vertex label
        if (label == null && features.supportsDefaultLabel()) {
            label = features.defaultLabel();
        }

        if (label == null) {
            throw Element.Exceptions.labelCanNotBeNull();
        } else if (label instanceof String) {
            SchemaManager schema = this.graph.schema();
            label = schema.vertexLabel((String) label);
        }

        assert (label instanceof VertexLabel);

        // check whether primaryKey exists
        Set<String> primaryKeys = ((VertexLabel) label).primaryKeys();
        Preconditions.checkArgument(CollectionUtil.containsAll(
                ElementHelper.getKeys(keyValues), primaryKeys),
                "The primary key(s) must be setted: %s", primaryKeys);

        // create HugeVertex
        HugeVertex vertex = new HugeVertex(this, id, (VertexLabel) label);
        // set properties
        ElementHelper.attachProperties(vertex, keyValues);

        // generate an id and assign it if not exists
        if (id == null) {
            vertex.assignId();
        }

        return this.addVertex(vertex);
    }

    public void removeVertex(HugeVertex vertex) {
        this.beforeWrite();
        this.removedVertexes.add(vertex);
        this.afterWrite();
    }

    public Iterator<Vertex> queryVertices(Object... vertexIds) {
        List<Vertex> list = new ArrayList<Vertex>(vertexIds.length);

        for (Object vertexId : vertexIds) {
            Id id = HugeElement.getIdValue(T.id, vertexId);
            BackendEntry entry = this.get(HugeType.VERTEX, id);
            Vertex vertex = this.serializer.readVertex(entry);
            assert vertex != null;
            list.add(vertex);
        }

        return list.iterator();
    }

    public Iterator<Vertex> queryVertices() {
        Query q = new Query(HugeType.VERTEX);
        return this.queryVertices(q);
    }

    public Iterator<Vertex> queryVertices(Query query) {
        List<Vertex> list = new ArrayList<Vertex>();

        Iterator<BackendEntry> entries = this.query(query).iterator();
        while (entries.hasNext()) {
            Vertex vertex = this.serializer.readVertex(entries.next());
            assert vertex != null;
            list.add(vertex);
        }

        return list.iterator();
    }

    public Iterator<Vertex> queryAdjacentVertices(Iterator<Edge> edges) {
        IdQuery query = new IdQuery(HugeType.VERTEX);
        while (edges.hasNext()) {
            HugeEdge edge = (HugeEdge) edges.next();
            query.query(edge.otherVertex().id());
        }

        return this.queryVertices(query);
    }

    public Edge addEdge(HugeEdge edge) {
        this.beforeWrite();
        this.addedEdges.add(edge);
        this.afterWrite();
        return edge;
    }

    public void removeEdge(HugeEdge edge) {
        this.beforeWrite();
        this.removedEdges.add(edge);
        this.afterWrite();
    }

    public Iterator<Edge> queryEdges(Object... edgeIds) {
        List<Edge> list = new ArrayList<Edge>(edgeIds.length);

        for (Object edgeId : edgeIds) {
            Id id = HugeElement.getIdValue(T.id, edgeId);
            BackendEntry entry = this.get(HugeType.EDGE, id);
            HugeVertex vertex = this.serializer.readVertex(entry);
            assert vertex != null;
            list.addAll(ImmutableList.copyOf(vertex.getEdges()));
        }

        return list.iterator();
    }

    public Iterator<Edge> queryEdges() {
        Query q = new Query(HugeType.EDGE);
        return this.queryEdges(q);
    }

    public Iterator<Edge> queryEdges(Query query) {
        Iterator<Vertex> vertices = this.queryVertices(query);

        Map<Id, Edge> results = new HashMap<>();
        while (vertices.hasNext()) {
            HugeVertex vertex = (HugeVertex) vertices.next();
            for (HugeEdge edge : vertex.getEdges()) {
                if (!results.containsKey(edge.id())) {
                    // NOTE: ensure all edges in results are OUT
                    results.put(edge.id(), edge.switchOutDirection());
                } else {
                    logger.debug("results contains edge: {}", edge);
                }
            }
        }

        return results.values().iterator();
    }

    public Iterator<Edge> queryEdgesByVertex(Id id) {
        return queryEdges(constructEdgesQuery(id, null));
    }

    public static ConditionQuery constructEdgesQuery(
            Id sourceVertex,
            Direction direction,
            String... edgeLabels) {

        Preconditions.checkNotNull(sourceVertex);
        Preconditions.checkArgument(
                (direction == null && edgeLabels.length == 0)
                || (direction != null && edgeLabels.length == 0)
                || (direction != null && edgeLabels.length != 0));

        ConditionQuery query = new ConditionQuery(HugeType.EDGE);

        // edge source vertex
        query.eq(HugeKeys.SOURCE_VERTEX, sourceVertex);

        // edge direction
        // TODO: deal with direction is BOTH
        if (direction != null) {
            query.eq(HugeKeys.DIRECTION, direction);
        }

        // edge labels
        if (edgeLabels.length == 1) {
            query.eq(HugeKeys.LABEL, edgeLabels[0]);
        } else if (edgeLabels.length > 1) {
            // TODO: support query by multi edge labels
            // query.query(Condition.in(HugeKeys.LABEL, edgeLabels));
            throw new BackendException("Not support query by multi edge-labels");
        } else {
            assert edgeLabels.length == 0;
        }

        return query;
    }

    protected Query optimizeQuery(ConditionQuery query) {
        HugeFeatures features = this.graph().features();

        // optimize vertex query
        Object label = query.condition(HugeKeys.LABEL);
        if (label != null && query.resultType() == HugeType.VERTEX
                && !features.vertex().supportsUserSuppliedIds()) {
            // query vertex by label + primary-values
            Set<String> keys = this.graph.schema().vertexLabel(
                    label.toString()).primaryKeys();
            if (!keys.isEmpty() && query.matchUserpropKeys(keys)) {
                String primaryValues = query.userpropValuesString();
                query.eq(HugeKeys.PRIMARY_VALUES, primaryValues);
                query.resetUserpropConditions();
                logger.debug("query vertices by primaryKeys: {}", query);

                // convert vertex-label + primary-key to vertex-id
                if (IdGeneratorFactory.supportSplicing()) {
                    Id id = SplicingIdGenerator.splicing(
                            label.toString(),
                            primaryValues);
                    query.query(id);
                    query.resetConditions();
                } else {
                    // assert this.store().supportsSysIndex();
                    logger.warn("Please ensure the backend supports"
                            + " query by primary-key: {}", query);
                }
                return query;
            }
        }

        // optimize edge query
        if (label != null && query.resultType() == HugeType.EDGE) {
            // query edge by sourceVertex + direction + label + sort-values
            Set<String> keys = this.graph.schema().edgeLabel(
                    label.toString()).sortKeys();
            if (query.condition(HugeKeys.SOURCE_VERTEX) != null
                    && query.condition(HugeKeys.DIRECTION) != null
                    && !keys.isEmpty() && query.matchUserpropKeys(keys)) {
                query.eq(HugeKeys.SORT_VALUES, query.userpropValuesString());
                query.resetUserpropConditions();
                logger.debug("query edges by sortKeys: {}", query);
                return query;
            }
        }

        // query only by sysprops, like: vertex label, edge label
        // NOTE: we assume sysprops would be indexed by backend store
        // but we don't support query edges only by direction
        if (query.allSysprop()) {
            if (query.resultType() == HugeType.EDGE
                    && query.condition(HugeKeys.DIRECTION) != null
                    && query.condition(HugeKeys.SOURCE_VERTEX) == null) {
                String msg = "Not support query edges only by direction";
                throw new BackendException(msg);
            }
            return query;
        }

        // optimize by index-query
        return this.indexTx.query(query);
    }
}
