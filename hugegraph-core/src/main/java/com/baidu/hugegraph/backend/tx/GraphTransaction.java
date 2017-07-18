package com.baidu.hugegraph.backend.tx;

import java.util.ArrayList;
import java.util.Arrays;
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
import com.baidu.hugegraph.util.E;
import com.google.common.collect.ImmutableList;

public class GraphTransaction extends AbstractTransaction {

    private IndexTransaction indexTx;

    private Set<HugeVertex> addedVertexes;
    private Set<HugeVertex> removedVertexes;

    private Set<HugeEdge> addedEdges;
    private Set<HugeEdge> removedEdges;

    public GraphTransaction(HugeGraph graph, BackendStore store,
                            BackendStore indexStore) {
        super(graph, store);

        this.indexTx = new IndexTransaction(graph, indexStore);
        assert !this.indexTx.autoCommit();
    }

    @Override
    public boolean hasUpdates() {
        boolean empty = (this.addedVertexes.isEmpty() &&
                         this.removedVertexes.isEmpty() &&
                         this.addedEdges.isEmpty() &&
                         this.removedEdges.isEmpty());
        return !empty || super.hasUpdates();
    }

    @Override
    protected void reset() {
        super.reset();

        this.addedVertexes = new LinkedHashSet<>();
        this.removedVertexes = new LinkedHashSet<>();

        this.addedEdges = new LinkedHashSet<>();
        this.removedEdges = new LinkedHashSet<>();
    }

    @Override
    protected void prepareCommit() {
        // Serialize and add updates into super.deletions
        this.prepareDeletions(this.removedVertexes, this.removedEdges);
        // Serialize and add updates into super.additions
        this.prepareAdditions(this.addedVertexes, this.addedEdges);
    }

    protected void prepareAdditions(Set<HugeVertex> updatedVertexes,
                                    Set<HugeEdge> updatedEdges) {

        Map<Id, HugeVertex> vertexes = new HashMap<>();

        // Copy updated vertexes(only with props, without edges)
        for (HugeVertex v : updatedVertexes) {
            vertexes.put(v.id(), v.prepareAdded());
        }

        // Copy updated edges and merge into owner vertex
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

        // Ensure all the target vertexes (of out edges) are in vertexes
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

        // Do update
        for (HugeVertex v : vertexes.values()) {
            // Add vertex entry
            this.addEntry(this.serializer.writeVertex(v));

            if (v.hasProperties()) {
                // Update index of vertex(include props and edges)
                this.indexTx.updateVertexIndex(v, false);
            } else {
                // Update index of vertex edges
                this.indexTx.updateEdgesIndex(v, false);
            }
        }

        // Clear updates
        updatedVertexes.clear();
        updatedEdges.clear();
    }

    protected void prepareDeletions(Set<HugeVertex> updatedVertexes,
                                    Set<HugeEdge> updatedEdges) {

        Set<HugeVertex> vertexes = new LinkedHashSet<>();
        vertexes.addAll(updatedVertexes);

        Set<HugeEdge> edges = new LinkedHashSet<>();
        edges.addAll(updatedEdges);

        // Clear updates
        updatedVertexes.clear();
        updatedEdges.clear();

        // In order to remove edges of vertexes, query all edges first
        for (HugeVertex v : vertexes) {
            @SuppressWarnings({ "unchecked", "rawtypes" })
            Collection<HugeEdge> vedges = (Collection) ImmutableList.copyOf(
                    this.queryEdgesByVertex(v.id()));
            edges.addAll(vedges);
        }

        // Remove vertexes
        for (HugeVertex v : vertexes) {
            /*
             * If the backend stores vertex together with edges, it's edges
             * would be removed after removing vertex. Otherwise, if the
             * backend stores vertex which is separated from edges, it's
             * edges should be removed manually when removing vertex.
             */
            this.removeEntry(this.serializer.writeVertex(v.prepareRemoved()));
            // Calling vertex.prepareAdded() returns a vertex without edges
            this.indexTx.updateVertexIndex(v.prepareAdded(), true);
        }

        // Remove edges independently
        for (HugeEdge edge : edges) {
            // Remove OUT
            HugeVertex vertex = edge.sourceVertex().prepareRemovedChildren();
            // Calling edge.prepareRemoved() returns an edge only with edge-id
            vertex.addEdge(edge.prepareRemoved());
            this.removeEntry(this.serializer.writeVertex(vertex));

            // Remove IN
            vertex = edge.targetVertex().prepareRemovedChildren();
            vertex.addEdge(edge.switchOwner().prepareRemoved());
            this.removeEntry(this.serializer.writeVertex(vertex));

            // Update edge index
            this.indexTx.updateEdgeIndex(edge, true);
        }
    }

    @Override
    public void commit() throws BackendException {
        try {
            super.commit();
        } catch (Throwable e) {
            this.indexTx.reset();
            throw e;
        }
        this.indexTx.commit();
    }

    @Override
    public void rollback() throws BackendException {
        try {
            super.rollback();
        } finally {
            this.indexTx.rollback();
        }
    }

    @Override
    public void close() {
        try {
            this.indexTx.close();
        } finally {
            super.close();
        }
    }

    @Override
    public Iterable<BackendEntry> query(Query query) {
        if (query instanceof ConditionQuery) {
            query = this.optimizeQuery((ConditionQuery) query);
            /*
             * NOTE: There are two possibilities for this query:
             * 1.sysprop-query, which would not be empty.
             * 2.index-query result(ids after optimize), which may be empty.
             */
            if (query.empty()) {
                // Return empty if there is no result after index-query
                return ImmutableList.of();
            }
        }
        return super.query(query);
    }

    public Vertex addVertex(HugeVertex vertex) {
        this.checkOwnerThread();

        this.beforeWrite();
        this.addedVertexes.add(vertex);
        this.afterWrite();
        return vertex;
    }

    public Vertex addVertex(Object... keyValues) {
        ElementHelper.legalPropertyKeyValueArray(keyValues);

        Id id = HugeVertex.getIdValue(keyValues);
        Object label = HugeVertex.getLabelValue(keyValues);

        HugeVertexFeatures features = graph().features().vertex();

        // Vertex id must be null now
        if (!features.supportsUserSuppliedIds() && id != null) {
            throw new IllegalArgumentException(
                      "Not support user defined id of Vertex");
        }

        // Check Vertex label
        if (label == null && features.supportsDefaultLabel()) {
            label = features.defaultLabel();
        }

        if (label == null) {
            throw Element.Exceptions.labelCanNotBeNull();
        } else if (label instanceof String) {
            SchemaManager schema = graph().schema();
            label = schema.vertexLabel((String) label);
        }

        assert (label instanceof VertexLabel);

        // Check whether primaryKey exists
        List<String> primaryKeys = ((VertexLabel) label).primaryKeys();
        E.checkArgument(CollectionUtil.containsAll(
                ElementHelper.getKeys(keyValues), primaryKeys),
                "The primary key(s) must be set: '%s'", primaryKeys);

        // Create HugeVertex
        HugeVertex vertex = new HugeVertex(this, id, (VertexLabel) label);

        // Set properties
        ElementHelper.attachProperties(vertex, keyValues);

        // Generate and assign an id if it doesn't exist
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

    public Iterable<Vertex> queryVertices(Object... vertexIds) {
        List<Vertex> list = new ArrayList<Vertex>(vertexIds.length);

        for (Object vertexId : vertexIds) {
            Id id = HugeElement.getIdValue(T.id, vertexId);
            BackendEntry entry = this.get(HugeType.VERTEX, id);
            Vertex vertex = this.serializer.readVertex(entry);
            assert vertex != null;
            list.add(vertex);
        }

        return list;
    }

    public Iterable<Vertex> queryVertices() {
        Query q = new Query(HugeType.VERTEX);
        return this.queryVertices(q);
    }

    public Iterable<Vertex> queryVertices(Query query) {
        assert Arrays.asList(HugeType.VERTEX, HugeType.EDGE)
                     .contains(query.resultType());
        List<Vertex> list = new ArrayList<Vertex>();

        Iterator<BackendEntry> entries = this.query(query).iterator();
        while (entries.hasNext()) {
            Vertex vertex = this.serializer.readVertex(entries.next());
            assert vertex != null;
            list.add(vertex);
        }

        return list;
    }

    public Iterable<Vertex> queryAdjacentVertices(Iterator<Edge> edges) {
        if (!edges.hasNext()) {
            return ImmutableList.<Vertex>of();
        }

        IdQuery query = new IdQuery(HugeType.VERTEX);
        while (edges.hasNext()) {
            HugeEdge edge = (HugeEdge) edges.next();
            query.query(edge.otherVertex().id());
        }

        return this.queryVertices(query);
    }

    public Edge addEdge(HugeEdge edge) {
        this.checkOwnerThread();

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

    public Iterable<Edge> queryEdges(Object... edgeIds) {
        List<Edge> list = new ArrayList<Edge>(edgeIds.length);

        for (Object edgeId : edgeIds) {
            Id id = HugeElement.getIdValue(T.id, edgeId);
            BackendEntry entry = this.get(HugeType.EDGE, id);
            HugeVertex vertex = this.serializer.readVertex(entry);
            assert vertex != null;
            list.addAll(ImmutableList.copyOf(vertex.getEdges()));
        }

        return list;
    }

    public Iterable<Edge> queryEdges() {
        Query q = new Query(HugeType.EDGE);
        return this.queryEdges(q);
    }

    public Iterable<Edge> queryEdges(Query query) {
        assert query.resultType() == HugeType.EDGE;
        Iterator<Vertex> vertices = this.queryVertices(query).iterator();

        Map<Id, Edge> results = new HashMap<>();
        while (vertices.hasNext()) {
            HugeVertex vertex = (HugeVertex) vertices.next();
            for (HugeEdge edge : vertex.getEdges()) {
                if (!results.containsKey(edge.id())) {
                    /*
                     * NOTE: Maybe some edges are IN and others are OUT if
                     * querying edges both directions, perhaps it would look
                     * better if we convert all edges in results to OUT, but
                     * that would break the logic when querying IN edges.
                     */
                    results.put(edge.id(), edge);
                } else {
                    logger.debug("Results contains edge: {}", edge);
                }
            }
        }

        return results.values();
    }

    public Iterable<Edge> queryEdgesByVertex(Id id) {
        return queryEdges(constructEdgesQuery(id, null));
    }

    public static ConditionQuery constructEdgesQuery(Id sourceVertex,
                                                     Direction direction,
                                                     String... edgeLabels) {

        E.checkState(sourceVertex != null,
                     "The edge query must contain source vertex");
        E.checkState((direction != null ||
                     (direction == null && edgeLabels.length == 0)),
                     "The edge query must contain direction " +
                     "if it contains edge label");

        ConditionQuery query = new ConditionQuery(HugeType.EDGE);

        // Edge source vertex
        query.eq(HugeKeys.SOURCE_VERTEX, sourceVertex);

        // Edge direction
        if (direction != null) {
            assert direction == Direction.OUT || direction == Direction.IN;
            query.eq(HugeKeys.DIRECTION, direction);
        }

        // Edge labels
        if (edgeLabels.length == 1) {
            query.eq(HugeKeys.LABEL, edgeLabels[0]);
        } else if (edgeLabels.length > 1) {
            // TODO: support query by multi edge labels
            // Like: query.query(Condition.in(HugeKeys.LABEL, edgeLabels));
            throw new BackendException(
                      "Not support querying by multi edge-labels");
        } else {
            assert edgeLabels.length == 0;
        }

        return query;
    }

    public static void verifyEdgesConditionQuery(ConditionQuery query) {
        final HugeKeys[] keys = new HugeKeys[] {
                HugeKeys.SOURCE_VERTEX,
                HugeKeys.DIRECTION,
                HugeKeys.LABEL,
                HugeKeys.SORT_VALUES,
                HugeKeys.TARGET_VERTEX
        };
        assert query.resultType() == HugeType.EDGE;

        int total = query.conditions().size();
        if (total == 1) {
            // Supported: 1.query just by edge label, 2.query with scan
            if (query.containsCondition(HugeKeys.LABEL) ||
                query.containsScanCondition()) {
                return;
            }
        }

        int matched = 0;
        for (HugeKeys key : keys) {
            Object value = query.condition(key);
            if (value == null) {
                break;
            }
            matched++;
        }
        if (matched != total) {
            throw new BackendException(
                    "Not supported querying edges by %s, expected %s",
                    query.conditions(), keys[matched]);
        }
    }

    protected Query optimizeQuery(ConditionQuery query) {
        HugeFeatures features = this.graph().features();

        // Optimize vertex query
        Object label = query.condition(HugeKeys.LABEL);
        if (label != null && query.resultType() == HugeType.VERTEX &&
            !features.vertex().supportsUserSuppliedIds()) {

            // Query vertex by label + primary-values
            List<String> keys = graph().schema().vertexLabel(
                    label.toString()).primaryKeys();
            if (!keys.isEmpty() && query.matchUserpropKeys(keys)) {
                String primaryValues = query.userpropValuesString(keys);
                query.eq(HugeKeys.PRIMARY_VALUES, primaryValues);
                query.resetUserpropConditions();
                logger.debug("Query vertices by primaryKeys: {}", query);

                // Convert vertex-label + primary-key to vertex-id
                if (IdGeneratorFactory.supportSplicing()) {
                    Id id = SplicingIdGenerator.splicing(
                            label.toString(),
                            primaryValues);
                    query.query(id);
                    query.resetConditions();
                } else {
                    // Assert this.store().supportsSysIndex();
                    logger.warn("Please ensure the backend supports " +
                                "query by primary-key: {}", query);
                }
                return query;
            }
        }

        // Optimize edge query
        if (label != null && query.resultType() == HugeType.EDGE) {
            // Query edge by sourceVertex + direction + label + sort-values
            List<String> keys = graph().schema().edgeLabel(
                    label.toString()).sortKeys();
            if (query.condition(HugeKeys.SOURCE_VERTEX) != null &&
                query.condition(HugeKeys.DIRECTION) != null &&
                !keys.isEmpty() &&
                query.matchUserpropKeys(keys)) {

                query.eq(HugeKeys.SORT_VALUES,
                         query.userpropValuesString(keys));
                query.resetUserpropConditions();
                logger.debug("Query edges by sortKeys: {}", query);
                return query;
            }
        }

        /*
         * Query only by sysprops, like: vertex label, edge label.
         * NOTE: we assume sysprops would be indexed by backend store
         * but we don't support query edges only by direction/targetVertex.
         */
        if (query.allSysprop()) {
            if (query.resultType() == HugeType.EDGE) {
                verifyEdgesConditionQuery(query);
            }
            return query;
        }

        // Optimize by index-query
        return this.indexTx.query(query);
    }
}
