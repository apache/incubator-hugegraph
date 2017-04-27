package com.baidu.hugegraph.backend.tx;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
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
import com.baidu.hugegraph.backend.id.SplicingIdGenerator;
import com.baidu.hugegraph.backend.query.Condition;
import com.baidu.hugegraph.backend.query.Condition.Relation;
import com.baidu.hugegraph.backend.query.ConditionQuery;
import com.baidu.hugegraph.backend.query.IdQuery;
import com.baidu.hugegraph.backend.query.Query;
import com.baidu.hugegraph.backend.store.BackendEntry;
import com.baidu.hugegraph.backend.store.BackendStore;
import com.baidu.hugegraph.schema.SchemaElement;
import com.baidu.hugegraph.schema.SchemaManager;
import com.baidu.hugegraph.structure.HugeEdge;
import com.baidu.hugegraph.structure.HugeElement;
import com.baidu.hugegraph.structure.HugeIndex;
import com.baidu.hugegraph.structure.HugeVertex;
import com.baidu.hugegraph.type.HugeTypes;
import com.baidu.hugegraph.type.define.HugeKeys;
import com.baidu.hugegraph.type.define.IndexType;
import com.baidu.hugegraph.type.schema.IndexLabel;
import com.baidu.hugegraph.type.schema.VertexLabel;
import com.baidu.hugegraph.util.CollectionUtil;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

public class GraphTransaction extends AbstractTransaction {

    private static final Logger logger = LoggerFactory.getLogger(GraphTransaction.class);

    private Set<HugeVertex> vertexes;

    public GraphTransaction(final HugeGraph graph, BackendStore store) {
        super(graph, store);
        this.vertexes = new LinkedHashSet<HugeVertex>();
    }

    @Override
    protected void prepareCommit() {
        // ensure all the target vertexes (of out edges) are in this.vertexes
        for (HugeVertex source : this.vertexes) {
            Iterator<Vertex> targets = source.vertices(Direction.OUT);
            while (targets.hasNext()) {
                HugeVertex target = (HugeVertex) targets.next();
                this.vertexes.add(target);
            }
        }

        // serialize and add into super.additions
        for (HugeVertex v : this.vertexes) {
            this.addEntry(this.serializer.writeVertex(v));
            this.updateVertexIndex(v, false);
        }

        this.vertexes.clear();
    }

    protected void updateVertexIndex(HugeVertex vertex, boolean removed) {
        // vertex index
        for (String indexName : vertex.vertexLabel().indexNames()) {
            updateIndex(indexName, vertex, removed);
        }

        // edge index
        for (HugeEdge edge : vertex.getEdges()) {
            for (String indexName : edge.edgeLabel().indexNames()) {
                updateIndex(indexName, edge, removed);
            }
        }
    }

    protected void updateIndex(String indexName, HugeElement element,
            boolean removed) {
        SchemaTransaction schema = this.graph.schemaTransaction();
        IndexLabel indexLabel = schema.getIndexLabel(indexName);
        Preconditions.checkNotNull(indexLabel,
                "Not existed index: " + indexName);

        List<Object> propertyValues = new LinkedList<>();
        for (String field : indexLabel.indexFields()) {
            propertyValues.add(element.property(field).value());
        }

        HugeIndex index = new HugeIndex(indexLabel);
        index.propertyValues(StringUtils.join(propertyValues,
                SplicingIdGenerator.NAME_SPLITOR));
        index.elementIds(element.id());

        this.addEntry(this.serializer.writeIndex(index));
    }

    @Override
    public Iterable<BackendEntry> query(Query query) {
        if (query instanceof ConditionQuery) {
            query = this.indexQuery((ConditionQuery) query);
        }
        return super.query(query);
    }

    protected Query indexQuery(ConditionQuery query) {
        if (query.allSysprop()) {
            return query;
        }

        // Condition => Entry
        ConditionQuery indexQuery = this.constructIndexQuery(query);
        Iterator<BackendEntry> entries = this.query(indexQuery).iterator();

        // Entry => Id
        Set<Id> ids = new LinkedHashSet<>();
        while (entries.hasNext()) {
            HugeIndex index = this.serializer.readIndex(entries.next());
            ids.addAll(index.elementIds());
        }
        return new IdQuery(query.resultType(), ids);
    }

    protected ConditionQuery constructIndexQuery(ConditionQuery query) {
        ConditionQuery indexQuery = null;
        SchemaElement schemaElement = null;

        SchemaTransaction schema = this.graph.schemaTransaction();

        boolean mustBeSearch = query.hasSearchCondition();
        Object label = query.condition(HugeKeys.LABEL);
        Preconditions.checkNotNull(label,
                "Must contain key 'label' in conditions");

        switch (query.resultType()) {
            case VERTEX:
                schemaElement = schema.getVertexLabel(label.toString());
                break;
            case EDGE:
                schemaElement = schema.getEdgeLabel(label.toString());
                break;
            default:
                throw new BackendException(
                        "Not supported index query: " + query.resultType());
        }

        Preconditions.checkNotNull(schemaElement, "Invalid label: " + label);

        Set<String> indexNames = schemaElement.indexNames();
        logger.debug("label '{}' index names: {}", label, indexNames);
        for (String name : indexNames) {
            IndexLabel indexLabel = schema.getIndexLabel(name);

            if (query.matchUserpropKeys(indexLabel.indexFields())) {
                logger.debug("matched index fields: {} of index '{}'",
                        indexLabel.indexFields(), name);

                boolean isSearch = indexLabel.indexType() == IndexType.SEARCH;
                if (mustBeSearch && !isSearch) {
                    continue;
                }

                if (indexLabel.indexType() == IndexType.SECONDARY) {
                    String valueStr = StringUtils.join(query.userpropValues(),
                            SplicingIdGenerator.NAME_SPLITOR);
                    indexQuery = new ConditionQuery(HugeTypes.SECONDARY_INDEX);
                    indexQuery.eq(HugeKeys.INDEX_LABEL_NAME, name);
                    indexQuery.eq(HugeKeys.PROPERTY_VALUES, valueStr);
                } else {
                    assert indexLabel.indexType() == IndexType.SEARCH;
                    if (query.userpropConditions().size() != 1) {
                        throw new BackendException(
                                "Only support search by one field");
                    }
                    Condition condition = query.userpropConditions().get(0);
                    assert condition instanceof Condition.Relation;
                    Condition.Relation r = (Relation) condition;
                    indexQuery = new ConditionQuery(HugeTypes.SEARCH_INDEX);
                    indexQuery.eq(HugeKeys.INDEX_LABEL_NAME, name);
                    indexQuery.query(new Condition.SyspropRelation(
                            HugeKeys.PROPERTY_VALUES,
                            r.relation(),
                            r.value()));
                }
                break;
            }
        }

        if (indexQuery == null) {
            throw new BackendException("No matched index for query: " + query);
        }
        return indexQuery;
    }

    public Vertex addVertex(HugeVertex vertex) {
        this.beforeWrite();
        vertex = this.vertexes.add(vertex) ? vertex : null;
        this.afterWrite();
        return vertex;
    }

    public Vertex addVertex(Object... keyValues) {
        ElementHelper.legalPropertyKeyValueArray(keyValues);

        Id id = HugeVertex.getIdValue(keyValues);
        Object label = HugeVertex.getLabelValue(keyValues);

        // Vertex id must be null now
        if (id != null) {
            String msg = "User defined id of Vertex is not supported";
            throw new IllegalArgumentException(msg);
        }

        if (label == null) {
            // Preconditions.checkArgument(label != null, "Vertex label must be not null");
            throw Element.Exceptions.labelCanNotBeNull();
        } else if (label instanceof String) {
            SchemaManager schema = this.graph.schema();
            label = schema.vertexLabel((String) label);
        }

        // create HugeVertex
        assert (label instanceof VertexLabel);

        // check keyValues whether contain primaryKey in definition of vertexLabel.
        Set<String> primaryKeys = ((VertexLabel) label).primaryKeys();
        Preconditions.checkArgument(CollectionUtil.containsAll(
                ElementHelper.getKeys(keyValues), primaryKeys),
                "The primary key(s) must be setted: " + primaryKeys);

        HugeVertex vertex = new HugeVertex(this.graph, id, (VertexLabel) label);
        // set properties
        ElementHelper.attachProperties(vertex, keyValues);

        // generate an id and assign it if not exists
        if (id == null) {
            vertex.assignId();
        }

        return this.addVertex(vertex);
    }

    public Iterator<Vertex> queryVertices(Object... vertexIds) {
        List<Vertex> list = new ArrayList<Vertex>(vertexIds.length);

        for (Object vertexId : vertexIds) {
            Id id = HugeElement.getIdValue(T.id, vertexId);
            BackendEntry entry = this.get(HugeTypes.VERTEX, id);
            Vertex vertex = this.serializer.readVertex(entry);
            assert vertex != null;
            list.add(vertex);
        }

        return list.iterator();
    }

    public Iterator<Vertex> queryVertices(Query q) {
        List<Vertex> list = new ArrayList<Vertex>();

        Iterator<BackendEntry> entries = this.query(q).iterator();
        while (entries.hasNext()) {
            Vertex vertex = this.serializer.readVertex(entries.next());
            assert vertex != null;
            list.add(vertex);
        }

        return list.iterator();
    }

    public Iterator<Edge> queryEdges(Object... edgeIds) {
        List<Edge> list = new ArrayList<Edge>(edgeIds.length);

        for (Object edgeId : edgeIds) {
            Id id = HugeElement.getIdValue(T.id, edgeId);
            BackendEntry entry = this.get(HugeTypes.EDGE, id);
            Vertex vertex = this.serializer.readVertex(entry);
            assert vertex != null;
            list.addAll(ImmutableList.copyOf(vertex.edges(Direction.BOTH)));
        }

        return list.iterator();
    }

    public Iterator<Edge> queryEdges(Query q) {
        Iterator<Vertex> vertices = this.queryVertices(q);

        List<Edge> list = new ArrayList<Edge>();
        while (vertices.hasNext()) {
            Vertex vertex = vertices.next();
            list.addAll(ImmutableList.copyOf(vertex.edges(Direction.BOTH)));
        }

        return list.iterator();
    }
}
