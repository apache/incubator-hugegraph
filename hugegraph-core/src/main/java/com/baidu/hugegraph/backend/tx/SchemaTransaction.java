package com.baidu.hugegraph.backend.tx;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.baidu.hugegraph.HugeGraph;
import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.backend.query.HugeQuery;
import com.baidu.hugegraph.backend.serializer.TextBackendEntry;
import com.baidu.hugegraph.backend.store.BackendEntry;
import com.baidu.hugegraph.backend.store.BackendStore;
import com.baidu.hugegraph.schema.HugeEdgeLabel;
import com.baidu.hugegraph.schema.HugePropertyKey;
import com.baidu.hugegraph.schema.HugeVertexLabel;
import com.baidu.hugegraph.type.define.Cardinality;
import com.baidu.hugegraph.type.define.DataType;
import com.baidu.hugegraph.type.schema.EdgeLabel;
import com.baidu.hugegraph.type.schema.PropertyKey;
import com.baidu.hugegraph.type.schema.VertexLabel;

public class SchemaTransaction extends AbstractTransaction {

    private static final Logger logger = LoggerFactory.getLogger(SchemaTransaction.class);

    // this could be an empty string, now setting a value just for test
    private static final String DEFAULT_COLUME = "default-colume";

    private static final String ID_COLUME = "_id";
    private static final String SCHEMATYPE_COLUME = "_schema";

    public SchemaTransaction(HugeGraph graph, BackendStore store) {
        super(graph, store);
        // TODO Auto-generated constructor stub
    }

    public List<HugePropertyKey> getPropertyKeys() {
        // TODO:to be checked

        List<HugePropertyKey> propertyKeys = new ArrayList<HugePropertyKey>();

        HugeQuery query = new HugeQuery();
        query.has(SCHEMATYPE_COLUME, "PROPERTY");

        Iterable<BackendEntry> entries = query(query);
        entries.forEach(item -> {
            TextBackendEntry entry = (TextBackendEntry) item;
            propertyKeys.add((HugePropertyKey) this.serializer.readPropertyKey(entry));
        });
        return propertyKeys;
    }

    public void getVertexLabels() {
        // todo:to be implemented
    }

    public void getEdgeLabels() {
        // todo:to be implemented
    }

    public void addPropertyKey(HugePropertyKey propertyKey) {
        logger.debug("SchemaTransaction add property key, "
                + "name: " + propertyKey.name() + ", "
                + "dataType: " + propertyKey.dataType() + ", "
                + "cardinality: " + propertyKey.cardinality());

        this.addEntry(serializer.writePropertyKey(propertyKey));
    }

    public PropertyKey getPropertyKey(String name) {
        Id id = this.idGenerator.generate(new HugePropertyKey(name, null));
        BackendEntry entry = this.store.get(id);
        return this.serializer.readPropertyKey(entry);
    }

    public void removePropertyKey(String name) {
        logger.debug("SchemaTransaction remove property key " + name);

        Id id = this.idGenerator.generate(new HugePropertyKey(name, null));
        this.removeEntry(id);
    }

    public void addVertexLabel(HugeVertexLabel vertexLabel) {
        logger.debug("SchemaTransaction add vertex label, "
                + "name: " + vertexLabel.name());

        this.addEntry(serializer.writeVertexLabel(vertexLabel));
    }

    public VertexLabel getVertexLabel(String name) {
        Id id = this.idGenerator.generate(new HugeVertexLabel(name, null));
        BackendEntry entry = this.store.get(id);
        return this.serializer.readVertexLabel(entry);
    }

    public void removeVertexLabel(String name) {
        logger.info("SchemaTransaction remove vertex label " + name);

        Id id = this.idGenerator.generate(new HugeVertexLabel(name, null));
        this.removeEntry(id);
    }

    public void addEdgeLabel(HugeEdgeLabel edgeLabel) {
        logger.debug("SchemaTransaction add edge label, "
                + "name: " + edgeLabel.name() + ", "
                + "multiplicity: " + edgeLabel.multiplicity() + ", "
                + "frequency: " + edgeLabel.frequency());

        this.addEntry(serializer.writeEdgeLabel(edgeLabel));
    }

    public EdgeLabel getEdgeLabel(String name) {
        Id id = this.idGenerator.generate(new HugeEdgeLabel(name, null));
        BackendEntry entry = this.store.get(id);
        return this.serializer.readEdgeLabel(entry);
    }

    public void removeEdgeLabel(String name) {
        logger.info("SchemaTransaction remove edge label " + name);

        Id id = this.idGenerator.generate(new HugeEdgeLabel(name, null));
        this.removeEntry(id);
    }
}
