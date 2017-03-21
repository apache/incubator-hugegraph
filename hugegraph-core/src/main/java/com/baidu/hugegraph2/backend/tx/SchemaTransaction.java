package com.baidu.hugegraph2.backend.tx;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.baidu.hugegraph2.backend.id.Id;
import com.baidu.hugegraph2.backend.id.IdGenerator;
import com.baidu.hugegraph2.backend.store.BackendStore;
import com.baidu.hugegraph2.schema.HugeEdgeLabel;
import com.baidu.hugegraph2.schema.HugePropertyKey;
import com.baidu.hugegraph2.schema.HugeVertexLabel;
import com.baidu.hugegraph2.schema.base.VertexLabel;

public class SchemaTransaction extends AbstractTransaction {

    private static final Logger logger = LoggerFactory.getLogger(SchemaTransaction.class);

    // this could be an empty string, now setting a value just for test
    private static final String DEFAULT_COLUME = "default-colume";

    public SchemaTransaction(BackendStore store) {
        super(store);
        // TODO Auto-generated constructor stub
    }

    public void addPropertyKey(HugePropertyKey propertyKey) {
        logger.debug("SchemaTransaction add property key, "
                + "name: " + propertyKey.name() + ", "
                + "dataType: " + propertyKey.dataType() + ", "
                + "cardinality: " + propertyKey.cardinality());

        Id id = IdGenerator.generate(propertyKey);
        this.addEntry(id, DEFAULT_COLUME, propertyKey);
    }

    public void removePropertyKey(String name) {
        logger.debug("SchemaTransaction remove property key " + name);

        Id id = IdGenerator.generate(name);
        this.removeEntry(id);
    }

    public void addVertexLabel(HugeVertexLabel vertexLabel) {
        logger.debug("SchemaTransaction add vertex label, "
                + "name: " + vertexLabel.name());

        Id id = IdGenerator.generate(vertexLabel);
        this.addEntry(id, DEFAULT_COLUME, vertexLabel);
    }

    public void removeVertexLabel(String name) {
        logger.info("SchemaTransaction remove vertex label " + name);

        Id id = IdGenerator.generate(name);
        this.removeEntry(id);
    }

    public void addEdgeLabel(HugeEdgeLabel edgeLabel) {
        logger.debug("SchemaTransaction add edge label, "
                + "name: " + edgeLabel.name() + ", "
                + "multiplicity: " + edgeLabel.multiplicity() + ", "
                + "cardinality: " + edgeLabel.cardinality());

        Id id = IdGenerator.generate(edgeLabel);
        this.addEntry(id, DEFAULT_COLUME, edgeLabel);
    }

    public VertexLabel getOrCreateVertexLabel(String label) {
        // TODO: get from cache or db, now let it just returns a fake label
        return new HugeVertexLabel(label);
    }
}
