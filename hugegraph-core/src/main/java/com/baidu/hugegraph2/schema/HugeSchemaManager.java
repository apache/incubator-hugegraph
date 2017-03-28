package com.baidu.hugegraph2.schema;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.baidu.hugegraph2.backend.BackendException;
import com.baidu.hugegraph2.backend.tx.SchemaTransaction;
import com.baidu.hugegraph2.type.schema.EdgeLabel;
import com.baidu.hugegraph2.type.schema.PropertyKey;
import com.baidu.hugegraph2.type.schema.VertexLabel;

/**
 * Created by jishilei on 17/3/17.
 */
public class HugeSchemaManager implements SchemaManager {

    private static final Logger logger = LoggerFactory.getLogger(HugeSchemaManager.class);

    private Map<String, SchemaElement> schemaElements;
    private final SchemaTransaction transaction;


    public HugeSchemaManager(SchemaTransaction transaction) {
        this.transaction = transaction;
        schemaElements = new HashMap<>();
    }

    @Override
    public PropertyKey propertyKey(String name) {
        PropertyKey propertyKey = (PropertyKey) schemaElements.get(name);
        if (propertyKey == null) {
            propertyKey = new HugePropertyKey(name, transaction);
            this.schemaElements.put(name, propertyKey);
        }
        return propertyKey;
    }

    @Override
    public VertexLabel vertexLabel(String name) {
        VertexLabel vertexLabel = (VertexLabel) schemaElements.get(name);
        if (vertexLabel == null) {
            vertexLabel = new HugeVertexLabel(name, transaction);
            this.schemaElements.put(name, vertexLabel);
        }
        return vertexLabel;
    }

    @Override
    public EdgeLabel edgeLabel(String name) {
        EdgeLabel edgeLabel = (EdgeLabel) schemaElements.get(name);
        if (edgeLabel == null) {
            edgeLabel = new HugeEdgeLabel(name, transaction);
            this.schemaElements.put(name, edgeLabel);
        }
        return edgeLabel;
    }

    @Override
    public void desc() {
//        for (String key: schemaElements.keySet()) {
//            logger.info(schemaElements.get(key).schema());
//        }
        schemaElements.forEach((key, val) -> logger.info(val.schema()));
    }

    @Override
    public VertexLabel getOrCreateVertexLabel(String label) {
        return this.transaction.getOrCreateVertexLabel(label);
    }

    @Override
    public VertexLabel getVertexLabel(String label) {
        return this.transaction.getVertexLabel(label);
    }

    @Override
    public boolean commit() {
        // commit schema changes
        try {
            this.transaction.commit();
            return true;
        } catch (BackendException e) {
            logger.error("Failed to commit schema changes: {}", e.getMessage());
            try {
                this.transaction.rollback();
            } catch (BackendException e2) {
                // TODO: any better ways?
                logger.error("Failed to rollback schema changes: {}", e2.getMessage());
            }
        }
        return false;
    }
}
