package com.baidu.hugegraph2.schema;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.baidu.hugegraph2.backend.BackendException;
import com.baidu.hugegraph2.backend.tx.SchemaTransaction;
import com.baidu.hugegraph2.schema.base.VertexLabel;
import com.baidu.hugegraph2.schema.base.maker.EdgeLabelMaker;
import com.baidu.hugegraph2.schema.base.maker.PropertyKeyMaker;
import com.baidu.hugegraph2.schema.base.maker.SchemaManager;
import com.baidu.hugegraph2.schema.base.maker.VertexLabelMaker;

/**
 * Created by jishilei on 17/3/17.
 */
public class HugeSchemaManager implements SchemaManager {

    private static final Logger logger = LoggerFactory.getLogger(HugeSchemaManager.class);

    private Map<String, PropertyKeyMaker> propertyKeyMakers;
    private Map<String, VertexLabelMaker> vertexLabelMakers;
    private Map<String, EdgeLabelMaker> edgeLabelMakers;

    private final SchemaTransaction transaction;

    public HugeSchemaManager(SchemaTransaction transaction) {
        this.transaction = transaction;

        this.propertyKeyMakers = new HashMap<>();
        this.vertexLabelMakers = new HashMap<>();
        this.edgeLabelMakers = new HashMap<>();
    }

    @Override
    public PropertyKeyMaker propertyKey(String name) {
        PropertyKeyMaker propertyKeyMaker = this.propertyKeyMakers.get(name);
        if (propertyKeyMaker == null) {
            propertyKeyMaker = new HugePropertyKeyMaker(this.transaction, name);
            this.propertyKeyMakers.put(name, propertyKeyMaker);
        }
        return propertyKeyMaker;
    }

    @Override
    public VertexLabelMaker vertexLabel(String name) {
        VertexLabelMaker vertexLabelMaker = this.vertexLabelMakers.get(name);
        if (vertexLabelMaker == null) {
            vertexLabelMaker = new HugeVertexLabelMaker(this.transaction, name);
            this.vertexLabelMakers.put(name, vertexLabelMaker);
        }
        return vertexLabelMaker;
    }

    @Override
    public EdgeLabelMaker edgeLabel(String name) {
        EdgeLabelMaker edgeLabelMaker = this.edgeLabelMakers.get(name);
        if (edgeLabelMaker == null) {
            edgeLabelMaker = new HugeEdgeLabelMaker(this.transaction, name);
            this.edgeLabelMakers.put(name, edgeLabelMaker);
        }
        return edgeLabelMaker;
    }

    @Override
    public void desc() {

        this.transaction.getPropertyKeys().forEach((p)->{
            logger.info(p.schema());
        });
    }

    @Override
    public VertexLabel getOrCreateVertexLabel(String label) {
        return this.transaction.getOrCreateVertexLabel(label);
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
