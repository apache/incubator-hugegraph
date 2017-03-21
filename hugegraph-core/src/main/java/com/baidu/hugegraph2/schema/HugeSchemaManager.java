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

        propertyKeyMakers = new HashMap<>();
        vertexLabelMakers = new HashMap<>();
        edgeLabelMakers = new HashMap<>();
    }

    @Override
    public PropertyKeyMaker propertyKey(String name) {
        PropertyKeyMaker propertyKeyMaker = propertyKeyMakers.get(name);
        if (propertyKeyMaker == null) {
            propertyKeyMaker = new HugePropertyKeyMaker(transaction, name);
            propertyKeyMakers.put(name, propertyKeyMaker);
        }
        return propertyKeyMaker;
    }

    @Override
    public VertexLabelMaker vertexLabel(String name) {
        VertexLabelMaker vertexLabelMaker = vertexLabelMakers.get(name);
        if (vertexLabelMaker == null) {
            vertexLabelMaker = new HugeVertexLabelMaker(transaction, name);
            vertexLabelMakers.put(name, vertexLabelMaker);
        }
        return vertexLabelMaker;
    }

    @Override
    public EdgeLabelMaker edgeLabel(String name) {
        EdgeLabelMaker edgeLabelMaker = edgeLabelMakers.get(name);
        if (edgeLabelMaker == null) {
            edgeLabelMaker = new HugeEdgeLabelMaker(transaction, name);
            edgeLabelMakers.put(name, edgeLabelMaker);
        }
        return edgeLabelMaker;
    }

    @Override
    public void desc() {

        //        for(Map.Entry<String, PropertyKey> entry : schemaStore.getPropertyKeys().entrySet()){
        //            logger.info(entry.getValue().schema());
        //        }
    }

    @Override
    public VertexLabel getOrCreateVertexLabel(String label) {
        return this.transaction.getOrCreateVertexLabel(label);
    }

    @Override
    public void commit() throws BackendException {
        this.transaction.commit();
    }

    @Override
    public void rollback() throws BackendException {
        this.transaction.rollback();
    }
}
