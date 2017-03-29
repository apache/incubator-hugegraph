package com.baidu.hugegraph2.schema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.baidu.hugegraph2.backend.tx.SchemaTransaction;
import com.baidu.hugegraph2.type.schema.EdgeLabel;
import com.baidu.hugegraph2.type.schema.PropertyKey;
import com.baidu.hugegraph2.type.schema.VertexLabel;

/**
 * Created by jishilei on 17/3/17.
 */
public class HugeSchemaManager implements SchemaManager {

    private static final Logger logger = LoggerFactory.getLogger(HugeSchemaManager.class);

    private final SchemaTransaction transaction;

    public HugeSchemaManager(SchemaTransaction transaction) {
        this.transaction = transaction;
    }

    @Override
    public PropertyKey propertyKey(String name) {
        PropertyKey propertyKey = this.transaction.getPropertyKey(name);
        if (propertyKey == null) {
            propertyKey = new HugePropertyKey(name, transaction);
        }
        return propertyKey;
    }

    @Override
    public VertexLabel vertexLabel(String name) {
        VertexLabel vertexLabel = this.transaction.getVertexLabel(name);
        if (vertexLabel == null) {
            vertexLabel = new HugeVertexLabel(name, transaction);
        }
        return vertexLabel;
    }

    @Override
    public EdgeLabel edgeLabel(String name) {
        EdgeLabel edgeLabel = this.transaction.getEdgeLabel(name);
        if (edgeLabel == null) {
            edgeLabel = new HugeEdgeLabel(name, transaction);
        }
        return edgeLabel;
    }

    @Override
    public void desc() {
        this.transaction.getPropertyKeys();
//        this.transaction.getVertexLabels();
//        this.transaction.getEdgeLabels();

        //        schemaElements.forEach((key, val) -> logger.info(val.schema()));
    }

}
