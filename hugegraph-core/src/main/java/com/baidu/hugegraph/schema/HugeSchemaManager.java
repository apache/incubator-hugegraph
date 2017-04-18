package com.baidu.hugegraph.schema;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.baidu.hugegraph.backend.tx.SchemaTransaction;
import com.baidu.hugegraph.type.schema.EdgeLabel;
import com.baidu.hugegraph.type.schema.PropertyKey;
import com.baidu.hugegraph.type.schema.VertexLabel;

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
        List<HugePropertyKey> propertyKeys = this.transaction.getPropertyKeys();
        propertyKeys.forEach(propertyKey -> logger.info(propertyKey.schema()));
//        this.transaction.getVertexLabels();
//        this.transaction.getEdgeLabels();

        //        schemaElements.forEach((key, val) -> logger.info(val.schema()));
    }

}
