package com.baidu.hugegraph2.schema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.baidu.hugegraph2.backend.store.SchemaStore;
import com.baidu.hugegraph2.schema.base.maker.EdgeLabelMaker;
import com.baidu.hugegraph2.schema.base.maker.PropertyKeyMaker;
import com.baidu.hugegraph2.schema.base.maker.SchemaManager;
import com.baidu.hugegraph2.schema.base.maker.VertexLabelMaker;

/**
 * Created by jishilei on 17/3/17.
 */
public class HugeSchemaManager implements SchemaManager {

    private static final Logger logger = LoggerFactory.getLogger(HugeSchemaManager.class);
    private PropertyKeyMaker propertyKeyMaker;

    private SchemaStore schemaStore;

    public HugeSchemaManager() {

        schemaStore = new SchemaStore();
    }

    @Override
    public PropertyKeyMaker propertyKey(String name) {
        propertyKeyMaker = new HugePropertyKeyMaker(schemaStore, name);
        return propertyKeyMaker;
    }

    @Override
    public VertexLabelMaker vertexLabel(String name) {
        return null;
    }

    @Override
    public EdgeLabelMaker edgeLabel(String name) {
        return null;
    }

    @Override
    public void desc() {

        //        for(Map.Entry<String, PropertyKey> entry : schemaStore.getPropertyKeys().entrySet()){
        //            logger.info(entry.getValue().schema());
        //        }
    }
}
