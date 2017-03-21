package com.baidu.hugegraph2.backend.store;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.baidu.hugegraph2.backend.BackendException;
import com.baidu.hugegraph2.example.ExampleGraphFactory;
import com.baidu.hugegraph2.schema.HugeEdgeLabel;
import com.baidu.hugegraph2.schema.HugePropertyKey;
import com.baidu.hugegraph2.schema.HugeVertexLabel;

/**
 * Created by jishilei on 17/3/20.
 */
public class SchemaStore implements DBStore {

    private static final Logger logger = LoggerFactory.getLogger(SchemaStore.class);

    @Override
    public void mutate(List<DBEntry> additions, List<Object> deletions, StoreTransaction tx) {

    }

    @Override
    public String getName() {
        return "schema";
    }

    @Override
    public void close() throws BackendException {

    }

    @Override
    public void clear() {

    }

    /**
     * 关键在于这几个方法，这里应该是与底层数据库打交道的，这里暂时以打印日志代替
     * @param propertyKey
     */
    public void addPropertyKey(HugePropertyKey propertyKey) {
        logger.info("SchemaStore add property key, "
                + "name: " + propertyKey.name() + ", "
                + "dataType: " + propertyKey.dataType() + ", "
                + "cardinality: " + propertyKey.cardinality());
    }

    public void removePropertyKey(String name) {
        logger.info("SchemaStore remove property key " + name);
    }

    public void addVertexLabel(HugeVertexLabel vertexLabel) {
        logger.info("SchemaStore add vertex label, "
                + "name: " + vertexLabel.name());
    }

    public void addEdgeLabel(HugeEdgeLabel edgeLabel) {
        logger.info("SchemaStore add edge label, "
                + "name: " + edgeLabel.name() + ", "
                + "multiplicity: " + edgeLabel.multiplicity() + ", "
                + "cardinality: " + edgeLabel.cardinality());
    }
}
