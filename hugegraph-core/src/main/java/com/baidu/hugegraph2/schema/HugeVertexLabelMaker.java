package com.baidu.hugegraph2.schema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.baidu.hugegraph2.IndexType;
import com.baidu.hugegraph2.backend.store.SchemaStore;
import com.baidu.hugegraph2.schema.base.SchemaType;
import com.baidu.hugegraph2.schema.base.maker.VertexLabelMaker;

/**
 * Created by jishilei on 17/3/17.
 */
public class HugeVertexLabelMaker implements VertexLabelMaker {

    private static final Logger logger = LoggerFactory.getLogger(HugeVertexLabelMaker.class);

    private String name;
    private SchemaStore schemaStore;
    private HugeVertexLabel vertexLabel;

    public HugeVertexLabelMaker(SchemaStore schemaStore, String name) {
        this.name = name;
        this.schemaStore = schemaStore;
        vertexLabel = new HugeVertexLabel(name);
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public SchemaType create() {
        schemaStore.addVertexLabel(vertexLabel);
        return vertexLabel;
    }

    @Override
    public SchemaType add() {
        schemaStore.removeVertexLabel(name);
        schemaStore.addVertexLabel(vertexLabel);
        return vertexLabel;
    }

    @Override
    public void remove() {
        schemaStore.removeVertexLabel(name);
    }

    @Override
    public VertexLabelMaker properties(String... propertyNames) {
        vertexLabel.properties(propertyNames);
        return this;
    }

    @Override
    public VertexLabelMaker index(String indexName) {
        vertexLabel.index(indexName);
        return this;
    }

    @Override
    public VertexLabelMaker secondary() {
        vertexLabel.indexType(IndexType.SECONDARY);
        return this;
    }

    @Override
    public VertexLabelMaker materialized() {
        vertexLabel.indexType(IndexType.MATERIALIZED);
        return this;
    }

    @Override
    public VertexLabelMaker by(String name) {
        // 先从vertexLabel的属性列表中查找名为name的属性，如果不存在，则error
        if (!vertexLabel.containPropertyKey(name)) {
            logger.error("The indexed column '" + name + "' must be exist in properties.");
            System.exit(-1);
        }
        vertexLabel.bindIndex(name);
        return this;
    }

    @Override
    public VertexLabelMaker partitionKey(String... keys) {
        vertexLabel.partitionKeys(keys);
        return this;
    }

    @Override
    public VertexLabelMaker clusteringKey(String... keys) {
        vertexLabel.clusteringKey(keys);
        return this;
    }
}
