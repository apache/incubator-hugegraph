package com.baidu.hugegraph2.schema;

import com.baidu.hugegraph2.backend.store.SchemaStore;
import com.baidu.hugegraph2.schema.base.SchemaType;
import com.baidu.hugegraph2.schema.base.maker.VertexLabelMaker;

/**
 * Created by jishilei on 17/3/17.
 */
public class HugeVertexLabelMaker implements VertexLabelMaker {

    private SchemaStore schemaStore;
    private HugeVertexLabel vertexLabel;
    private String name;

    public HugeVertexLabelMaker(SchemaStore schemaStore, String name) {
        this.name = name;
        this.schemaStore = schemaStore;
        vertexLabel = new HugeVertexLabel(name);
    }

    @Override
    public String getName() {
        return null;
    }

    @Override
    public SchemaType create() {
        schemaStore.addVertexLabel(vertexLabel);
        return vertexLabel;
    }

    @Override
    public SchemaType add() {
        return null;
    }

    @Override
    public void remove() {

    }

    @Override
    public VertexLabelMaker index(String byName) {

        return null;
    }

    @Override
    public VertexLabelMaker secondary() {
        return null;
    }

    @Override
    public VertexLabelMaker materialized() {
        return null;
    }

    @Override
    public VertexLabelMaker by(String name) {
        return null;
    }
}
