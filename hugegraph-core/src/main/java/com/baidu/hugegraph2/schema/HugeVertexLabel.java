package com.baidu.hugegraph2.schema;

import com.baidu.hugegraph2.schema.base.VertexLabel;

/**
 * Created by liningrui on 2017/3/20.
 */
public class HugeVertexLabel implements VertexLabel {

    private String name;

    public HugeVertexLabel(String name) {
        this.name = name;
    }

    @Override
    public String schema() {
        return null;
    }

    @Override
    public String name() {
        return name;
    }
}
