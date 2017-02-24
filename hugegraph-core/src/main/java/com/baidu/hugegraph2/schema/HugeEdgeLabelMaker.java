package com.baidu.hugegraph2.schema;

import com.baidu.hugegraph2.schema.base.maker.EdgeLabelMaker;

/**
 * Created by jishilei on 17/3/17.
 */
public class HugeEdgeLabelMaker implements EdgeLabelMaker {
    @Override
    public EdgeLabelMaker connection(String fromVertexLabel, String toVertexLabel) {
        return null;
    }

    @Override
    public EdgeLabelMaker multi() {
        return null;
    }

    @Override
    public EdgeLabelMaker simple() {
        return null;
    }
}
