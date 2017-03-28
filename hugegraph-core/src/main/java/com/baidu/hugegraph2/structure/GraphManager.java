package com.baidu.hugegraph2.structure;

import org.apache.tinkerpop.gremlin.structure.Vertex;

/**
 * Created by liningrui on 2017/3/28.
 */
public interface GraphManager {
    public Vertex addVertex(Object... keyValues);

}
