package com.baidu.hugegraph2.structure;

import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.baidu.hugegraph2.backend.tx.GraphTransaction;

/**
 * Created by liningrui on 2017/3/28.
 */
public class HugeGraphManager implements GraphManager {

    private static final Logger logger = LoggerFactory.getLogger(HugeGraphManager.class);

    private final GraphTransaction transaction;

    public HugeGraphManager(GraphTransaction transaction) {
        this.transaction = transaction;
    }

    @Override
    public Vertex addVertex(Object... keyValues) {
        Vertex vertex = this.transaction.addVertex(keyValues);
        this.transaction.commit();
        return vertex;
    }
}
