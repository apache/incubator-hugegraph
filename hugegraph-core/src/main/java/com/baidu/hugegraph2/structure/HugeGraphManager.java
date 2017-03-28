package com.baidu.hugegraph2.structure;

import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.baidu.hugegraph2.backend.BackendException;
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
        Vertex vertex = null;
        try {
            vertex = this.transaction.addVertex(keyValues);
            this.transaction.commit();
        } catch (BackendException e) {
            logger.error("Failed to commit schema changes: {}", e.getMessage());
            try {
                this.transaction.rollback();
            } catch (BackendException e2) {
                // TODO: any better ways?
                logger.error("Failed to rollback schema changes: {}", e2.getMessage());
            }
        }
        return vertex;
    }
}
