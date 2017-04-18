package com.baidu.hugegraph.structure;

import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.backend.tx.GraphTransaction;

/**
 * Created by liningrui on 2017/3/28.
 */
public class HugeGraphManager implements GraphManager {

    private static final Logger logger = LoggerFactory.getLogger(HugeGraphManager.class);

    private final GraphTransaction transaction;

    public HugeGraphManager(GraphTransaction transaction) {
        this.transaction = transaction;
    }

    public boolean commit() {
        try {
            this.transaction.commit();
            return true;
        } catch (BackendException e) {
            logger.error("Failed to commit graph changes: {}", e.getMessage());
            try {
                this.transaction.rollback();
            } catch (BackendException e2) {
                // TODO: any better ways?
                logger.error("Failed to rollback graph changes: {}", e2.getMessage());
            }
        }
        return false;
    }

    @Override
    public Vertex addVertex(Object... keyValues) {
        Vertex vertex = this.transaction.addVertex(keyValues);
        if (commit()) {
            return vertex;
        }
        return null;
    }
}
