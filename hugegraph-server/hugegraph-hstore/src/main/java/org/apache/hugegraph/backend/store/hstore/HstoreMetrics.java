package org.apache.hugegraph.backend.store.hstore;

import java.util.List;
import java.util.Map;

import org.apache.hugegraph.backend.store.BackendMetrics;
import org.apache.hugegraph.util.InsertionOrderUtil;

public class HstoreMetrics implements BackendMetrics {

    private final List<HstoreSessions> dbs;
    private final HstoreSessions.Session session;

    public HstoreMetrics(List<HstoreSessions> dbs,
                         HstoreSessions.Session session) {
        this.dbs = dbs;
        this.session = session;
    }

    @Override
    public Map<String, Object> metrics() {
        Map<String, Object> results = InsertionOrderUtil.newMap();
        results.put(NODES, 1);
        results.put(CLUSTER_ID, SERVER_LOCAL);

        return results;
    }
}
