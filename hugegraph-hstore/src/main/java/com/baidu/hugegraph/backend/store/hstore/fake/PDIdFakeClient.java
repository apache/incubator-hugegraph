package com.baidu.hugegraph.backend.store.hstore.fake;

import com.baidu.hugegraph.backend.store.hstore.HstoreSessions;
import com.baidu.hugegraph.backend.store.hstore.HstoreTables;
import com.baidu.hugegraph.pd.grpc.Pdpb;

/**
 * @author zhangyingjie
 * @date 2022/1/17
 **/
public class PDIdFakeClient extends IdClient {

    public PDIdFakeClient(HstoreSessions.Session session, String table) {
        super(session, table);
    }

    @Override
    public Pdpb.GetIdResponse getIdByKey(String key, int delta) {
        return Pdpb.GetIdResponse.newBuilder().setId(
                getId(key, delta)).setDelta(delta).build();
    }

    public long getId(String key, int delta) {
        synchronized (PDIdFakeClient.class) {
            byte[] value = session.get(this.table, HstoreTables.Counters.COUNTER_OWNER,
                                       key.getBytes());
            session.increase(this.table, HstoreTables.Counters.COUNTER_OWNER,
                             key.getBytes(), b(delta));
            if (value.length != 0) {
                return l(value);
            } else {
                return 0L;
            }
        }
    }

    @Override
    public void increaseId(String key, long increment) {
        synchronized (PDIdFakeClient.class) {
            session.increase(this.table, HstoreTables.Counters.COUNTER_OWNER,
                             key.getBytes(), b(increment));
        }
    }

    @Override
    public Pdpb.ResetIdResponse resetIdByKey(String key) {
        return null;
    }
}