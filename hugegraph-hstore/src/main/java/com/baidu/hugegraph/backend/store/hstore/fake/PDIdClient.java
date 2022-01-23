package com.baidu.hugegraph.backend.store.hstore.fake;

import com.baidu.hugegraph.backend.store.hstore.HstoreSessions;
import com.baidu.hugegraph.backend.store.hstore.HstoreSessionsImpl;
import com.baidu.hugegraph.pd.client.PDClient;
import com.baidu.hugegraph.pd.grpc.Pdpb;

/**
 * @author zhangyingjie
 * @date 2022/1/17
 **/
public class PDIdClient extends IdClient{

    PDClient pdClient;

    public PDIdClient(HstoreSessions.Session session, String table) {
        super(session,table);
        pdClient= HstoreSessionsImpl.getDefaultPdClient();
    }

    @Override
    public Pdpb.GetIdResponse getIdByKey(String key, int delta) throws Exception {
        return pdClient.getIdByKey(key,delta);
    }

    @Override
    public Pdpb.ResetIdResponse resetIdByKey(String key) throws Exception {
        return pdClient.resetIdByKey(key);
    }

    @Override
    public void increaseId(String key, long increment) throws Exception {
        pdClient.getIdByKey(key,(int)increment);
    }
}
