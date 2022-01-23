package com.baidu.hugegraph.backend.store.hstore.fake;

import com.baidu.hugegraph.backend.store.hstore.HstoreOptions;
import com.baidu.hugegraph.backend.store.hstore.HstoreSessions;

/**
 * @author zhangyingjie
 * @date 2022/1/17
 **/
public class IdClientFactory {

    public  static IdClient getClient(HstoreSessions.Session session,
                                      String table){
        Boolean isFake = session.getConf()
                                .get(HstoreOptions.PD_FAKE);
        if (isFake)
            return new PDIdFakeClient(session,table);
        return new PDIdClient(session,table);

    }

}
