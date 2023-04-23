package com.baidu.hugegraph.store.client;

import com.baidu.hugegraph.store.client.type.HgNodeStatus;

/**
 * @author lynn.bond@hotmail.com created on 2021/10/12
 * @version 1.0.0
 */
public interface HgStoreNodeNotifier {

    /**
     * It will be invoked by NodeManager, when some exception or issue was happened.
     * @param graphName
     * @param storeNotice
     * @return return 0 please, for no matter what.
     */
    int notice(String graphName, HgStoreNotice storeNotice);

}
