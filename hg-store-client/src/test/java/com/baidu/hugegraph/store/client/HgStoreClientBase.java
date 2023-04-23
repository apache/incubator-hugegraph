package com.baidu.hugegraph.store.client;

import com.baidu.hugegraph.pd.client.PDClient;
import com.baidu.hugegraph.pd.client.PDConfig;
import com.baidu.hugegraph.store.HgStoreClient;
import com.baidu.hugegraph.store.HgStoreSession;
import org.junit.Before;

public class HgStoreClientBase {

    protected HgStoreClient storeClient;
    protected PDClient pdClient;

    protected static String Graph_Name = "testGraphName";
    protected static String Table_Name = "testTableName";
    @Before
    public void init(){
        storeClient = HgStoreClient.create(PDConfig.of("127.0.0.1:8686")
                .setEnableCache(true));
        pdClient = storeClient.getPdClient();

        HgStoreSession session = storeClient.openSession(Graph_Name);
        session.dropTable(Table_Name);
    }
}
