package com.baidu.hugegraph.store.client;


public class HgStoreService {
    private static HgStoreService instance=new HgStoreService();
    private HgStoreService(){}

    static HgStoreService of(){
        return instance;
    }


}
