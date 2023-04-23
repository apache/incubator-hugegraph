package com.baidu.hugegraph.store.client;

import java.util.List;

/**
 * @author lynn.bond@hotmail.com created on 2021/10/12
 */
public final class HgStoreNodeCandidates {

    List<HgStoreNode> nodeList;

    HgStoreNodeCandidates(List<HgStoreNode> nodeList){
        this.nodeList=nodeList;
    }

    public int size(){
        return this.nodeList.size();
    }

    public HgStoreNode getNode(int index){
        return this.nodeList.get(index);
    }

}
