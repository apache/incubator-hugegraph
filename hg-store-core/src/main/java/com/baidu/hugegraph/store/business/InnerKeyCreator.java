package com.baidu.hugegraph.store.business;


import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.baidu.hugegraph.store.meta.GraphIdManager;
import com.baidu.hugegraph.store.term.Bits;
import com.baidu.hugegraph.store.util.HgStoreException;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class InnerKeyCreator {

    final BusinessHandler businessHandler;

    public InnerKeyCreator(BusinessHandler businessHandler) {
        this.businessHandler = businessHandler;
    }

    private volatile Map<Integer, GraphIdManager> graphIdCache = new ConcurrentHashMap<>();


    public int getGraphId(Integer partId, String graphName) throws HgStoreException {
        try {
            GraphIdManager manager;
            if ((manager = graphIdCache.get(partId)) == null) {
                manager = new GraphIdManager(businessHandler, partId);
                graphIdCache.put(partId, manager);
            }
            return (int) manager.getGraphId(graphName);
        } catch (Exception e) {
            throw new HgStoreException(HgStoreException.EC_RKDB_PD_FAIL, e.getMessage());
        }
    }

    public void delGraphId(Integer partId, String graphName) {
        if (graphIdCache.containsKey(partId)) {
            graphIdCache.get(partId).releaseGraphId(graphName);
        } else {
            new GraphIdManager(businessHandler, partId).releaseGraphId(graphName);
        }
    }

    public void clearCache(Integer partId) {
        graphIdCache.remove(partId);
    }

    /**
     * 从key中解析出keyCode
     */
    public int parseKeyCode(byte[] innerKey) {
        return Bits.getShort(innerKey, innerKey.length - Short.BYTES);
    }

    public byte[] getKey(Integer partId, String graph, int code, byte[] key) {
        int graphId = getGraphId(partId, graph);
        byte[] buf = new byte[Short.BYTES + key.length + Short.BYTES];
        Bits.putShort(buf, 0, graphId);
        Bits.put(buf, Short.BYTES, key);
        Bits.putShort(buf, key.length + Short.BYTES, code);
        return buf;
    }

    public byte[] getStartKey(Integer partId, String graph) {
        int graphId = getGraphId(partId, graph);
        byte[] buf = new byte[Short.BYTES];
        Bits.putShort(buf, 0, graphId);
        return buf;
    }

    public byte[] getStartKey(Integer partId, String graph, byte[] key) {
        int graphId = getGraphId(partId, graph);
        byte[] buf = new byte[Short.BYTES + key.length];
        Bits.putShort(buf, 0, graphId);
        Bits.put(buf, Short.BYTES, key);
        return buf;
    }

    public byte[] getEndKey(Integer partId, String graph) {
        int graphId = getGraphId(partId, graph);
        byte[] buf = new byte[Short.BYTES];
        Bits.putShort(buf, 0, graphId + 1);
        return buf;
    }

    public byte[] getEndKey(Integer partId, String graph, byte[] key) {
        int graphId = getGraphId(partId, graph);
        byte[] buf = new byte[Short.BYTES + key.length];
        Bits.putShort(buf, 0, graphId);
        Bits.put(buf, Short.BYTES, key);
        return buf;
    }


    public byte[] getPrefixKey(Integer partId, String graph, byte[] prefix) {
        int graphId = getGraphId(partId, graph);
        byte[] buf = new byte[Short.BYTES + prefix.length];
        Bits.putShort(buf, 0, graphId);
        Bits.put(buf, Short.BYTES, prefix);
        return buf;
    }
}
