package com.baidu.hugegraph.store.cli.scan;

import com.baidu.hugegraph.store.HgOwnerKey;
import com.baidu.hugegraph.store.HgSessionManager;
import com.baidu.hugegraph.store.HgStoreSession;
import com.baidu.hugegraph.store.client.HgStoreNodeManager;

import static com.baidu.hugegraph.store.cli.util.HgCliUtil.*;

/**
 * @author lynn.bond@hotmail.com on 2022/2/28
 */
public class HgStoreCommitter {

    protected final static HgStoreNodeManager nodeManager = HgStoreNodeManager.getInstance();

    private final String graph;


    public static HgStoreCommitter of(String graph) {
        return new HgStoreCommitter(graph);
    }

    private HgStoreCommitter(String graph) {
        this.graph = graph;
    }

    protected HgStoreSession getStoreSession() {
        return HgSessionManager.getInstance().openSession(this.graph);
    }

    protected HgStoreSession getStoreSession(String graphName) {
        return HgSessionManager.getInstance().openSession(graphName);
    }

    public void put(String tableName,int amount) {
        //*************** Put Benchmark **************//*
        String keyPrefix = "PUT-BENCHMARK";
        HgStoreSession session = getStoreSession();

        int length = String.valueOf(amount).length();

        session.beginTx();

        long start = System.currentTimeMillis();
        for (int i = 0; i < amount; i++) {
            HgOwnerKey key = toOwnerKey(keyPrefix + "-" + padLeftZeros(String.valueOf(i), length));
            byte[] value = toBytes(keyPrefix + "-V-" + i);

            session.put(tableName, key, value);

            if ((i + 1) % 100_000 == 0) {
                println("---------- " + (i + 1) + " --------");
                println("Preparing took: " + (System.currentTimeMillis() - start) + " ms.");
                session.commit();
                println("Committing took: " + (System.currentTimeMillis() - start) + " ms.");
                start = System.currentTimeMillis();
                session.beginTx();
            }
        }

        if (session.isTx()) {
            session.commit();
        }


    }
}
