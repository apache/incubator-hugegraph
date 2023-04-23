package com.baidu.hugegraph.store.client.util;

import com.baidu.hugegraph.store.HgKvStore;
import com.baidu.hugegraph.store.HgOwnerKey;

import java.util.Collections;
import java.util.List;

import static com.baidu.hugegraph.store.HgKvStore.*;

/**
 * @author lynn.bond@hotmail.com
 */
public final class HgStoreClientConst {
    public final static String DEFAULT_NODE_CLUSTER_ID = "default-node-cluster";

    public final static String EMPTY_STRING = "";
    public final static String EMPTY_TABLE = "";
    public final static byte[] EMPTY_BYTES = new byte[0];
    public final static byte[] MAX_BYTES=new byte[]{(byte)0b11111111};
    public final static List EMPTY_LIST = Collections.EMPTY_LIST;

    public final static byte[] ALL_PARTITION_OWNER = new byte[0]; // means to dispatch to all partitions.
    public final static HgOwnerKey EMPTY_OWNER_KEY =  HgOwnerKey.of(EMPTY_BYTES, EMPTY_BYTES);
    public final static HgOwnerKey ALL_PARTITION_OWNER_KEY = HgOwnerKey.of(ALL_PARTITION_OWNER, ALL_PARTITION_OWNER);

    //public final static int SCAN_GTE_BEGIN_LT_END = SCAN_GTE_BEGIN | SCAN_LT_END;
    public final static int SCAN_TYPE_RANGE= SCAN_GTE_BEGIN | SCAN_LTE_END;
    public final static int SCAN_TYPE_ANY = SCAN_ANY;
    public final static int NO_LIMIT=0;

    public final static int TX_SESSIONS_MAP_CAPACITY=32;
    public static final int NODE_MAX_RETRYING_TIMES=10;


}
