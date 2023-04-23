package com.baidu.hugegraph.store.node.util;

import java.util.Collections;
import java.util.List;

/**
 * @author lynn.bond@hotmail.com created on 2021/10/22
 */
public final class HgStoreConst {

    public final static int SCAN_WAIT_CLIENT_TAKING_TIME_OUT_SECONDS=300;

    public final static byte[] EMPTY_BYTES=new byte[0];

    public static final List EMPTY_LIST = Collections.EMPTY_LIST;

    public final static int SCAN_ALL_PARTITIONS_ID=-1;  // means scan all partitions.

    private HgStoreConst(){}

}
