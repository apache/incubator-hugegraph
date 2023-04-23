package com.baidu.hugegraph.store;

/**
 * @author lynn.bond@hotmail.com
 */
public interface HgTokvEntry {
    String table();
    HgOwnerKey ownerKey();
    byte[] value();
}
