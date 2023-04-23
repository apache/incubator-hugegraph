package com.baidu.hugegraph.store;

/**
 * @author lynn.bond@hotmail.com
 */
public interface HgTkvEntry {
    String table();
    byte[] key();
    byte[] value();
}
