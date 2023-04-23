package com.baidu.hugegraph.store;

/**
 * @author lynn.bond@hotmail.com
 */
public interface HgKvEntry {
    byte[] key();

    byte[] value();

    default int code(){
        return -1;
    }
}
