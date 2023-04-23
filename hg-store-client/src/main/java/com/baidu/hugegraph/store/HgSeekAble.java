package com.baidu.hugegraph.store;

/**
 * @author lynn.bond@hotmail.com created on 2022/03/11
 */
public interface HgSeekAble {
    byte[] position();

    void seek(byte[] position);
}
