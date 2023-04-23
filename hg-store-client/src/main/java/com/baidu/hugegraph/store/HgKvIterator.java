package com.baidu.hugegraph.store;

import java.io.Closeable;
import java.util.Iterator;

/**
 * @author lynn.bond@hotmail.com created on 2021/10/21
 */
public interface HgKvIterator<E> extends Iterator<E>, HgSeekAble, Closeable {

    byte[] key();

    byte[] value();

    void close();

}
