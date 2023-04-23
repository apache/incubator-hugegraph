package com.baidu.hugegraph.store.client.grpc;

import java.io.Closeable;
import java.util.Iterator;

/**
 * @author lynn.bond@hotmail.com on 2022/3/16
 */
public interface  KvCloseableIterator<T> extends Iterator<T>, Closeable {
    public void close();
}
