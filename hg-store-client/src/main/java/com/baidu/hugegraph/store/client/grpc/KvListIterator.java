package com.baidu.hugegraph.store.client.grpc;

import java.util.Iterator;
import java.util.List;

/**
 * @author lynn.bond@hotmail.com on 2022/4/6
 */
class KvListIterator<T> implements KvCloseableIterator{

    private final Iterator<T> iterator;

    KvListIterator(List<T> list){
        this.iterator=list.iterator();
    }

    @Override
    public void close() {
        /*Nothing to do.*/
    }

    @Override
    public boolean hasNext() {
        return this.iterator.hasNext();
    }

    @Override
    public T next() {
        return this.iterator.next();
    }
}
