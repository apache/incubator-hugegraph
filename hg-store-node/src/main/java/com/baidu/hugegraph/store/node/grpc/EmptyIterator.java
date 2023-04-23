package com.baidu.hugegraph.store.node.grpc;

import com.baidu.hugegraph.rocksdb.access.ScanIterator;

/**
 * @author lynn.bond@hotmail.com on 2021/11/29
 */
final class EmptyIterator implements ScanIterator {

    @Override
    public boolean hasNext() {
        return false;
    }

    @Override
    public boolean isValid() {
        return false;
    }

    @Override
    public <T> T next() {
        return null;
    }

    @Override
    public long count() {
        return 0;
    }

    @Override
    public byte[] position() {
        return new byte[0];
    }

    @Override
    public void close() {

    }
}
