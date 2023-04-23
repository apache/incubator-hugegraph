package com.baidu.hugegraph.store.node.grpc;

import com.baidu.hugegraph.rocksdb.access.ScanIterator;

import java.util.NoSuchElementException;
import java.util.function.Supplier;

/**
 * This is a wrapper of the ScanIterator that provides a mechanism
 * to set a threshold value in order to abort the iterating operation.
 *
 * @author lynn.bond@hotmail.com on 2023/2/8
 */
final class FusingScanIterator implements ScanIterator {
    public static final byte[] EMPTY_BYTES = new byte[0];
    private long max;
    private long accumulator;
    private Supplier<ScanIterator> supplier;
    private ScanIterator iterator;
    private byte[] position = EMPTY_BYTES;

    public static FusingScanIterator maxOf(long maxThreshold, Supplier<ScanIterator> iteratorSupplier) {
        FusingScanIterator res = new FusingScanIterator();
        res.max = maxThreshold;
        res.supplier = iteratorSupplier;
        return res;
    }

    private FusingScanIterator() {
    }

    private ScanIterator getIterator() {
        ScanIterator buf = this.supplier.get();
        if (buf == null) {
            return null;
        }
        if (!buf.hasNext()) {
            buf = null;
        }
        return buf;
    }

    private void init() {
        if (this.iterator == null) {
            this.iterator = this.getIterator();
        }
    }

    @Override
    public boolean hasNext() {
        if (this.isThresholdExceeded()) {
            return false;
        }
        if (this.iterator == null) {
            this.iterator = this.getIterator();
        }
        return this.iterator != null;
    }

    @Override
    public boolean isValid() {
        return hasNext();
    }

    @Override
    public byte[] position() {
        return this.position;
    }

    /**
     * @return true, when the threshold is exceeded.
     */
    private boolean isThresholdExceeded() {
        return this.accumulator >= this.max;
    }

    @Override
    public <T> T next() {
        if (this.isThresholdExceeded()) {
            throw new NoSuchElementException();
        }
        this.init();
        if (this.iterator == null) {
            throw new NoSuchElementException();
        }
        T t = this.iterator.next();
        position = this.iterator.position();
        this.accumulator++;
        if (!this.iterator.hasNext() || this.isThresholdExceeded()) {
            this.iterator.close();
            this.iterator = null;
        }
        return t;
    }

    @Override
    public void close() {
        if (this.iterator != null) {
            this.iterator.close();
        }
    }
}