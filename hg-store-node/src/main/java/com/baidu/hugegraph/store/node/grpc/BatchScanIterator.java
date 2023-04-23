package com.baidu.hugegraph.store.node.grpc;

import com.baidu.hugegraph.pd.common.KVPair;
import com.baidu.hugegraph.rocksdb.access.ScanIterator;
import com.baidu.hugegraph.store.node.util.HgAssert;
import com.baidu.hugegraph.store.node.util.HgStoreConst;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

/**
 * @author lynn.bond@hotmail.com on 2022/2/28
 * @version 0.3.0 added limit support
 */
@NotThreadSafe
public final class BatchScanIterator implements ScanIterator {
    private final Supplier<KVPair<QueryCondition, ScanIterator>> batchSupplier;
    private final Supplier<Long> limitSupplier;

    private ScanIterator iterator;
    private boolean hasNext = false;
    private long curCount;
    private long curLimit;
    private AtomicBoolean closed=new AtomicBoolean();
    public static BatchScanIterator of(Supplier<KVPair<QueryCondition, ScanIterator>> iteratorSupplier,
                                       Supplier<Long> limitSupplier) {
        HgAssert.isArgumentNotNull(iteratorSupplier, "iteratorSupplier");
        HgAssert.isArgumentNotNull(limitSupplier, "limitSupplier");
        return new BatchScanIterator(iteratorSupplier, limitSupplier);
    }

    private BatchScanIterator(Supplier<KVPair<QueryCondition, ScanIterator>> iteratorSupplier,
                              Supplier<Long> limitSupplier) {
        this.batchSupplier = iteratorSupplier;
        this.limitSupplier = limitSupplier;
    }

    private ScanIterator getIterator() {
        ScanIterator buf = null;
        int count = 0;
        this.curCount = 0l;

        do {
            buf = this.batchSupplier.get().getValue();

            if (buf == null) {
                break;
            }

            if (!buf.hasNext()) {
                buf.close();
                buf = null;
            }

            if (++count == Integer.MAX_VALUE) {
                throw new RuntimeException("Do loop times more than Integer.MAX_VALUE");
            }

        } while (buf == null);

        if (buf != null) {
            Long limit = this.limitSupplier.get();

            if (limit == null || limit <= 0) {
                this.curLimit = Integer.MAX_VALUE;
            } else {
                this.curLimit = limit;
            }

        }

        return buf;

    }

    @Override
    public boolean hasNext() {

        if (this.iterator == null) {
            this.iterator = this.getIterator();
        } else if (!this.iterator.hasNext()) {
            this.iterator.close();
            this.iterator = this.getIterator();
        } else if (this.curCount == this.curLimit) {
            this.iterator.close();
            this.iterator = this.getIterator();
        }

        if (this.iterator == null) {
            return false;
        } else {
            this.hasNext = true;
            return true;
        }
    }

    @Override
    public <T> T next() {
        if (this.hasNext) {
            this.hasNext = false;
        } else {
            if (!this.hasNext()) {
                throw new NoSuchElementException();
            }
        }

        this.curCount++;

        return this.iterator.next();
    }

    @Override
    public void close() {
        if(this.closed.getAndSet(true)==false){
            if (this.iterator != null) {
                this.iterator.close();
            }
        }
    }

    @Override
    public boolean isValid() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long count() {
        throw new UnsupportedOperationException();
    }

    @Override
    public byte[] position() {
        if (this.iterator != null) {
            return this.iterator.position();
        }
        return HgStoreConst.EMPTY_BYTES;
    }
}
