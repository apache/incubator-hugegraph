package com.baidu.hugegraph.rocksdb.access;

import com.baidu.hugegraph.rocksdb.access.RocksDBSession.BackendColumn;
import com.baidu.hugegraph.util.Bytes;
import com.baidu.hugegraph.util.E;
import lombok.extern.slf4j.Slf4j;
import org.rocksdb.RocksIterator;

import java.util.Arrays;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class RocksDBScanIterator<T> implements ScanIterator {
    private static final byte[] EMPTY_VALUE = new byte[0];
    private final RocksIterator rawIt;
    private final byte[] keyBegin;
    private final byte[] keyEnd;
    private final int scanType;

    private AtomicBoolean closed = new AtomicBoolean(false);
    private byte[] key;
    private boolean matched;
    private final RocksDBSession.RefCounter iterReference;

    public RocksDBScanIterator(RocksIterator rawIt, byte[] keyBegin, byte[] keyEnd,
                               int scanType, RocksDBSession.RefCounter iterReference) {
        this.rawIt = rawIt;
        this.keyBegin = keyBegin;
        this.keyEnd = keyEnd;
        this.scanType = scanType;

        this.key = keyBegin;
        this.matched = false;
        this.iterReference = iterReference;
        this.seek();
    }

    private void checkArguments() {
        E.checkArgument(!(this.match(ScanIterator.Trait.SCAN_PREFIX_BEGIN) &&
                        this.match(ScanIterator.Trait.SCAN_PREFIX_END)),
                "Can't set SCAN_PREFIX_WITH_BEGIN and " +
                        "SCAN_PREFIX_WITH_END at the same time");

        E.checkArgument(!(this.match(ScanIterator.Trait.SCAN_PREFIX_BEGIN) &&
                        this.match(ScanIterator.Trait.SCAN_GT_BEGIN)),
                "Can't set SCAN_PREFIX_WITH_BEGIN and " +
                        "SCAN_GT_BEGIN/SCAN_GTE_BEGIN at the same time");

        E.checkArgument(!(this.match(ScanIterator.Trait.SCAN_PREFIX_END) &&
                        this.match(ScanIterator.Trait.SCAN_LT_END)),
                "Can't set SCAN_PREFIX_WITH_END and " +
                        "SCAN_LT_END/SCAN_LTE_END at the same time");

        if (this.match(ScanIterator.Trait.SCAN_PREFIX_BEGIN)) {
            E.checkArgument(this.keyBegin != null,
                    "Parameter `keyBegin` can't be null " +
                            "if set SCAN_PREFIX_WITH_BEGIN");
            E.checkArgument(this.keyEnd == null,
                    "Parameter `keyEnd` must be null " +
                            "if set SCAN_PREFIX_WITH_BEGIN");
        }

        if (this.match(ScanIterator.Trait.SCAN_PREFIX_END)) {
            E.checkArgument(this.keyEnd != null,
                    "Parameter `keyEnd` can't be null " +
                            "if set SCAN_PREFIX_WITH_END");
        }

        if (this.match(ScanIterator.Trait.SCAN_GT_BEGIN)) {
            E.checkArgument(this.keyBegin != null,
                    "Parameter `keyBegin` can't be null " +
                            "if set SCAN_GT_BEGIN or SCAN_GTE_BEGIN");
        }

        if (this.match(ScanIterator.Trait.SCAN_LT_END)) {
            E.checkArgument(this.keyEnd != null,
                    "Parameter `keyEnd` can't be null " +
                            "if set SCAN_LT_END or SCAN_LTE_END");
        }
    }

    @Override
    public boolean hasNext() {

        if (this.closed.get()) {
            //log.warn("Iterator has been closed");
            return false;
        }
        this.matched = this.rawIt.isOwningHandle();
        if (!this.matched) {
            // Maybe closed
            return this.matched;
        }

        this.matched = this.rawIt.isValid();
        if (this.matched) {
            // Update position for paging
            this.key = this.rawIt.key();
            this.matched = this.filter(this.key);
        }
        if (!this.matched) {
            // The end
            this.key = null;
            // Free the iterator if finished
            this.close();
        }
        return this.matched;
    }

    private void seek() {
        if (this.closed.get()) {
            log.warn("Iterator has been closed");
            return;
        }
        if (this.keyBegin == null) {
            // Seek to the first if no `keyBegin`
            this.rawIt.seekToFirst();
        } else {
            /*
             * Seek to `keyBegin`:
             * if set SCAN_GT_BEGIN/SCAN_GTE_BEGIN (key > / >= 'xx')
             * or if set SCAN_PREFIX_WITH_BEGIN (key prefix with 'xx')
             */
            this.rawIt.seek(this.keyBegin);

            // Skip `keyBegin` if set SCAN_GT_BEGIN (key > 'xx')
            if (this.match(ScanIterator.Trait.SCAN_GT_BEGIN) &&
                    !this.match(ScanIterator.Trait.SCAN_GTE_BEGIN)) {
                while (this.rawIt.isValid() &&
                        Bytes.equals(this.rawIt.key(), this.keyBegin)) {
                    this.rawIt.next();
                }
            }
        }
    }

    @Override
    public boolean isValid() {
        return this.rawIt.isValid();
    }

    @Override
    public BackendColumn next() {
        if (this.closed.get()) {
            log.warn("Iterator has been closed");
            throw new NoSuchElementException();
        }
        if (!this.matched) {
            if (!this.hasNext()) {
                throw new NoSuchElementException();
            }
        }

        BackendColumn col = BackendColumn.of(this.key,
                this.match(Trait.SCAN_KEYONLY) ? EMPTY_VALUE : this.rawIt.value());

        this.rawIt.next();
        this.matched = false;

        return col;
    }

    @Override
    public long count() {
        long count = 0L;
        while (this.hasNext()) {
            this.rawIt.next();
            count++;
        }
        return count;
    }

    @Override
    public byte[] position() {
        return this.key;
    }

    private boolean filter(byte[] v) {

        if (this.match(ScanIterator.Trait.SCAN_PREFIX_BEGIN)) {
            return Bytes.prefixWith(v, this.keyBegin);

        } else if (this.match(ScanIterator.Trait.SCAN_PREFIX_END)) {
            assert this.keyEnd != null;
            return Bytes.prefixWith(v, this.keyEnd);

        } else if (this.match(ScanIterator.Trait.SCAN_LT_END)) {
            assert this.keyEnd != null;

            if (this.match(ScanIterator.Trait.SCAN_LTE_END)) {
                v = Arrays.copyOfRange(v, 0, this.keyEnd.length);
                return Bytes.compare(v, this.keyEnd) <= 0;
            } else {
                return Bytes.compare(v, this.keyEnd) < 0;
            }
        } else {

            return true;
        }
    }

    @Override
    public void close() {
        if (this.closed.getAndSet(true) == false) {
            if (this.rawIt.isOwningHandle()) {
                this.rawIt.close();
            }
            this.iterReference.release();
        }
    }

    private boolean match(int expected) {
        return (expected & this.scanType) == expected;
    }

}
