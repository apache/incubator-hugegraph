package com.baidu.hugegraph.store.business;

import com.baidu.hugegraph.rocksdb.access.ScanIterator;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.function.BiFunction;

import lombok.extern.slf4j.Slf4j;

/**
 * @author lynn.bond@hotmail.com created on 2021/11/2
 * @version 1.1.0 implements position method to pass partition-id on 2022/03/10
 */
@Slf4j
public class MultiPartitionIterator implements ScanIterator {
    public final static byte[] EMPTY_BYTES = new byte[0];
    private Queue<Integer> partitions;
    private BiFunction<Integer, byte[], ScanIterator> supplier;
    private ScanIterator iterator;
    private Integer curPartitionId;
    private Integer positionPartitionId;
    private byte[] positionKey;

    public static MultiPartitionIterator of(List<Integer> partitionIdList, BiFunction<Integer, byte[], ScanIterator> supplier) {
        return new MultiPartitionIterator(partitionIdList, supplier);
    }

    private MultiPartitionIterator(List<Integer> partitionIds, BiFunction<Integer, byte[], ScanIterator> supplier) {
        /*****************************************************************************
         ** CAUTION: MAKE SURE IT SORTED IN A FIXED ORDER! TO DO THIS IS FOR PAGING. **
         *****************************************************************************/
        Collections.sort(partitionIds);
        this.partitions = new LinkedList<>(partitionIds);
        this.supplier = supplier;
    }

    private ScanIterator getIterator() {
        if (this.partitions.isEmpty()) {
            return null;
        }
        ScanIterator buf = null;
        while (!partitions.isEmpty()) {
            this.curPartitionId = partitions.poll();
            if (!this.inPosition(this.curPartitionId)) {
                continue;
            }
            buf = supplier.apply(this.curPartitionId, getPositionKey(this.curPartitionId));
            if (buf == null) {
                continue;
            }
            if (buf.hasNext()) {
                break;
            }
        }
        if (buf == null) {
            return null;
        }
        if (!buf.hasNext()) {
            buf.close();
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
        this.init();
        if (this.iterator == null) {
            return false;
        } else {
            return true;
        }
    }

    @Override
    public boolean isValid() {
        this.init();
        if (this.iterator == null) {
            return false;
        } else {
            return true;
        }
    }

    @Override
    public <T> T next() {
        this.init();
        if (this.iterator == null) {
            throw new NoSuchElementException();
        }
        T t = this.iterator.next();
        if (!this.iterator.hasNext()) {
            this.iterator.close();
            this.iterator = null;
        }
        return t;
    }

    @Override
    public long count() {
        long count = 0;
        this.iterator = this.getIterator();
        while (this.iterator != null) {
            count += this.iterator.count();
            // this.iterator.close();
            this.iterator = this.getIterator();
        }
        return count;
    }

    /**
     * @return the current partition-id in bytes form.
     */
    @Override
    public byte[] position() {
        if (this.curPartitionId == null) {
            return EMPTY_BYTES;
        }
        return toBytes(this.curPartitionId.shortValue());
    }

    @Override
    public void seek(byte[] position) {
        if (position == null || position.length < Integer.BYTES) return;
        byte[] buf = new byte[Integer.BYTES];
        System.arraycopy(position, 0, buf, 0, Integer.BYTES);
        this.positionPartitionId = toInt(buf);
        this.positionKey = new byte[position.length - Integer.BYTES];
        System.arraycopy(position, Integer.BYTES, this.positionKey, 0, this.positionKey.length);

    }

    @Override
    public void close() {
        if (this.iterator != null) {
            this.iterator.close();
        }
    }

    private boolean inPosition(int partitionId) {
        if (this.positionPartitionId == null) return true;
        if (partitionId >= this.positionPartitionId) return true;
        return false;
    }

    private byte[] getPositionKey(int partitionId) {
        if (this.positionKey == null || this.positionKey.length == 0) return null;
        if (this.positionPartitionId == null) return null;
        if (this.positionPartitionId.intValue() == partitionId) {
            return this.positionKey;
        } else {
            return null;
        }

    }

    private static byte[] toBytes(final int i) {
        ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
        buffer.putInt(i);
        return buffer.array();
    }

    public static int toInt(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
        buffer.put(bytes);
        buffer.flip();//need flip
        return buffer.getInt();
    }

}
