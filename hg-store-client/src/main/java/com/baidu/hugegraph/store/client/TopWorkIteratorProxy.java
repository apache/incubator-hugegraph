package com.baidu.hugegraph.store.client;

import com.baidu.hugegraph.store.HgKvEntry;
import com.baidu.hugegraph.store.HgKvIterator;

import java.io.IOException;
import java.util.*;

/**
 * @author lynn.bond@hotmail.com created on 2021/10/21
 * @version 0.1.0
 */
class TopWorkIteratorProxy implements HgKvIterator {
    private static final byte[] EMPTY_BYTES = new byte[0];
    private HgKvIterator<HgKvEntry> iterator;
    private Queue<HgKvIterator> queue;
    private HgKvEntry entry;
    private long limit;
    private int count;

    TopWorkIteratorProxy(List<HgKvIterator> iterators, long limit) {
        this.queue = new LinkedList<>(iterators);
        this.limit = limit <= 0 ? Integer.MAX_VALUE : limit;
    }

    private HgKvIterator getIterator() {
        if (this.queue.isEmpty()) return null;

        HgKvIterator buf = null;

        while ((buf = this.queue.poll()) != null) {
            if (buf.hasNext()) {
                break;
            }
        }

        if (buf == null) {
            return null;
        }

        this.queue.add(buf);

        return buf;
    }
    private void closeIterators() {
        if (this.queue.isEmpty()) return;

        HgKvIterator buf;

        while ((buf = this.queue.poll()) != null) {
            buf.close();
        }

    }
    private void setIterator() {
        this.iterator = null;
    }

    @Override
    public byte[] key() {
        if (this.entry != null) {
            return this.entry.key();
        }
        return null;
    }

    @Override
    public byte[] value() {
        if (this.entry != null) {
            return this.entry.value();
        }
        return null;
    }

    @Override
    public byte[] position() {
        return this.iterator != null ? this.iterator.position() : EMPTY_BYTES;
    }

    @Override
    public void seek(byte[] position) {
        if(this.iterator!=null)this.iterator.seek(position);
    }

    @Override
    public boolean hasNext() {
        if (this.count >= this.limit) {
            return false;
        }
        if (this.iterator == null) {
            this.iterator = this.getIterator();
        }
        if (this.iterator == null) {
            return false;
        } else {
            return true;
        }

    }

    @Override
    public Object next() {
        if (this.iterator == null) {
            hasNext();
        }
        if (this.iterator == null) {
            throw new NoSuchElementException();
        }
        this.entry = this.iterator.next();
        this.setIterator();
        this.count++;
        return this.entry;
    }

    @Override
    public void close()  {
        if(this.iterator!=null)this.iterator.close();
        this.closeIterators();
    }
}
