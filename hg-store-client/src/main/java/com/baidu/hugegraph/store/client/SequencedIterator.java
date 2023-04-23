package com.baidu.hugegraph.store.client;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Queue;

import com.baidu.hugegraph.store.HgKvEntry;
import com.baidu.hugegraph.store.HgKvIterator;
import com.baidu.hugegraph.store.HgKvOrderedIterator;

import lombok.extern.slf4j.Slf4j;

/**
 * Proxy iterator orderly, to switch next one will happen when the current one is empty.
 *
 * @author lynn.bond@hotmail.com created on 2022/03/10
 * @version 0.1.0
 */
@Slf4j
public class SequencedIterator implements HgKvIterator {
    private static final byte[] EMPTY_BYTES = new byte[0];
    private HgKvOrderedIterator<HgKvEntry> iterator;
    private Queue<HgKvOrderedIterator> queue;
    private HgKvEntry entry;
    private long limit;
    private int count;
    private byte[] position = EMPTY_BYTES;
    private byte[] position4Seeking = EMPTY_BYTES;

    SequencedIterator(List<HgKvOrderedIterator> iterators, long limit) {
        Collections.sort(iterators);
        this.queue = new LinkedList(iterators);
        this.limit = limit <= 0 ? Integer.MAX_VALUE : limit;
    }

    private HgKvOrderedIterator getIterator() {
        if (this.queue.isEmpty()) return null;
        HgKvOrderedIterator buf;
        while ((buf = this.queue.poll()) != null) {
            buf.seek(this.position4Seeking);
            if (buf.hasNext()) {
                break;
            }
        }
        if (buf == null) {
            return null;
        }
        return buf;
    }

    private void closeIterators() {
        if (this.queue.isEmpty()) return;
        HgKvOrderedIterator buf;
        while ((buf = this.queue.poll()) != null) {
            buf.close();
        }

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
        return this.position;
    }

    @Override
    public void seek(byte[] pos) {
        if (pos != null) {
            this.position4Seeking = pos;
        }
    }

    @Override
    public boolean hasNext() {
        if (this.count >= this.limit) {
            return false;
        }
        if (this.iterator == null) {
            this.iterator = this.getIterator();
        } else if (!this.iterator.hasNext()) {
            this.iterator.close();
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
        this.position = this.iterator.position();
        if (!this.iterator.hasNext()) {
            this.iterator.close();
            this.iterator = null;
        }
        this.count++;
        return this.entry;
    }

    @Override
    public void close() {
        if (this.iterator != null) {
            this.iterator.close();
        }
        this.closeIterators();
    }
}
