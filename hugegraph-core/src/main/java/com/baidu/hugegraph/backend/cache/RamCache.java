package com.baidu.hugegraph.backend.cache;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.baidu.hugegraph.backend.id.Id;

public class RamCache implements Cache {

    private static final Logger logger = LoggerFactory.getLogger(
            RamCache.class);

    private static int MB = 1024 * 1024;
    private static int DEFAULT_SIZE = 1 * MB;
    private static int MAX_INIT_CAP = 100 * MB;

    private long hits = 0;
    private long miss = 0;
    private long size = 0;

    // implement LRU cache
    private Map<Id, Object> store;
    private Queue<Id> sortedIds;

    public RamCache() {
        this(DEFAULT_SIZE);
    }

    public RamCache(long size) {
        if (size < 8) {
            size = 8;
        }
        this.size = size;

        long cap = size >> 3;
        if (cap > MAX_INIT_CAP) {
            cap = MAX_INIT_CAP;
        }
        // NOTE: maybe we can use LinkedHashMap if not support multi-thread
        // this.store = new ConcurrentHashMap<>((int) cap);
        // this.sortedIds = new ConcurrentLinkedQueue<>();
        this.store = new HashMap<>((int) cap);
        this.sortedIds = new LinkedList<>();
    }

    // TODO: synchronized the id instead of access() and write()
    private synchronized void access(Id id) {
        ++this.hits;
        // add to tail
        this.sortedIds.remove(id);
        this.sortedIds.add(id);
    }

    private synchronized void write(Id id, Object value) {
        assert id != null;
        if (this.sortedIds.size() >= this.size) {
            // remove the oldest
            Id removed = this.sortedIds.poll();
            assert removed != null;
            this.store.remove(removed);
            logger.debug("RamCache replace '{}' with '{}' (size={})",
                    removed, id, this.size);
        }
        // add the new item to tail
        this.sortedIds.add(id);
        this.store.put(id, value);
    }

    @Override
    public Object get(Id id) {
        Object value = this.store.get(id);
        if (value != null) {
            this.access(id);
            logger.debug("RamCache cached '{}' (hits={}, miss={})",
                    id, this.hits, this.miss);
        } else {
            logger.debug("RamCache missed '{}' (miss={}, hits={})",
                    id, ++this.miss, this.hits);
        }
        return value;
    }

    @Override
    public Object getOrFetch(Id id, Function<Id, Object> fetcher) {
        Object value = this.store.get(id);
        if (value != null) {
            this.access(id);
            logger.debug("RamCache cached '{}' (hits={}, miss={})",
                    id, this.hits, this.miss);
        } else {
            logger.debug("RamCache missed '{}' (miss={}, hits={})",
                    id, ++this.miss, this.hits);
            value = fetcher.apply(id);
            this.update(id, value);
        }
        return value;
    }

    @Override
    public void update(Id id, Object value) {
        if (id == null || value == null) {
            return;
        }
        this.write(id, value);
    }

    @Override
    public void updateIfAbsent(Id id, Object value) {
        if (id == null || value == null || this.store.containsKey(id)) {
            return;
        }
        this.write(id, value);
    }

    @Override
    public void invalidate(Id id) {
        this.store.remove(id);
    }

    @Override
    public void clear() {
        this.store.clear();
    }

    @Override
    public long size() {
        return this.size;
    }

    @Override
    public String toString() {
        return this.store.toString();
    }
}
