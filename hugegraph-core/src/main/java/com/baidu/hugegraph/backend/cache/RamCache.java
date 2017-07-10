package com.baidu.hugegraph.backend.cache;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.baidu.hugegraph.backend.id.Id;
import com.baidu.hugegraph.concurrent.KeyLock;

public class RamCache implements Cache {

    public static final int MB = 1024 * 1024;
    public static final int DEFAULT_SIZE = 1 * MB;
    public static final int MAX_INIT_CAP = 100 * MB;

    private static final Logger logger = LoggerFactory.getLogger(RamCache.class);

    private final KeyLock keyLock = new KeyLock();

    private long hits = 0;
    private long miss = 0;
    private int capacity = 0;

    // Implement LRU cache
    private Map<Id, LinkNode<Id, Object>> map;
    private LinkedQueueNonBigLock<Id, Object> queue;

    public RamCache() {
        this(DEFAULT_SIZE);
    }

    // NOTE: count in number of items, not in bytes
    public RamCache(int capacity) {
        if (capacity < 8) {
            capacity = 8;
        }
        this.capacity = capacity;

        int initialCapacity = capacity >> 3;
        if (initialCapacity > MAX_INIT_CAP) {
            initialCapacity = MAX_INIT_CAP;
        }

        this.map = new ConcurrentHashMap<>(initialCapacity);
        this.queue = new LinkedQueueNonBigLock<>();
    }

    private void access(LinkNode<Id, Object> node) {
        // Ignore concurrent write for hits
        ++this.hits;

        Id id = node.key();
        this.keyLock.lock(id);
        try {
            // Add to tail
            this.queue.remove(node);
            this.queue.enqueue(node);
        } finally {
            this.keyLock.unlock(id);
        }
    }

    private void write(Id id, Object value) {
        assert id != null;

        this.keyLock.lock(id);
        try {
            if (this.map.size() < this.capacity) {
                // Add the new item to tail, then map it
                this.map.put(id, this.queue.enqueue(id, value));
            } else {
                // Remove the oldest
                LinkNode<Id, Object> removed = this.queue.dequeue();
                assert removed != null;
                this.map.remove(removed.key());
                logger.debug("RamCache replace '{}' with '{}' (capacity={})",
                             removed.value(), id, this.capacity);

                // Reuse the removed node
                LinkNode<Id, Object> newest = removed;
                newest.reset(id, value);
                this.map.put(id, newest);
            }
        } finally {
            this.keyLock.unlock(id);
        }
    }

    private void remove(Id id) {
        assert id != null;

        this.keyLock.lock(id);
        try {
            this.queue.remove(this.map.remove(id));
        } finally {
            this.keyLock.unlock(id);
        }
    }

    @Override
    public Object get(Id id) {
        LinkNode<Id, Object> node = this.map.get(id);
        if (node != null) {
            this.access(node);
            logger.debug("RamCache cached '{}' (hits={}, miss={})",
                         id, this.hits, this.miss);
            return node.value();
        } else {
            logger.debug("RamCache missed '{}' (miss={}, hits={})",
                         id, ++this.miss, this.hits);
            return null;
        }
    }

    @Override
    public Object getOrFetch(Id id, Function<Id, Object> fetcher) {
        LinkNode<Id, Object> node = this.map.get(id);
        if (node != null) {
            this.access(node);
            logger.debug("RamCache cached '{}' (hits={}, miss={})",
                         id, this.hits, this.miss);
            return node.value();
        } else {
            logger.debug("RamCache missed '{}' (miss={}, hits={})",
                         id, ++this.miss, this.hits);
            Object value = fetcher.apply(id);
            this.update(id, value);
            return value;
        }
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
        if (id == null || value == null || this.map.containsKey(id)) {
            return;
        }
        this.write(id, value);
    }

    @Override
    public void invalidate(Id id) {
        if (id == null || !this.map.containsKey(id)) {
            return;
        }
        this.remove(id);
    }

    @Override
    public void clear() {
        // TODO: synchronized
        this.map.clear();
        this.queue.clear();
    }

    @Override
    public long capacity() {
        return this.capacity;
    }

    @Override
    public long size() {
        return this.map.size();
    }

    @Override
    public String toString() {
        return this.map.toString();
    }

    private static class LinkNode<K, V> {

        private K key;
        private V value;
        private LinkNode<K, V> prev;
        private LinkNode<K, V> next;

        public LinkNode(K key, V value) {
            this.reset(key, value);
        }

        public final K key() {
            return this.key;
        }

        public final V value() {
            return this.value;
        }

        public void reset(K key, V value) {
            this.key = key;
            this.value = value;
            this.prev = this.next = null;
        }

        @Override
        public String toString() {
            return this.value == null ? null : this.value.toString();
        }
    }

    private static class LinkedQueueNonBigLock<K, V> {

        private final LinkNode<K, V> empty;
        private final LinkNode<K, V> head;
        private LinkNode<K, V> rear;

        @SuppressWarnings("unchecked")
        public LinkedQueueNonBigLock() {
            this.empty = new LinkNode<>(null, (V) "<empty>");
            this.head = new LinkNode<>(null, (V) "<head>");
            this.rear = this.head;

            this.clear();
        }

        public void clear() {
            synchronized (this.head) {
                synchronized (this.rear) {
                    this.head.next = this.empty;
                    this.head.prev = this.empty;

                    this.rear = this.head;
                }
            }
        }

        public final LinkNode<K, V> enqueue(K key, V value) {
            return this.enqueue(new LinkNode<>(key, value));
        }

        public final LinkNode<K, V> enqueue(LinkNode<K, V> node) {
            synchronized (this.rear) {
                node.next = this.empty;
                // Build the link between rear and node
                this.rear.next = node;
                node.prev = this.rear;
                // Reset rear
                this.rear = node;
            }
            return node;
        }

        public final LinkNode<K, V> dequeue() {
            synchronized (this.head) {
                if (this.rear == this.head) {
                    return null;
                }

                /* If there is only one element, the rear would point to the
                 * deleting node, so we should lock the rear to avoid enqueue()
                 */
                LinkNode<K, V> node = this.head.next;
                if (node == this.rear) {
                    synchronized (this.rear) {
                        if (node == this.rear) {
                            this.rear = this.head;
                        }
                    }
                }

                // Break the link between head and node
                this.head.next = node.next;
                node.next.prev = this.head;
                // Clear node links
                node.prev = this.empty;
                node.next = this.empty;

                return node;
            }
        }

        public final void remove(LinkNode<K, V> node) {
            synchronized (node) {
                synchronized (node.prev) {
                    if (node == this.rear) {
                        this.rear = node.prev;
                    }
                    // Build the link between node.prev and node.next
                    node.prev.next = node.next;
                    node.next.prev = node.prev;
                    // Clear node links
                    node.prev = this.empty;
                    node.next = this.empty;
                }
            }
        }
    }
}
