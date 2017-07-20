package com.baidu.hugegraph.backend.cache;

import java.util.LinkedList;
import java.util.List;
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

    private static final Logger logger = LoggerFactory.getLogger(Cache.class);

    private final KeyLock keyLock = new KeyLock();

    private long hits = 0;
    private long miss = 0;
    private int capacity = 0;
    // Default expire time(ms)
    private long expire = 0;

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
    public void expire(long seconds) {
        // Convert the unit from seconds to milliseconds
        this.expire = seconds * 1000;
    }

    @Override
    public void tick() {
        if (this.expire <= 0) {
            return;
        }

        long current = now();
        List<Id> expireItems = new LinkedList<>();
        for (LinkNode<Id, Object> node : this.map.values()) {
            if (current - node.time() > this.expire) {
                expireItems.add(node.key());
            }
        }

        logger.debug("Cache expire items: {} (expire {}ms)",
                     expireItems.size(), this.expire);
        for (Id id : expireItems) {
            this.remove(id);
        }
        logger.debug("Cache expired items: {} (size {})",
                     expireItems.size(), size());
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

    private static final long now() {
        return System.currentTimeMillis();
    }

    private static class LinkNode<K, V> {

        private K key;
        private V value;
        private long time;
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

        public long time() {
            return this.time;
        }

        public void reset(K key, V value) {
            this.time = now();
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
            this.rear = new LinkNode<>(null, (V) "<rear>");

            this.reset();
        }

        /**
         * Reset the head node and rear node
         * NOTE:
         *  only called by LinkedQueueNonBigLock() without lock,
         *  or called by clear() with lock(head, rear)
         */
        private void reset() {
            this.head.prev = this.empty;
            this.head.next = this.rear;

            this.rear.prev = this.head;
            this.rear.next = this.empty;

            assert this.head.next == this.rear;
            assert this.rear.prev == this.head;
        }

        public void clear() {
            synchronized (this.rear) {
                /*
                 * If someone is removing the last node by remove(),
                 * it will update the rear.prev, so we should lock it.
                 */
                assert this.rear.prev != null : this.head.next;
                synchronized (this.rear.prev) {
                    synchronized (this.head) {
                        this.reset();
                    }
                }
            }
        }

        public final LinkNode<K, V> enqueue(K key, V value) {
            return this.enqueue(new LinkNode<>(key, value));
        }

        public final LinkNode<K, V> enqueue(LinkNode<K, V> node) {
            synchronized (this.rear) {
                while (true) {
                    LinkNode<K, V> last = this.rear.prev;
                    synchronized (last) {
                        if (last != this.rear.prev) {
                            // The rear.prev has changed, try to get lock again
                            continue;
                        }
                        // Build the link between `last` and `node`
                        last.next = node;
                        node.prev = last;
                        // Build the link between `node` and the rear
                        node.next = this.rear;
                        assert this.rear.prev == last : this.rear.prev;
                        this.rear.prev = node;

                        return node;
                    }
                }
            }
        }

        public final LinkNode<K, V> dequeue() {
            synchronized (this.head) {
                /*
                 * There is no need to lock this.head.next because the remove()
                 * can't get its previous lock(that's the locked head here).
                 */
                boolean updatedTheOnlyOneElem = false;

                LinkNode<K, V> node = this.head.next;
                if (node == this.rear) {
                    // Empty queue
                    return null;
                }

                /*
                 * If there is only one element, the rear.prev would point to
                 * the deleting node, so we should lock the rear.prev to avoid
                 * enqueue() which may update the rear.prev(that's `node`).
                 */
                if (node == this.rear.prev) {
                    /*
                     * FIXME: Maybe lead dead lock if someone else is removing
                     * `node` or clearing the queue and catching the lock of
                     * the rear.(but dequeue() would only be called when the
                     * cache is full--in general one element is not full)
                     */
                    synchronized (node) {
                        if (node == this.rear.prev) {
                            updatedTheOnlyOneElem = true;
                            // Break the link between the head and `node`
                            this.head.next = node.next;
                            node.next.prev = this.head;
                        }
                    }
                }

                if (!updatedTheOnlyOneElem) {
                    // Break the link between the head and `node`(without lock)
                    this.head.next = node.next;
                    node.next.prev = this.head;
                }
                // Clear `node` links
                node.prev = this.empty;
                node.next = this.empty;

                return node;
            }
        }

        public final void remove(LinkNode<K, V> node) {
            synchronized (node) {
                assert node != this.empty;
                assert node != this.head && node != this.rear;
                if (node.next == node.prev && node.prev == this.empty) {
                    // Ignore the node if it has been removed
                    return;
                }
                while (true) {
                    LinkNode<K, V> prev = node.prev;
                    synchronized (prev) {
                        if (prev != node.prev) {
                            /*
                             * The previous node has changed(maybe it released
                             * lock after it's removed, and we got the lock),
                             * so try again until it's not changed.
                             */
                            continue;
                        }
                        assert node.next != node.prev : node.next;

                        // Build the link between node.prev and node.next
                        node.prev.next = node.next;
                        node.next.prev = node.prev;

                        // Clear node links
                        assert prev == node.prev;
                        node.prev = this.empty;
                        node.next = this.empty;

                        return;
                    }
                }
            }
        }
    }
}
