package org.apache.hugegraph.pd.util;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * @param <K>
 * @param <V>
 * @author lynn.bond@hotmail.com on 2022/3/10
 */
public class HgMapCache<K, V> {
    private Map<K, V> cache = new ConcurrentHashMap<K, V>();
    private Supplier<Boolean> expiry;

    public static HgMapCache expiredOf(long interval){
        return new HgMapCache(new CycleIntervalPolicy(interval));
    }

    private HgMapCache(Supplier<Boolean> expiredPolicy) {
        this.expiry = expiredPolicy;
    }

    private boolean isExpired() {
        if (expiry != null && expiry.get()) {
            cache.clear();
            return true;
        }
        return false;
    }

    public void put(K key, V value) {
        if (key == null || value == null) return;
        this.cache.put(key, value);
    }


    public V get(K key) {
        if (isExpired()) return null;
        return this.cache.get(key);
    }

    public void removeAll() {
        this.cache.clear();
    }

    public boolean remove(K key) {
        if (key != null) {
            this.cache.remove(key);
            return true;
        }
        return false;
    }

    public Map<K, V> getAll() {
        return this.cache;
    }

    private static class CycleIntervalPolicy implements Supplier<Boolean>{
        private long expireTime=0;
        private long interval=0;

        public CycleIntervalPolicy(long interval){
            this.interval=interval;
            init();
        }
        private void init(){
            expireTime=System.currentTimeMillis()+interval;
        }

        @Override
        public Boolean get() {
            if(System.currentTimeMillis()>expireTime){
                init();
                return true;
            }
            return false;
        }

    }

}
