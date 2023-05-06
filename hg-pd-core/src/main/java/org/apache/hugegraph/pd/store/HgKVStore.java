package org.apache.hugegraph.pd.store;

import com.baidu.hugegraph.pd.common.PDException;

import org.apache.hugegraph.pd.config.PDConfig;

import java.util.List;
import java.util.concurrent.TimeUnit;

public interface HgKVStore {
    void init(PDConfig config);

    void put(byte[] key, byte[] value) throws PDException;

    byte[] get(byte[] key) throws PDException;

    List<KV> scanPrefix(byte[] prefix);

    long remove(byte[] bytes) throws PDException;

    long removeByPrefix(byte[] bytes) throws PDException;

    void putWithTTL(byte[] key, byte[] value, long ttl) throws PDException;

    void putWithTTL(byte[] key, byte[] value, long ttl, TimeUnit timeUnit) throws PDException;

    byte[] getWithTTL(byte[] key) throws PDException;
    void removeWithTTL(byte[] key) throws PDException;

    List<byte[]> getListWithTTL(byte[] key) throws PDException;

    void clear() throws PDException;

    void saveSnapshot(String snapshotPath) throws PDException;

    void loadSnapshot(String snapshotPath) throws PDException;

    List<KV> scanRange(byte[] start,byte[] end);

    void close();
}
