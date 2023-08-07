/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hugegraph.pd.meta;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.config.PDConfig;
import org.apache.hugegraph.pd.store.KV;

import com.caucho.hessian.io.Hessian2Input;
import com.caucho.hessian.io.Hessian2Output;

import lombok.extern.slf4j.Slf4j;

/**
 * 自增id的实现类
 */
@Slf4j
public class IdMetaStore extends MetadataRocksDBStore {


    private static final String ID_PREFIX = "@ID@";
    private static final String CID_PREFIX = "@CID@";
    private static final String CID_SLOT_PREFIX = "@CID_SLOT@";
    private static final String CID_DEL_SLOT_PREFIX = "@CID_DEL_SLOT@";
    private static final String SEPARATOR = "@";
    private static final ConcurrentHashMap<String, Object> SEQUENCES = new ConcurrentHashMap<>();
    public static long CID_DEL_TIMEOUT = 24 * 3600 * 1000;
    private final long clusterId;

    public IdMetaStore(PDConfig pdConfig) {
        super(pdConfig);
        this.clusterId = pdConfig.getClusterId();
    }

    public static long bytesToLong(byte[] b) {
        ByteBuffer buf = ByteBuffer.wrap(b);
        return buf.getLong();
    }

    public static byte[] longToBytes(long l) {
        ByteBuffer buf = ByteBuffer.wrap(new byte[Long.SIZE]);
        buf.putLong(l);
        buf.flip();
        return buf.array();
    }

    /**
     * 获取自增id
     *
     * @param key
     * @param delta
     * @return
     * @throws PDException
     */
    public long getId(String key, int delta) throws PDException {
        Object probableLock = getLock(key);
        byte[] keyBs = (ID_PREFIX + key).getBytes(Charset.defaultCharset());
        synchronized (probableLock) {
            byte[] bs = getOne(keyBs);
            long current = bs != null ? bytesToLong(bs) : 0L;
            long next = current + delta;
            put(keyBs, longToBytes(next));
            return current;
        }
    }

    private Object getLock(String key) {
        Object probableLock = new Object();
        Object currentLock = SEQUENCES.putIfAbsent(key, probableLock);
        if (currentLock != null) {
            probableLock = currentLock;
        }
        return probableLock;
    }

    public void resetId(String key) throws PDException {
        Object probableLock = new Object();
        Object currentLock = SEQUENCES.putIfAbsent(key, probableLock);
        if (currentLock != null) {
            probableLock = currentLock;
        }
        byte[] keyBs = (ID_PREFIX + key).getBytes(Charset.defaultCharset());
        synchronized (probableLock) {
            removeByPrefix(keyBs);
        }
    }

    /**
     * 在删除name标识的cid的24小时内重复申请同一个name的cid保持同一值
     * 如此设计为了防止缓存的不一致，造成数据错误
     *
     * @param key
     * @param name cid 标识
     * @param max
     * @return
     * @throws PDException
     */
    public long getCId(String key, String name, long max) throws PDException {
        // 检测是否有过期的cid，删除图的频率比较低，此处对性能影响不大
        byte[] delKeyPrefix = (CID_DEL_SLOT_PREFIX +
                               key + SEPARATOR).getBytes(Charset.defaultCharset());
        synchronized (this) {
            scanPrefix(delKeyPrefix).forEach(kv -> {
                long[] value = (long[]) deserialize(kv.getValue());
                if (value.length >= 2) {
                    if (System.currentTimeMillis() - value[1] > CID_DEL_TIMEOUT) {
                        try {
                            delCId(key, value[0]);
                            remove(kv.getKey());
                        } catch (Exception e) {
                            log.error("Exception ", e);
                        }
                    }
                }
            });

            // 从延时删除队列恢复Key
            byte[] cidDelayKey = getCIDDelayKey(key, name);
            byte[] value = getOne(cidDelayKey);
            if (value != null) {
                // 从延迟删除队列删除
                remove(cidDelayKey);
                return ((long[]) deserialize(value))[0];
            } else {
                return getCId(key, max);
            }
        }
    }

    /**
     * 添加到删除队列，延后删除
     */
    public long delCIdDelay(String key, String name, long cid) throws PDException {
        byte[] delKey = getCIDDelayKey(key, name);
        put(delKey, serialize(new long[]{cid, System.currentTimeMillis()}));
        return cid;
    }

    /**
     * 获取自增循环不重复id, 达到上限后从0开始自增
     *
     * @param key
     * @param max id上限，达到该值后，重新从0开始自增
     * @return
     * @throws PDException
     */
    public long getCId(String key, long max) throws PDException {
        Object probableLock = getLock(key);
        byte[] keyBs = (CID_PREFIX + key).getBytes(Charset.defaultCharset());
        synchronized (probableLock) {
            byte[] bs = getOne(keyBs);
            long current = bs != null ? bytesToLong(bs) : 0L;
            long last = current == 0 ? max - 1 : current - 1;
            {   // 查找一个未使用的cid
                List<KV> kvs = scanRange(genCIDSlotKey(key, current), genCIDSlotKey(key, max));
                for (KV kv : kvs) {
                    if (current == bytesToLong(kv.getValue())) {
                        current++;
                    } else {
                        break;
                    }
                }
            }
            if (current == max) {
                current = 0;
                List<KV> kvs = scanRange(genCIDSlotKey(key, current), genCIDSlotKey(key, last));
                for (KV kv : kvs) {
                    if (current == bytesToLong(kv.getValue())) {
                        current++;
                    } else {
                        break;
                    }
                }
            }
            if (current == last) {
                return -1;
            }
            put(genCIDSlotKey(key, current), longToBytes(current));
            put(keyBs, longToBytes(current + 1));
            return current;
        }
    }

    private byte[] genCIDSlotKey(String key, long value) {
        byte[] keySlot = (CID_SLOT_PREFIX + key + SEPARATOR).getBytes(Charset.defaultCharset());
        ByteBuffer buf = ByteBuffer.allocate(keySlot.length + Long.SIZE);
        buf.put(keySlot);
        buf.put(longToBytes(value));
        return buf.array();
    }

    private byte[] getCIDDelayKey(String key, String name) {
        byte[] bsKey = (CID_DEL_SLOT_PREFIX +
                        key + SEPARATOR +
                        name).getBytes(Charset.defaultCharset());
        return bsKey;
    }

    /**
     * 删除一个循环id，释放id值
     *
     * @param key
     * @param cid
     * @return
     * @throws PDException
     */
    public long delCId(String key, long cid) throws PDException {
        return remove(genCIDSlotKey(key, cid));
    }

    private byte[] serialize(Object obj) {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
            Hessian2Output output = new Hessian2Output(bos);
            output.writeObject(obj);
            output.flush();
            return bos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private Object deserialize(byte[] bytes) {
        try (ByteArrayInputStream bis = new ByteArrayInputStream(bytes)) {
            Hessian2Input input = new Hessian2Input(bis);
            Object obj = input.readObject();
            input.close();
            return obj;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
