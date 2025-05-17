package org.apache.hugegraph.pd.common;

public class PartitionUtils {

    public static final int   MAX_VALUE = 0xffff;

    /**
     * 计算key的hashcode
     *
     * @param key
     * @return hashcode
     */
    public static int calcHashcode(byte[] key) {
        final int p = 16777619;
        int hash = (int) 2166136261L;
        for (byte element : key)
            hash = (hash ^ element) * p;
        hash += hash << 13;
        hash ^= hash >> 7;
        hash += hash << 3;
        hash ^= hash >> 17;
        hash += hash << 5;
        hash = hash & PartitionUtils.MAX_VALUE;
        if ( hash == PartitionUtils.MAX_VALUE )
            hash = PartitionUtils.MAX_VALUE - 1;
        return hash;
    }
}
