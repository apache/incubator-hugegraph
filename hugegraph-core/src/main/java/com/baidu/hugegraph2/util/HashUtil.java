package com.baidu.hugegraph2.util;

import com.google.common.hash.Hashing;

/**
 * Created by jishilei on 2017/3/26.
 */
public class HashUtil {

    public static byte[] hash(byte[] bytes) {
        return Hashing.murmur3_32().hashBytes(bytes).asBytes();
    }

}
