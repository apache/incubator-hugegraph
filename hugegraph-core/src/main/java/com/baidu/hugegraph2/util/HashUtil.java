package com.baidu.hugegraph2.util;

import java.nio.charset.Charset;

import com.google.common.hash.Hashing;

/**
 * Created by jishilei on 2017/3/26.
 */
public class HashUtil {

    public static byte[] hash(byte[] bytes) {
        return Hashing.murmur3_32().hashBytes(bytes).asBytes();
    }

    public static String hash(String value) {
        final Charset charset = Charset.forName("UTF-8");
        return Hashing.murmur3_32().hashString(value, charset).toString();
    }

}
