package com.baidu.hugegraph.rocksdb.access.util;

import java.util.Collection;
import java.util.Map;
import java.util.function.Supplier;

/**
 * @author lynn.bond@hotmail.com
 */
public final class Asserts {

    public static void isTrue(boolean expression, String message) {
        if (message== null) throw new IllegalArgumentException("message is null");
        if (!expression) {
            throw new IllegalArgumentException(message);
        }
    }

    public static void isTrue(boolean expression, Supplier<RuntimeException> s) {
        if (s == null) throw new IllegalArgumentException("Supplier<RuntimeException> is null");
        if (!expression) {
            throw s.get();
        }
    }

}