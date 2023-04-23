package com.baidu.hugegraph.store.util;

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

    public static void isFalse(boolean expression, String message) {
        isTrue(!expression, message);
    }


    public static boolean isInvalid(String... strs) {
        if (strs == null || strs.length == 0) return true;
        for (String item : strs) {
            if (item == null || "".equals(item.trim())) {
                return true;
            }
        }
        return false;
    }

    public static <T> T isNonNull(T obj) {
        if (obj == null) {
            throw new NullPointerException();
        }
        return obj;
    }

    public static <T> T isNonNull(T obj, String message) {
        if (obj == null) {
            throw new NullPointerException(message);
        }
        return obj;
    }



}