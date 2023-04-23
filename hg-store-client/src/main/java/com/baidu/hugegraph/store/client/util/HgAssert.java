package com.baidu.hugegraph.store.client.util;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * @author lynn.bond@hotmail.com
 */
public final class HgAssert {
    public static void isTrue(boolean expression, String message) {
        if (message == null) throw new IllegalArgumentException("message is null");
        if (!expression) {
            throw new IllegalArgumentException(message);
        }
    }

    public static void isTrue(boolean expression, Supplier<String> msg) {
        if (msg == null) {
            throw new IllegalArgumentException("message supplier is null");
        }
        if (!expression) {
            throw new IllegalArgumentException(msg.get());
        }
    }

    public static void isFalse(boolean expression, String message) {
        isTrue(!expression, message);
    }

    public static void isFalse(boolean expression, Supplier<String> msg) {
        isTrue(!expression, msg);
    }

    public static void isArgumentValid(byte[] bytes, String parameter) {
        isFalse(isInvalid(bytes), () -> "The argument is invalid: " + parameter);
    }

    public static void isArgumentValid(String str, String parameter) {
        isFalse(isInvalid(str), () -> "The argument is invalid: " + parameter);
    }

    public static void isArgumentValid(Collection<?> collection, String parameter) {
        isFalse(isInvalid(collection), () -> "The argument is invalid: " + parameter);
    }

    public static void isArgumentNotNull(Object obj, String parameter) {
        isTrue(obj != null, () -> "The argument is null: " + parameter);
    }

    public static void istValid(byte[] bytes, String msg) {
        isFalse(isInvalid(bytes), msg);
    }

    public static void isValid(String str, String msg) {
        isFalse(isInvalid(str), msg);
    }

    public static void isNotNull(Object obj, String msg) {
        isTrue(obj != null, msg);
    }

    public static boolean isContains(Object[] objs, Object obj) {
        if (objs == null || objs.length == 0 || obj == null) return false;
        for (Object item : objs) {
            if (obj.equals(item)) return true;
        }
        return false;
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

    public static boolean isInvalid(byte[] bytes) {
        if (bytes == null || bytes.length == 0) return true;
        return false;
    }

    public static boolean isInvalid(Map<?, ?> map) {
        if (map == null || map.isEmpty()) return true;
        return false;
    }

    public static boolean isInvalid(Collection<?> list) {
        if (list == null || list.isEmpty()) return true;
        return false;
    }

    public static <T> boolean isContains(Collection<T> list, T item) {
        if (list == null || item == null) return false;
        return list.contains(item);
    }

    public static boolean isNull(Object... objs) {
        if (objs == null) return true;
        for (Object item : objs) {
            if (item == null) {
                return true;
            }
        }
        return false;
    }
}