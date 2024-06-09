/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hugegraph.store.node.util;

import java.util.Collection;
import java.util.Map;
import java.util.function.Supplier;

public final class HgAssert {

    @Deprecated
    public static void isTrue(boolean expression, String message) {
        if (message == null) {
            throw new IllegalArgumentException("message is null");
        }
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

    @Deprecated
    public static void isTrue(boolean expression, RuntimeException e) {
        if (e == null) {
            throw new IllegalArgumentException("e is null");
        }
        if (!expression) {
            throw e;
        }
    }

    public static void isFalse(boolean expression, String message) {
        isTrue(!expression, message);
    }

    public static void isFalse(boolean expression, Supplier<String> msg) {
        isTrue(!expression, msg);
    }

    public static void isFalse(boolean expression, RuntimeException e) {
        isTrue(!expression, e);
    }

    public static void isArgumentValid(byte[] bytes, String parameter) {
        isFalse(isInvalid(bytes), () -> "The argument is invalid: " + parameter);
    }

    public static void isArgumentValid(String str, String parameter) {
        isFalse(isInvalid(str), () -> "The argument is invalid: " + parameter);
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
        if (objs == null || objs.length == 0 || obj == null) {
            return false;
        }
        for (Object item : objs) {
            if (obj.equals(item)) {
                return true;
            }
        }
        return false;
    }

    public static boolean isInvalid(String... strs) {
        if (strs == null || strs.length == 0) {
            return true;
        }
        for (String item : strs) {
            if (item == null || "".equals(item.trim())) {
                return true;
            }
        }
        return false;
    }

    public static boolean isInvalid(byte[] bytes) {
        return bytes == null || bytes.length == 0;
    }

    public static boolean isInvalid(Map<?, ?> map) {
        return map == null || map.isEmpty();
    }

    public static boolean isInvalid(Collection<?> list) {
        return list == null || list.isEmpty();
    }

    public static <T> boolean isContains(Collection<T> list, T item) {
        if (list == null || item == null) {
            return false;
        }
        return list.contains(item);
    }

    public static boolean isNull(Object... objs) {
        if (objs == null) {
            return true;
        }
        for (Object item : objs) {
            if (item == null) {
                return true;
            }
        }
        return false;
    }
}
