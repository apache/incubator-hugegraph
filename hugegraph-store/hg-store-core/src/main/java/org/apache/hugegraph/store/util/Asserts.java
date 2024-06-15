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

package org.apache.hugegraph.store.util;

public final class Asserts {

    public static void isTrue(boolean expression, String message) {
        if (message == null) {
            throw new IllegalArgumentException("message is null");
        }
        if (!expression) {
            throw new IllegalArgumentException(message);
        }
    }

    public static void isFalse(boolean expression, String message) {
        isTrue(!expression, message);
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
