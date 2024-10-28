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
package org.apache.hugegraph.memory.util;

public class RoundUtil {

    // TODO: configurable
    private static final long ALIGNMENT = 8;
    private static final long MB = 1 << 20;

    public static long sizeAlign(long size) {
        long reminder = size % ALIGNMENT;
        return reminder == 0 ? size : size + ALIGNMENT - reminder;
    }

    public static long roundDelta(long reservedSize, long delta) {
        return quantizedSize(reservedSize + delta) - reservedSize;
    }

    private static long quantizedSize(long size) {
        if (size < 16 * MB) {
            return roundUp(size, MB);
        }
        if (size < 64 * MB) {
            return roundUp(size, 4 * MB);
        }
        return roundUp(size, 8 * MB);
    }

    private static long roundUp(long size, long factor) {
        return (size + factor - 1) / factor * factor;
    }
}
