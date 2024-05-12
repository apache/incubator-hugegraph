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

package org.apache.hugegraph.pd.common;

public class PartitionUtils {

    public static final int MAX_VALUE = 0xffff;

    /**
     * compute key hashcode
     *
     * @param key
     * @return hashcode
     */
    public static int calcHashcode(byte[] key) {
        final int p = 16777619;
        int hash = (int) 2166136261L;
        for (byte element : key) {
            hash = (hash ^ element) * p;
        }
        hash += hash << 13;
        hash ^= hash >> 7;
        hash += hash << 3;
        hash ^= hash >> 17;
        hash += hash << 5;
        hash = hash & PartitionUtils.MAX_VALUE;
        if (hash == PartitionUtils.MAX_VALUE) {
            hash = PartitionUtils.MAX_VALUE - 1;
        }
        return hash;
    }
}
