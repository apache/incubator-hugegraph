/*
 * Copyright 2017 HugeGraph Authors
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package com.baidu.hugegraph.util;

import java.nio.charset.Charset;

import com.google.common.base.Charsets;
import com.google.common.hash.Hashing;

public final class HashUtil {

    private static final Charset CHARSET = Charsets.UTF_8;

    public static byte[] hash(byte[] bytes) {
        return Hashing.murmur3_32().hashBytes(bytes).asBytes();
    }

    public static String hash(String value) {
        return Hashing.murmur3_32().hashString(value, CHARSET).toString();
    }

    public static byte[] hash128(byte[] bytes) {
        return Hashing.murmur3_128().hashBytes(bytes).asBytes();
    }

    public static String hash128(String value) {
        return Hashing.murmur3_128().hashString(value, CHARSET).toString();
    }
}
