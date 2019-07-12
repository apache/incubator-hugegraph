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

package com.baidu.hugegraph.backend.page;

import java.util.Base64;

import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.backend.serializer.BytesBuffer;
import com.baidu.hugegraph.util.Bytes;
import com.baidu.hugegraph.util.E;

public class PageState {

    private final byte[] position;
    private final int offset;
    private final int total;

    public PageState(byte[] position, int offset, int total) {
        E.checkNotNull(position, "position");
        this.position = position;
        this.offset = offset;
        this.total = total;
    }

    public byte[] position() {
        return this.position;
    }

    public int offset() {
        return this.offset;
    }

    public long total() {
        return this.total;
    }

    @Override
    public String toString() {
        return toString(this.toBytes());
    }

    public byte[] toBytes() {
        int length = 2 + this.position.length + 2 * BytesBuffer.INT_LEN;
        BytesBuffer buffer = BytesBuffer.allocate(length);
        buffer.writeBytes(this.position);
        buffer.writeInt(this.offset);
        buffer.writeInt(this.total);
        return buffer.bytes();
    }

    public static PageState fromString(String page) {
        return fromBytes(toBytes(page));
    }

    public static PageState fromBytes(byte[] bytes) {
        if (bytes.length == 0) {
            // The first page
            return new PageState(new byte[0], 0, 0);
        }
        try {
            BytesBuffer buffer = BytesBuffer.wrap(bytes);
            return new PageState(buffer.readBytes(), buffer.readInt(),
                                 buffer.readInt());
        } catch (Exception e) {
            throw new BackendException("Invalid page: '0x%s'",
                                       e, Bytes.toHex(bytes));
        }
    }

    public static String toString(byte[] bytes) {
        return Base64.getEncoder().encodeToString(bytes);
    }

    public static byte[] toBytes(String page) {
        try {
            return Base64.getDecoder().decode(page);
        } catch (Exception e) {
            throw new BackendException("Invalid page: '%s'", e, page);
        }
    }
}
