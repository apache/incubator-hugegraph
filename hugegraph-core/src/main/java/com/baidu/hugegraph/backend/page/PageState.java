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
import java.util.Iterator;

import com.baidu.hugegraph.HugeException;
import com.baidu.hugegraph.backend.serializer.BytesBuffer;
import com.baidu.hugegraph.iterator.Metadatable;
import com.baidu.hugegraph.util.Bytes;
import com.baidu.hugegraph.util.E;

public final class PageState {

    public static final String PAGE = "page";
    public static final String PAGE_NONE = "";

    private int offset;
    private String page;

    public PageState(int offset, String page) {
        E.checkArgument(offset >= 0, "The offset must be >= 0");
        E.checkNotNull(page, "page");
        this.offset = offset;
        this.page = page;
    }

    public void increase() {
        this.offset++;
        this.page = PAGE_NONE;
    }

    public int offset() {
        return this.offset;
    }

    public void page(String page) {
        this.page = page;
    }

    public String page() {
        return this.page;
    }

    @Override
    public String toString() {
        return Base64.getEncoder().encodeToString(this.toBytes());
    }

    public byte[] toBytes() {
        BytesBuffer buffer = BytesBuffer.allocate(256);
        buffer.writeInt(this.offset);
        buffer.writeString(this.page);
        return buffer.bytes();
    }

    public static PageState fromString(String page) {
        byte[] bytes;
        try {
            bytes = Base64.getDecoder().decode(page);
        } catch (Exception e) {
            throw new HugeException("Invalid page: '%s'", e, page);
        }
        return fromBytes(bytes);
    }

    public static PageState fromBytes(byte[] bytes) {
        if (bytes.length == 0) {
            // The first page
            return new PageState(0, PAGE_NONE);
        }
        try {
            BytesBuffer buffer = BytesBuffer.wrap(bytes);
            int offset = buffer.readInt();
            String page = buffer.readString();
            return new PageState(offset, page);
        } catch (Exception e) {
            throw new HugeException("Invalid page: '0x%s'",
                                    e, Bytes.toHex(bytes));
        }
    }

    public static String page(Iterator<?> iterator) {
        E.checkState(iterator instanceof Metadatable,
                     "Invalid paging iterator: %s", iterator.getClass());
        Object page = ((Metadatable) iterator).metadata(PAGE);
        return (String) page;
    }
}
