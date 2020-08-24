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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.backend.serializer.BytesBuffer;

import net.jpountz.lz4.LZ4BlockInputStream;
import net.jpountz.lz4.LZ4BlockOutputStream;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;

public class LZ4Util {

    public static BytesBuffer compress(byte[] bytes, int blockSize) {
        LZ4Factory factory = LZ4Factory.fastestInstance();
        LZ4Compressor compressor = factory.fastCompressor();
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        LZ4BlockOutputStream lz4Output = new LZ4BlockOutputStream(
                                         baos, blockSize, compressor);
        try {
            lz4Output.write(bytes);
            lz4Output.close();
        } catch (IOException e) {
            throw new BackendException("Failed to compress", e);
        }
        return BytesBuffer.wrap(baos.toByteArray());
    }

    public static BytesBuffer decompress(byte[] bytes, int blockSize) {
        LZ4Factory factory = LZ4Factory.fastestInstance();
        LZ4FastDecompressor decompresser = factory.fastDecompressor();
        ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
        ByteArrayOutputStream baos = new ByteArrayOutputStream(blockSize);
        LZ4BlockInputStream lzInput = new LZ4BlockInputStream(bais,
                                                              decompresser);
        int count;
        byte[] buffer = new byte[blockSize];
        try {
            while ((count = lzInput.read(buffer)) != -1) {
                baos.write(buffer, 0, count);
            }
            lzInput.close();
        } catch (IOException e) {
            throw new BackendException("Failed to decompress", e);
        }
        return BytesBuffer.wrap(baos.toByteArray());
    }
}
