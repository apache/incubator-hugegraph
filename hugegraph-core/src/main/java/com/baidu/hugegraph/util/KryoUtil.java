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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.UUID;

import org.apache.tinkerpop.shaded.kryo.Kryo;
import org.apache.tinkerpop.shaded.kryo.Serializer;
import org.apache.tinkerpop.shaded.kryo.io.Input;
import org.apache.tinkerpop.shaded.kryo.io.Output;

import com.baidu.hugegraph.backend.BackendException;

public final class KryoUtil {

    private static final ThreadLocal<Kryo> kryos = new ThreadLocal<>();

    public static Kryo kryo() {
        Kryo kryo = kryos.get();
        if (kryo != null) {
            return kryo;
        }

        kryo = new Kryo();
        kryo.addDefaultSerializer(UUID.class, new Serializer<UUID>() {

            @Override
            public UUID read(Kryo kryo, Input input, Class<UUID> c) {
                return new UUID(input.readLong(), input.readLong());
            }

            @Override
            public void write(Kryo kryo, Output output, UUID uuid) {
                output.writeLong(uuid.getMostSignificantBits());
                output.writeLong(uuid.getLeastSignificantBits());
            }
        });
        kryos.set(kryo);
        return kryo;
    }

    public static byte[] toKryo(Object value) {
        try (OutputStream bos = new ByteArrayOutputStream();
             Output output = new Output(bos)) {
            kryo().writeObject(output , value);
            return output.toBytes();
        } catch (IOException e) {
            throw new BackendException("Failed to serialize: %s", e, value);
        }
    }

    public static <T> T fromKryo(byte[] value, Class<T> clazz) {
        E.checkState(value != null,
                     "Kryo value can't be null for '%s'",
                     clazz.getSimpleName());
        return kryo().readObject(new Input(value), clazz);
    }
}
