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

package com.baidu.hugegraph.meta;

import java.net.URI;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;

import com.baidu.hugegraph.HugeException;
import com.baidu.hugegraph.type.define.CollectionType;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.collection.CollectionFactory;

import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.KV;
import io.etcd.jetcd.KeyValue;
import io.etcd.jetcd.kv.GetResponse;
import io.etcd.jetcd.options.GetOption;
import io.etcd.jetcd.watch.WatchEvent;
import io.etcd.jetcd.watch.WatchResponse;

public class EtcdMetaDriver implements MetaDriver {

    private Client client;

    @SuppressWarnings("unchecked")
    public EtcdMetaDriver(Object... endpoints) {
        int length = endpoints.length;
        if (endpoints[0] instanceof List && endpoints.length == 1) {
            this.client = Client.builder()
                                .endpoints(((List<String>) endpoints[0])
                                           .toArray(new String[0])).build();
        } else if (endpoints[0] instanceof String) {
            for (int i = 1; i < length; i++) {
                E.checkArgument(endpoints[i] instanceof String,
                                "Inconsistent endpoint %s(%s) with %s(%s)",
                                endpoints[i], endpoints[i].getClass(),
                                endpoints[0], endpoints[0].getClass());
            }
            this.client = Client.builder()
                                .endpoints((String[]) endpoints)
                                .build();
        } else if (endpoints[0] instanceof URI) {
            for (int i = 1; i < length; i++) {
                E.checkArgument(endpoints[i] instanceof String,
                                "Invalid endpoint %s(%s)",
                                endpoints[i], endpoints[i].getClass(),
                                endpoints[0], endpoints[0].getClass());
            }
            this.client = Client.builder()
                                .endpoints((URI[]) endpoints)
                                .build();
        } else {
            E.checkArgument(false, "Invalid endpoint %s(%s)",
                            endpoints[0], endpoints[0].getClass());
        }
    }

    @Override
    public String get(String key) {
        List<KeyValue> keyValues;
        KV kvClient = this.client.getKVClient();
        try {
            keyValues = kvClient.get(toByteSequence(key))
                                .get().getKvs();
        } catch (InterruptedException | ExecutionException e) {
            throw new HugeException("Failed to get key '%s' from etcd", key, e);
        }

        if (keyValues.size() > 0) {
            return keyValues.get(0).getValue().toString(Charset.defaultCharset());
        }

        return null;
    }

    @Override
    public void put(String key, String value) {
        KV kvClient = this.client.getKVClient();
        try {
            kvClient.put(toByteSequence(key), toByteSequence(value)).get();
        } catch (InterruptedException | ExecutionException e) {
            try {
                kvClient.delete(toByteSequence(key)).get();
            } catch (Throwable t) {
                throw new HugeException("Failed to put '%s:%s' to etcd",
                                        key, value, e);
            }
        }
    }

    @Override
    public void delete(String key) {
        KV kvClient = this.client.getKVClient();
        try {
            kvClient.delete(toByteSequence(key)).get();
        } catch (InterruptedException | ExecutionException e) {
            throw new HugeException(
                      "Failed to delete key '%s' from etcd", key, e);
        }
    }

    @Override
    public Map<String, String> scanWithPrefix(String prefix) {
        GetOption getOption = GetOption.newBuilder()
                                       .withPrefix(toByteSequence(prefix))
                                       .build();
        GetResponse response;
        try {
            response = this.client.getKVClient().get(toByteSequence(prefix),
                                                     getOption).get();
        } catch (InterruptedException | ExecutionException e) {
            throw new HugeException("Failed to scan etcd with prefix " +
                                    prefix, e);
        }
        int size = (int) response.getCount();
        Map<String, String> keyValues = CollectionFactory.newMap(
                                        CollectionType.JCF, size);
        for (KeyValue kv : response.getKvs()) {
            keyValues.put(kv.getKey().toString(Charset.defaultCharset()),
                          kv.getValue().toString(Charset.defaultCharset()));
        }
        return keyValues;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> List<String> extractValuesFromResponse(T response) {
        List<String> values = new ArrayList<>();
        E.checkArgument(response instanceof WatchResponse,
                        "Invalid response type %s", response.getClass());
        for (WatchEvent event : ((WatchResponse) response).getEvents()) {
            // Skip if not etcd PUT event
            if (!isEtcdPut(event)) {
                return null;
            }

            String value = event.getKeyValue().getValue()
                                .toString(Charset.defaultCharset());
            values.add(value);
        }
        return values;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> void listen(String key, Consumer<T> consumer) {

        this.client.getWatchClient().watch(toByteSequence(key),
                                           (Consumer<WatchResponse>) consumer);
    }

    private static ByteSequence toByteSequence(String content) {
        return ByteSequence.from(content.getBytes());
    }

    private static boolean isEtcdPut(WatchEvent event) {
        return event.getEventType() == WatchEvent.EventType.PUT;
    }
}
