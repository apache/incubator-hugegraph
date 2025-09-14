/*
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

package org.apache.hugegraph.meta;

import java.io.File;
import java.net.URI;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;

import org.apache.commons.io.FileUtils;
import org.apache.hugegraph.HugeException;
import org.apache.hugegraph.meta.lock.EtcdDistributedLock;
import org.apache.hugegraph.meta.lock.LockResult;
import org.apache.hugegraph.type.define.CollectionType;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.collection.CollectionFactory;

import com.google.common.base.Strings;

import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.ClientBuilder;
import io.etcd.jetcd.KV;
import io.etcd.jetcd.KeyValue;
import io.etcd.jetcd.kv.GetResponse;
import io.etcd.jetcd.lease.LeaseKeepAliveResponse;
import io.etcd.jetcd.options.DeleteOption;
import io.etcd.jetcd.options.GetOption;
import io.etcd.jetcd.options.WatchOption;
import io.etcd.jetcd.watch.WatchEvent;
import io.etcd.jetcd.watch.WatchResponse;
import io.netty.handler.ssl.ApplicationProtocolConfig;
import io.netty.handler.ssl.ApplicationProtocolNames;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;

public class EtcdMetaDriver implements MetaDriver {

    private final Client client;
    private final EtcdDistributedLock lock;

    public EtcdMetaDriver(String trustFile, String clientCertFile,
                          String clientKeyFile, Object... endpoints) {
        ClientBuilder builder = this.etcdMetaDriverBuilder(endpoints);

        SslContext sslContext = openSslContext(trustFile, clientCertFile,
                                               clientKeyFile);
        this.client = builder.sslContext(sslContext).build();
        this.lock = EtcdDistributedLock.getInstance(this.client);
    }

    public EtcdMetaDriver(Object... endpoints) {
        ClientBuilder builder = this.etcdMetaDriverBuilder(endpoints);
        this.client = builder.build();
        this.lock = EtcdDistributedLock.getInstance(this.client);
    }

    private static ByteSequence toByteSequence(String content) {
        return ByteSequence.from(content.getBytes());
    }

    private static boolean isEtcdPut(WatchEvent event) {
        return event.getEventType() == WatchEvent.EventType.PUT;
    }

    public static SslContext openSslContext(String trustFile,
                                            String clientCertFile,
                                            String clientKeyFile) {
        SslContext ssl;
        try {
            File trustManagerFile = FileUtils.getFile(trustFile);
            File keyCertChainFile = FileUtils.getFile(clientCertFile);
            File KeyFile = FileUtils.getFile(clientKeyFile);
            ApplicationProtocolConfig alpn = new ApplicationProtocolConfig(
                    ApplicationProtocolConfig.Protocol.ALPN,
                    ApplicationProtocolConfig.SelectorFailureBehavior.NO_ADVERTISE,

                    ApplicationProtocolConfig.SelectedListenerFailureBehavior
                            .ACCEPT,
                    ApplicationProtocolNames.HTTP_2);

            ssl = SslContextBuilder.forClient()
                                   .applicationProtocolConfig(alpn)
                                   .sslProvider(SslProvider.OPENSSL)
                                   .trustManager(trustManagerFile)
                                   .keyManager(keyCertChainFile, KeyFile)
                                   .build();
        } catch (Exception e) {
            throw new HugeException("Failed to open ssl context", e);
        }
        return ssl;
    }

    public ClientBuilder etcdMetaDriverBuilder(Object... endpoints) {
        int length = endpoints.length;
        ClientBuilder builder = null;
        if (endpoints[0] instanceof List && endpoints.length == 1) {
            builder = Client.builder()
                            .endpoints(((List<String>) endpoints[0])
                                               .toArray(new String[0]));
        } else if (endpoints[0] instanceof String) {
            for (int i = 1; i < length; i++) {
                E.checkArgument(endpoints[i] instanceof String,
                                "Inconsistent endpoint %s(%s) with %s(%s)",
                                endpoints[i], endpoints[i].getClass(),
                                endpoints[0], endpoints[0].getClass());
            }
            builder = Client.builder().endpoints((String[]) endpoints);
        } else if (endpoints[0] instanceof URI) {
            for (int i = 1; i < length; i++) {
                E.checkArgument(endpoints[i] instanceof String,
                                "Invalid endpoint %s(%s)",
                                endpoints[i], endpoints[i].getClass(),
                                endpoints[0], endpoints[0].getClass());
            }
            builder = Client.builder().endpoints((URI[]) endpoints);
        } else {
            E.checkArgument(false, "Invalid endpoint %s(%s)",
                            endpoints[0], endpoints[0].getClass());
        }
        return builder;
    }

    @Override
    public long keepAlive(String key, long leaseId) {
        try {
            LeaseKeepAliveResponse response =
                    this.client.getLeaseClient().keepAliveOnce(leaseId).get();
            return response.getID();
        } catch (InterruptedException | ExecutionException e) {
            // keepAlive once Failed
            return 0;
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
            throw new HugeException("Failed to get key '%s' from etcd", e, key);
        }

        if (!keyValues.isEmpty()) {
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
                                        e, key, value);
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
                    "Failed to delete key '%s' from etcd", e, key);
        }
    }

    @Override
    public void deleteWithPrefix(String prefix) {
        KV kvClient = this.client.getKVClient();
        try {
            DeleteOption option = DeleteOption.newBuilder()
                                              .isPrefix(true)
                                              .build();
            kvClient.delete(toByteSequence(prefix), option);
        } catch (Throwable e) {
            throw new HugeException(
                    "Failed to delete prefix '%s' from etcd", e, prefix);
        }
    }

    @Override
    public Map<String, String> scanWithPrefix(String prefix) {
        GetOption getOption = GetOption.newBuilder()
                                       .isPrefix(true)
                                       .build();
        GetResponse response;
        try {
            response = this.client.getKVClient().get(toByteSequence(prefix),
                                                     getOption).get();
        } catch (InterruptedException | ExecutionException e) {
            throw new HugeException("Failed to scan etcd with prefix '%s'",
                                    e, prefix);
        }
        int size = (int) response.getCount();
        Map<String, String> keyValues = CollectionFactory.newMap(
                CollectionType.JCF, size);
        for (KeyValue kv : response.getKvs()) {
            String key = kv.getKey().toString(Charset.defaultCharset());
            String value = kv.getValue().isEmpty() ? "" :
                           kv.getValue().toString(Charset.defaultCharset());
            keyValues.put(key, value);
        }
        return keyValues;
    }

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

    @Override
    public <T> Map<String, String> extractKVFromResponse(T response) {
        E.checkArgument(response instanceof WatchResponse,
                        "Invalid response type %s", response.getClass());

        Map<String, String> resultMap = new HashMap<>();
        for (WatchEvent event : ((WatchResponse) response).getEvents()) {
            // Skip if not etcd PUT event
            if (!isEtcdPut(event)) {
                continue;
            }

            String key = event.getKeyValue().getKey().toString(Charset.defaultCharset());
            String value = event.getKeyValue().getValue()
                                .toString(Charset.defaultCharset());
            if (Strings.isNullOrEmpty(key)) {
                continue;
            }
            resultMap.put(key, value);
        }
        return resultMap;
    }

    @Override
    public LockResult tryLock(String key, long ttl, long timeout) {
        return this.lock.tryLock(key, ttl, timeout);
    }

    @Override
    public boolean isLocked(String key) {
        try {
            long size = this.client.getKVClient().get(toByteSequence(key))
                                   .get().getCount();

            return size > 0;
        } catch (InterruptedException | ExecutionException e) {
            throw new HugeException("Failed to check is locked '%s'", e, key);
        }
    }

    @Override
    public void unlock(String key, LockResult lockResult) {
        this.lock.unLock(key, lockResult);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> void listen(String key, Consumer<T> consumer) {

        this.client.getWatchClient().watch(toByteSequence(key),
                                           (Consumer<WatchResponse>) consumer);
    }

    /**
     * Listen etcd key with prefix
     */
    @SuppressWarnings("unchecked")
    @Override
    public <T> void listenPrefix(String prefix, Consumer<T> consumer) {
        ByteSequence sequence = toByteSequence(prefix);
        WatchOption option = WatchOption.newBuilder().isPrefix(true).build();
        this.client.getWatchClient().watch(sequence, option, (Consumer<WatchResponse>) consumer);

    }
}
