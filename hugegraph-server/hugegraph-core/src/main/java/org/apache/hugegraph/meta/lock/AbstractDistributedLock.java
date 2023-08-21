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

package org.apache.hugegraph.meta.lock;

import org.apache.hugegraph.pd.client.KvClient;

import io.etcd.jetcd.Client;

/**
 * @author zhangyingjie
 * @date 2022/6/18
 **/
public abstract class AbstractDistributedLock {

    private static volatile AbstractDistributedLock defaultLock;

    public static AbstractDistributedLock getInstance(AutoCloseable client) {
        if (defaultLock == null) {
            synchronized (AbstractDistributedLock.class) {
                if (defaultLock == null) {
                    if (client instanceof Client) {
                        defaultLock = DistributedLock.getInstance((Client) client);
                    } else {
                        defaultLock = new PdDistributedLock((KvClient) client);
                    }

                }
            }
        }
        return defaultLock;
    }

    public abstract LockResult lock(String key, long ttl);

    public abstract void unLock(String key, LockResult lockResult);
}
