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

package org.apache.hugegraph.pd.client;

import java.util.concurrent.ConcurrentHashMap;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

public class Channels {

    private static final ConcurrentHashMap<String, ManagedChannel> chs = new ConcurrentHashMap<>();

    public static ManagedChannel getChannel(String target) {

        ManagedChannel channel;
        if ((channel = chs.get(target)) == null || channel.isShutdown() || channel.isTerminated()) {
            synchronized (chs) {
                if ((channel = chs.get(target)) == null || channel.isShutdown() ||
                    channel.isTerminated()) {
                    channel = ManagedChannelBuilder.forTarget(target).usePlaintext().build();
                    chs.put(target, channel);
                }
            }
        }

        return channel;
    }
}
