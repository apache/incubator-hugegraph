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

package org.apache.hugegraph.store.node.listener;

import java.util.concurrent.ThreadPoolExecutor;

import org.apache.hugegraph.store.node.grpc.HgStoreStreamImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextClosedEvent;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ContextClosedListener implements ApplicationListener<ContextClosedEvent> {

    @Autowired
    HgStoreStreamImpl storeStream;

    @Override
    public void onApplicationEvent(ContextClosedEvent event) {
        try {
            log.info("closing scan threads....");
            ThreadPoolExecutor executor = storeStream.getRealExecutor();
            if (executor != null) {
                try {
                    executor.shutdownNow();
                } catch (Exception e) {

                }
            }
        } catch (Exception ignored) {

        } finally {
            log.info("closed scan threads");
        }
    }
}
