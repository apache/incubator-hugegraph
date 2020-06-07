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

package com.baidu.hugegraph.backend.store.raft;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;

import com.alipay.sofa.jraft.Iterator;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.core.StateMachineAdapter;
import com.baidu.hugegraph.backend.store.BackendStore;
import com.baidu.hugegraph.util.Log;

public class BackendStoreStateMachine extends StateMachineAdapter {

    private static final Logger LOG = Log.logger(BackendStoreStateMachine.class);

    private final BackendStore store;
    private final AtomicLong leaderTerm;

    public BackendStoreStateMachine(BackendStore store) {
        this.store = store;
        this.leaderTerm = new AtomicLong(-1);
    }

    @Override
    public void onApply(Iterator iter) {
        LOG.debug("Node role: {}", this.isLeader() ? "leader" : "follower");
        while (iter.hasNext()) {
            StoreCommand command;
            StoreClosure closure = (StoreClosure) iter.done();
            if (closure != null) {
                // Leader just take it out from the closure
                command = closure.command();
            } else {
                // Follower need deserializeMutation data
                ByteBuffer buffer = iter.getData();
                command = StoreCommand.fromBytes(buffer.array());
            }
            try {
                Object data = this.applyCommand(command);
                success(closure, data);
            } catch (Exception e) {
                failure(closure, e);
            }
            iter.next();
        }
    }

    private Object applyCommand(StoreCommand command) throws Exception {
        Object result = command.apply();
        LOG.debug("The store {} performed command {}", this.store,
                  command.command());
        return result;
    }

    public boolean isLeader() {
        return this.leaderTerm.get() > 0;
    }

    @Override
    public void onLeaderStart(long term) {
        LOG.debug("Become to Leader");
        this.leaderTerm.set(term);
        super.onLeaderStart(term);
    }

    @Override
    public void onLeaderStop(Status status) {
        LOG.debug("Abdicated from Leader");
        this.leaderTerm.set(-1);
        super.onLeaderStop(status);
    }

    private static void success(StoreClosure closure, Object data) {
        if (closure != null) {
            // closure is null on follower node
            closure.complete(data);
        }
    }

    private static void failure(StoreClosure closure, Exception e) {
        if (closure != null) {
            closure.failure(e);
        }
    }
}
