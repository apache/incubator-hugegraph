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

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;

public class LockResult {

    private boolean lockSuccess;
    private long leaseId;
    private ScheduledExecutorService service;
    private ScheduledFuture<?> future;

    public void lockSuccess(boolean isLockSuccess) {
        this.lockSuccess = isLockSuccess;
    }

    public boolean lockSuccess() {
        return this.lockSuccess;
    }

    public long getLeaseId() {
        return this.leaseId;
    }

    public void setLeaseId(long leaseId) {
        this.leaseId = leaseId;
    }

    public ScheduledExecutorService getService() {
        return this.service;
    }

    public void setService(ScheduledExecutorService service) {
        this.service = service;
    }

    public ScheduledFuture<?> getFuture() {
        return future;
    }

    public void setFuture(ScheduledFuture<?> future) {
        this.future = future;
    }
}
