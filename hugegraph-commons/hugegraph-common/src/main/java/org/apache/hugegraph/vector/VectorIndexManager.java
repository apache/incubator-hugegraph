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

package org.apache.hugegraph.vector;

import java.util.Iterator;
import java.util.Set;

public class VectorIndexManager<Id> {

    private final VectorIndexStateStore<Id> stateStore;
    private final VectorIndexRuntime<Id> runtime;
    private final VectorTaskScheduler scheduler;

    public VectorIndexManager(VectorIndexStateStore<Id> stateStore,
                              VectorIndexRuntime<Id> runtime,
                              VectorTaskScheduler scheduler) {
        this.stateStore = stateStore;
        this.runtime = runtime;
        this.scheduler = scheduler;
    }

    public void init() {
        this.runtime.init();
    }

    public void stop() {
        this.runtime.stop();
        this.stateStore.stop();
    }

    public void signal(Id indexLableId) {
        this.scheduler.execute(() -> this.processIndex(indexLableId));
    }

    private void processIndex(Id indexLableId) {

        long currentSequence = this.runtime.getCurrentSequence(indexLableId);

        Iterator<VectorRecord> it =
                stateStore.scanDeltas(indexLableId, currentSequence < 0 ? 0 : currentSequence)
                          .iterator();

        this.runtime.update(indexLableId, it);
    }

    public Set<Id> searchVector(Id indexLableId, float[] vector, int topK) {
        Set<Id> result = null;
        result = stateStore.getVertex(indexLableId, runtime.search(indexLableId, vector, topK));
        return result;
    }
}
