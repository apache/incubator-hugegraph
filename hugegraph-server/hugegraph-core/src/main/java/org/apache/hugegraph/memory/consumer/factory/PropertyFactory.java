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

package org.apache.hugegraph.memory.consumer.factory;

import org.apache.hugegraph.memory.MemoryManager;
import org.apache.hugegraph.memory.consumer.impl.property.HugeEdgePropertyOffHeap;
import org.apache.hugegraph.memory.pool.impl.TaskMemoryPool;
import org.apache.hugegraph.schema.PropertyKey;
import org.apache.hugegraph.structure.HugeEdgeProperty;
import org.apache.hugegraph.structure.HugeElement;

public class PropertyFactory<V> {

    private MemoryManager.MemoryMode memoryMode;

    public HugeEdgeProperty<V> newHugeEdgeProperty(HugeElement owner, PropertyKey key,
                                                   V value) {
        switch (memoryMode) {

            case ENABLE_ON_HEAP_MANAGEMENT:
            case ENABLE_OFF_HEAP_MANAGEMENT:
                TaskMemoryPool taskMemoryPool = (TaskMemoryPool) MemoryManager.getInstance()
                                                                              .getCorrespondingTaskMemoryPool(
                                                                                      Thread.currentThread()
                                                                                            .getName());
                return new HugeEdgePropertyOffHeap<>(
                        taskMemoryPool.getCurrentWorkingOperatorMemoryPool(), owner, key, value);
            case DISABLE_MEMORY_MANAGEMENT:
            default:
                return new HugeEdgeProperty<>(owner, key, value);
        }
    }

    public void setMemoryMode(MemoryManager.MemoryMode memoryMode) {
        this.memoryMode = memoryMode;
    }

    private static class PropertyFactoryHolder {

        private static final PropertyFactory<Object> INSTANCE = new PropertyFactory<>();

        private PropertyFactoryHolder() {
            // empty constructor
        }
    }

    public static PropertyFactory<Object> getInstance() {
        return PropertyFactory.PropertyFactoryHolder.INSTANCE;
    }
}
