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

package org.apache.hugegraph.memory.consumer;

import org.apache.hugegraph.memory.pool.MemoryPool;

/**
 * This interface is used by immutable, memory-heavy objects which will be stored in off heap.
 */
public interface MemoryConsumer {

    /**
     * This method will read Off heap ByteBuf storing binary data of self.
     *
     * @return self value
     */
    Object zeroCopyReadFromByteBuf();

    /**
     * Serialize to DataOutputStream in stack first, then request an off heap ByteBuf from
     * OperatorMemoryPool based on size of DataOutputStream. Finally, serializing it to ByteBuf.
     */
    void serializeSelfToByteBuf();

    /**
     * Called after serializingSelfToByteBuf, pointing all self's on heap vars to null, in order
     * to let GC release all its on heap memory.
     */
    void releaseOriginalOnHeapVars();

    MemoryPool getOperatorMemoryPool();
}
