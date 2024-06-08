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

package org.apache.hugegraph.store.raft;

import org.apache.hugegraph.store.util.HgStoreException;

/**
 * 接收raft发送的数据
 */
public interface RaftTaskHandler {

    boolean invoke(final int groupId, final byte[] request, RaftClosure response) throws
                                                                                  HgStoreException;

    boolean invoke(final int groupId, final byte methodId, final Object req,
                   RaftClosure response) throws HgStoreException;
}
