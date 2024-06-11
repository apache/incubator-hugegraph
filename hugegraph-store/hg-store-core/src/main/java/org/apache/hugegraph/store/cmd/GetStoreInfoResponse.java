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

package org.apache.hugegraph.store.cmd;

import org.apache.hugegraph.pd.grpc.Metapb;
import org.apache.hugegraph.store.meta.Store;

import com.google.protobuf.InvalidProtocolBufferException;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class GetStoreInfoResponse extends HgCmdBase.BaseResponse {

    private byte[] store;

    public Store getStore() {
        try {
            return new Store(Metapb.Store.parseFrom(this.store));
        } catch (InvalidProtocolBufferException e) {
            log.error("GetStoreResponse parse exception {}", e);
        }
        return null;
    }

    public void setStore(Store store) {
        this.store = store.getProtoObj().toByteArray();
    }
}
