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

package org.apache.hugegraph.backend.store.hstore.fake;

import org.apache.hugegraph.backend.store.hstore.HstoreSessions;
import org.apache.hugegraph.backend.store.hstore.HstoreSessionsImpl;
import org.apache.hugegraph.pd.client.PDClient;
import org.apache.hugegraph.pd.common.Useless;
import org.apache.hugegraph.pd.grpc.Pdpb;

@Useless
public class PDIdClient extends IdClient {

    PDClient pdClient;

    public PDIdClient(HstoreSessions.Session session, String table) {
        super(session, table);
        pdClient = HstoreSessionsImpl.getDefaultPdClient();
    }

    @Override
    public Pdpb.GetIdResponse getIdByKey(String key, int delta) throws Exception {
        return pdClient.getIdByKey(key, delta);
    }

    @Override
    public Pdpb.ResetIdResponse resetIdByKey(String key) throws Exception {
        return pdClient.resetIdByKey(key);
    }

    @Override
    public void increaseId(String key, long increment) throws Exception {
        pdClient.getIdByKey(key, (int) increment);
    }
}
