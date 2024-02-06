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

package org.apache.hugegraph.backend.store.raft;

import java.util.Map;

import org.apache.hugegraph.job.SysJob;
import org.apache.hugegraph.util.E;
import org.apache.hugegraph.util.JsonUtil;

public class RaftRemovePeerJob extends SysJob<String> {

    public static final String TASK_TYPE = "raft_remove_peer";

    @Override
    public String type() {
        return TASK_TYPE;
    }

    @Override
    public String execute() throws Exception {
        String input = this.task().input();
        E.checkArgumentNotNull(input, "The input can't be null");
        @SuppressWarnings("unchecked")
        Map<String, Object> map = JsonUtil.fromJson(input, Map.class);

        Object value = map.get("endpoint");
        E.checkArgument(value instanceof String,
                        "Invalid endpoint value '%s'", value);
        String endpoint = (String) value;
        return this.graph().raftGroupManager().removePeer(endpoint);
    }
}
