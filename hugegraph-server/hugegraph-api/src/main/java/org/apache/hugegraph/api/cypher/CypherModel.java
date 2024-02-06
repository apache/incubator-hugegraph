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

package org.apache.hugegraph.api.cypher;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * As same as response of GremlinAPI
 */
public class CypherModel {

    public String requestId;
    public Status status = new Status();
    public Result result = new Result();

    public static CypherModel dataOf(String requestId, List<Object> data) {
        CypherModel res = new CypherModel();
        res.requestId = requestId;
        res.status.code = 200;
        res.result.data = data;
        return res;
    }

    public static CypherModel failOf(String requestId, String message) {
        CypherModel res = new CypherModel();
        res.requestId = requestId;
        res.status.code = 400;
        res.status.message = message;
        return res;
    }

    private CypherModel() {
    }

    public static class Status {
        public String message = "";
        public int code;
        public Map<String, Object> attributes = Collections.EMPTY_MAP;
    }

    private static class Result {
        public List<Object> data;
        public Map<String, Object> meta = Collections.EMPTY_MAP;
    }

}
