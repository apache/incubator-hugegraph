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

package org.apache.hugegraph.metrics;

public enum MetricsKeys {
    // 最大响应时间 Maximum response time
    // 平均响应时间 Mean response time
    // 请求总数 Total Requests
    // 失败数   Failed Requests
    // 成功数   Success Requests

    MAX_RESPONSE_TIME(1, "MAX_RESPONSE_TIME"),

    MEAN_RESPONSE_TIME(2, "MEAN_RESPONSE_TIME"),

    TOTAL_REQUEST(3, "TOTAL_REQUEST"),

    FAILED_REQUEST(4, "FAILED_REQUEST"),

    SUCCESS_REQUEST(5, "SUCCESS_REQUEST"),
    ;

    private final byte code;
    private final String name;


    MetricsKeys(int code, String name) {
        assert code < 256;
        this.code = (byte) code;
        this.name = name;
    }
}
