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

package org.apache.hugegraph.metrics;

public enum MetricsKeys {

    MAX_RESPONSE_TIME(1, "max_response_time"),

    MEAN_RESPONSE_TIME(2, "mean_response_time"),

    TOTAL_REQUEST(3, "total_request"),

    FAILED_REQUEST(4, "failed_request"),

    SUCCESS_REQUEST(5, "success_request");

    private final byte code;
    private final String name;

    MetricsKeys(int code, String name) {
        assert code < 256;
        this.code = (byte) code;
        this.name = name;
    }
}
