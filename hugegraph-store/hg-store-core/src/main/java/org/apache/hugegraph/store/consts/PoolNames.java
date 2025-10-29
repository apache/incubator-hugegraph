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

package org.apache.hugegraph.store.consts;

/**
 * @date 2023/10/30
 **/
public class PoolNames {

    public static final String GRPC = "hg-grpc";
    //todo Unify SCAN and SCAN_V2
    public static final String SCAN = "hg-scan";
    public static final String SCAN_V2 = "hg-scan-v2";
    public static final String I_JOB = "hg-i-job";
    public static final String U_JOB = "hg-u-job";
    public static final String COMPACT = "hg-compact";
    public static final String HEARTBEAT = "hg-heartbeat";
    public static final String P_HEARTBEAT = "hg-p-heartbeat";

}
