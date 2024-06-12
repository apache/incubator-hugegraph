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

package org.apache.hugegraph.store.node.util;

import java.util.Collections;
import java.util.List;

/**
 * created on 2021/10/22
 */
public final class HgStoreConst {

    public final static int SCAN_WAIT_CLIENT_TAKING_TIME_OUT_SECONDS = 300;

    public final static byte[] EMPTY_BYTES = new byte[0];

    public static final List EMPTY_LIST = Collections.EMPTY_LIST;

    public final static int SCAN_ALL_PARTITIONS_ID = -1;  // means scan all partitions.

    private HgStoreConst() {
    }

}
