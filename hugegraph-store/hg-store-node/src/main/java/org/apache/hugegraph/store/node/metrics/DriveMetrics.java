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

package org.apache.hugegraph.store.node.metrics;

import java.io.File;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 2021/11/23
 */
@Deprecated
public class DriveMetrics {

    private static final long MIB = 1024 * 1024;

    // TODO: add a cache
    public Map<String, Map<String, Object>> metrics() {
        File[] rootDrive = File.listRoots();

        if (rootDrive == null) {
            return new LinkedHashMap(0);
        }

        Map<String, Map<String, Object>> metrics = new HashMap<>();

        for (File d : rootDrive) {
            Map<String, Object> buf = new HashMap<>();
            buf.put("total_space", d.getTotalSpace() / MIB);
            buf.put("free_space", d.getFreeSpace() / MIB);
            buf.put("usable_space", d.getUsableSpace() / MIB);
            buf.put("size_unit", "MB");

            metrics.put(d.getPath().replace("\\", ""), buf);

        }

        return metrics;

    }

}
