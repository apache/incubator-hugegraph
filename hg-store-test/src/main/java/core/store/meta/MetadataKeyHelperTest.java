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

package core.store.meta;

import static org.junit.Assert.assertArrayEquals;

import org.apache.hugegraph.store.meta.MetadataKeyHelper;
import org.junit.Test;

public class MetadataKeyHelperTest {

    @Test
    public void testKey() {
        assertArrayEquals("HUGEGRAPH/TASK/".getBytes(), MetadataKeyHelper.getTaskPrefix());
        assertArrayEquals("HUGEGRAPH/TASK/0/".getBytes(), MetadataKeyHelper.getTaskPrefix(0));
        assertArrayEquals("HUGEGRAPH/TASK_DONE/0000000000000000".getBytes(),
                          MetadataKeyHelper.getDoneTaskKey(0));
    }
}
