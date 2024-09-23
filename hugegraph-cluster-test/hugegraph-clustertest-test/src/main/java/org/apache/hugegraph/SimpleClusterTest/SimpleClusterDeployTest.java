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

package org.apache.hugegraph.SimpleClusterTest;

import java.io.IOException;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

public class SimpleClusterDeployTest extends BaseSimpleTest {

    @Test
    public void testPDNodesDeployment() throws IOException {
        List<String> addrs = env.getPDRestAddrs();
        for (String addr : addrs) {
            String url = addr;
            String[] cmds = {"curl", url};
            StringBuffer sb = new StringBuffer();
            for (int i = 0; i < cmds.length; i++) {
                sb.append(cmds[i] + " ");
            }
            String responseMsg = execCmd(cmds);
            Assert.assertEquals(responseMsg, "");
        }
    }

    @Test
    public void testStoreNodesDeployment() throws IOException {
        List<String> addrs = env.getStoreRestAddrs();
        for (String addr : addrs) {
            String[] cmds = {"curl", addr};
            StringBuilder sb = new StringBuilder();
            for (String cmd : cmds) {
                sb.append(cmd).append(" ");
            }
            String responseMsg = execCmd(cmds);
            Assert.assertTrue(responseMsg.startsWith("{"));
        }
    }

    @Test
    public void testServerNodesDeployment() throws IOException {
        List<String> addrs = env.getServerRestAddrs();
        for (String addr : addrs) {
            String[] cmds = {"curl", addr};
            StringBuffer sb = new StringBuffer();
            for (String cmd : cmds) {
                sb.append(cmd).append(" ");
            }
            String responseMsg = execCmd(cmds);
            Assert.assertTrue(responseMsg.startsWith("{"));
        }
    }
}
