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

package org.apache.hugegraph.MultiClusterTest;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hugegraph.ct.env.BaseEnv;
import org.apache.hugegraph.ct.env.MultiNodeEnv;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class BaseMultiClusterTest {

    protected static BaseEnv env;

    protected static Process p;

    @BeforeClass
    public static void initEnv() throws InterruptedException {
        env = new MultiNodeEnv();
        env.startCluster();
    }

    @AfterClass
    public static void clearEnv() throws InterruptedException {
        env.clearCluster();
        Thread.sleep(2000);
    }

    protected String execCurl(String[] cmds) throws IOException {
        ProcessBuilder process = new ProcessBuilder(cmds);
        p = process.start();
        BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
        StringBuilder builder = new StringBuilder();
        String line;
        while ((line = reader.readLine()) != null) {
            builder.append(line);
            builder.append(System.getProperty("line.separator"));
        }
        p.destroy();
        return builder.toString();
    }
}
