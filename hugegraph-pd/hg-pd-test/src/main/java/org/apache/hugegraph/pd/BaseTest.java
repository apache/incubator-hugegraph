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

package org.apache.hugegraph.pd;

import org.apache.hugegraph.pd.client.PDConfig;

public class BaseTest {

    protected static String pdGrpcAddr = System.getenv("PD_GRPC_ADDR");
    protected static String pdRestAddr = System.getenv("PD_REST_ADDR");
    protected static String user = System.getenv("PD_USER");
    protected static String pwd = System.getenv("PD_PWD");
    protected static String key = System.getenv("PD_AUTH_KEY");
    protected static String value = System.getenv("PD_AUTH_VALUE");

    protected PDConfig getPdConfig() {
        return PDConfig.of(pdGrpcAddr).setAuthority(user, pwd);
    }
}
