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

package org.apache.hugegraph.pd.clitools;

import org.apache.hugegraph.pd.client.PDClient;
import org.apache.hugegraph.pd.client.PDConfig;
import org.apache.hugegraph.pd.common.PDException;
import org.apache.hugegraph.pd.grpc.Metapb;

public class Main {

    public static void main(String[] args) throws PDException {
        if (args.length < 3) {
            String error = " usage: pd-address config key[=value] \n key list: " +
                           "\n\tenableBatchLoad";
            System.out.println(error);
            System.exit(0);
        }
        String pd = args[0];
        String cmd = args[1];
        String param = args[2];
        System.out.println(pd + " " + cmd + " " + param);
        System.out.println("Result: \n");
        switch (cmd) {
            case "config":
                doConfig(pd, param);
            case "change_raft":
                doChangeRaft(pd, param);
        }
    }

    private static void doChangeRaft(String pd, String param) throws PDException {
        PDClient pdClient = PDClient.create(PDConfig.of(pd));
        pdClient.updatePdRaft(param);
    }

    public static void doConfig(String pd, String param) throws PDException {
        PDClient pdClient = PDClient.create(PDConfig.of(pd));
        String[] pair = param.split("=");
        String key = pair[0].trim();
        Object value = null;
        if (pair.length > 1) {
            value = pair[1].trim();
        }
        if (value == null) {
            Metapb.PDConfig pdConfig = pdClient.getPDConfig();
            switch (key) {
                case "enableBatchLoad":
                    //    value = pdConfig.getEnableBatchLoad();
                    break;
                case "shardCount":
                    value = pdConfig.getShardCount();
                    break;
            }

            System.out.println("Get config " + key + "=" + value);
        } else {
            Metapb.PDConfig.Builder builder = Metapb.PDConfig.newBuilder();
            switch (key) {
                case "enableBatchLoad":
                    //   builder.setEnableBatchLoad(Boolean.valueOf((String)value));
                case "shardCount":
                    builder.setShardCount(Integer.valueOf((String) value));
            }
            pdClient.setPDConfig(builder.build());
            System.out.println("Set config " + key + "=" + value);
        }
    }

}
