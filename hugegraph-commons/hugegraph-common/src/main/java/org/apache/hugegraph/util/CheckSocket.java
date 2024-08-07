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

package org.apache.hugegraph.util;

import static java.lang.System.exit;

import java.net.InetAddress;
import java.net.Socket;

public final class CheckSocket {

    public static void main(String[] args) {
        try {
            // Check if the socket connection can be closed normally
            new Socket(InetAddress.getByName(args[0]), Integer.parseInt(args[1])).close();
            exit(0);
        } catch (Exception e) {
            e.printStackTrace();
            exit(-1);
        }
    }
}
