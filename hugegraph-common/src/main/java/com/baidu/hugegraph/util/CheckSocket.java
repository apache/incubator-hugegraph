// Copyright 2017 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.baidu.hugegraph.util;

import java.net.InetAddress;
import java.net.Socket;

public class CheckSocket {

    public static final int E_USAGE  = 1;
    public static final int E_FAILED = 2;
    public static final String MSG_USAGE =
        "Usage: " + CheckSocket.class.getSimpleName() + " hostname port";

    public static void main(String args[]) {
        if (2 != args.length) {
            System.err.println(MSG_USAGE);
            System.exit(E_USAGE);
        }
        try {
            Socket s = new Socket(
                InetAddress.getByName(args[0]),
                Integer.valueOf(args[1]).intValue());
            s.close();
            System.exit(0);
        } catch (Throwable t) {
            System.err.println(t.toString());
            System.exit(E_FAILED);
        }
    }
}
