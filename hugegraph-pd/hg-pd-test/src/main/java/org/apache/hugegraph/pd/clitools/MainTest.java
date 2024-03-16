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

import java.util.Arrays;
import java.util.List;

import org.apache.hugegraph.pd.common.PDException;
import org.junit.Ignore;
import org.junit.Test;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MainTest extends BaseCliToolsTest {


    public static boolean test2sup(List<Integer> arrays, int tail, int res) {
        System.out.printf("%d %d%n", tail, res);
        if (tail == 0) {
            System.out.printf("a = %d %d%n", tail, res);
            return false;
        } else if (tail == 1) {
            System.out.printf("b = %d %d%n", arrays.get(0), res);
            return (arrays.get(0) == res);
        } else if (tail == 2) {
            System.out.printf("c = %d %d %d%n", arrays.get(0), arrays.get(1), res);
            return (arrays.get(0) + arrays.get(1) == Math.abs(res)) ||
                   (Math.abs(arrays.get(0) - arrays.get(1)) == Math.abs(res));
        } else {
            return test2sup(arrays, tail - 1, res + arrays.get(tail - 1)) ||
                   test2sup(arrays, tail - 1, res - arrays.get(tail - 1));
        }
    }

    @Ignore
    @Test
    public void getConfig() throws PDException {
        Main.main(new String[]{"127.0.0.1:8686", "config", "enableBatchLoad"});
    }

    // @Test
    public void setBatchTrue() throws PDException {
        Main.main(new String[]{"127.0.0.1:8686", "config", "enableBatchLoad= true "});
    }

    //    @Test
    public void setBatchFalse() throws PDException {
        Main.main(new String[]{"127.0.0.1:8686", "config", "enableBatchLoad=false"});
    }

    @Ignore
    @Test
    public void getConfig2() throws PDException {
        Main.main(new String[]{"127.0.0.1:8686", "config", "shardCount"});
    }

    //    @Test
    public void setShardCount1() throws PDException {
        Main.main(new String[]{"127.0.0.1:8686", "config", "shardCount=1"});
    }

    //    @Test
    public void setShardCount3() throws PDException {
        Main.main(new String[]{"127.0.0.1:8686", "config", "shardCount=3"});
    }

    @Test
    public void test2() {
        Integer[] a = new Integer[]{1, 0, 3, 2};
        List<Integer> aa = Arrays.asList(a);
        System.out.printf(test2sup(aa, aa.size(), 0) ? "TRUE" : "FALSE");
    }


}
