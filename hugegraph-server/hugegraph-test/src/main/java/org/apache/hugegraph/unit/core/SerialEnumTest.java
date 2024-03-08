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

package org.apache.hugegraph.unit.core;

import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.type.define.Cardinality;
import org.apache.hugegraph.type.define.SerialEnum;
import org.junit.Test;

public class SerialEnumTest {

    @Test
    public void testRegister() {
        SerialEnum.register(Cardinality.class);
        Assert.assertTrue(SerialEnum.TABLE.containsRow(Cardinality.class));
    }

    @Test
    public void testFromCode() {
        Assert.assertEquals(Cardinality.SINGLE,
                            SerialEnum.fromCode(Cardinality.class, (byte) 1));
        Assert.assertEquals(Cardinality.LIST,
                            SerialEnum.fromCode(Cardinality.class, (byte) 2));
        Assert.assertEquals(Cardinality.SET,
                            SerialEnum.fromCode(Cardinality.class, (byte) 3));
    }
}
