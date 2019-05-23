/*
 *
 *  * Copyright 2017 HugeGraph Authors
 *  *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements. See the NOTICE file distributed with this
 *  * work for additional information regarding copyright ownership. The ASF
 *  * licenses this file to You under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *  * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *  * License for the specific language governing permissions and limitations
 *  * under the License.
 *
 */

package com.baidu.hugegraph.unit.core;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.junit.Test;

import com.baidu.hugegraph.testutil.Assert;
import com.baidu.hugegraph.type.HugeType;
import com.baidu.hugegraph.type.define.Directions;

public class DirectionsTest {

    @Test
    public void testString() {
        Assert.assertEquals("out", Directions.OUT.string());
        Assert.assertEquals("in", Directions.IN.string());
        Assert.assertEquals("both", Directions.BOTH.string());
    }

    @Test
    public void testType() {
        Assert.assertEquals(HugeType.EDGE_OUT, Directions.OUT.type());
        Assert.assertEquals(HugeType.EDGE_IN, Directions.IN.type());
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            Directions.BOTH.type();
        });
    }

    @Test
    public void testFromHugeType() {
        Assert.assertEquals(Directions.OUT,
                            Directions.convert(HugeType.EDGE_OUT));
        Assert.assertEquals(Directions.IN,
                            Directions.convert(HugeType.EDGE_IN));
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            Directions.convert(HugeType.EDGE);
        });
    }

    @Test
    public void testOpposite() {
        Assert.assertEquals(Directions.IN, Directions.OUT.opposite());
        Assert.assertEquals(Directions.OUT, Directions.IN.opposite());
        Assert.assertEquals(Directions.BOTH, Directions.BOTH.opposite());
    }

    @Test
    public void testToDirection() {
        Assert.assertEquals(Direction.OUT, Directions.OUT.direction());
        Assert.assertEquals(Direction.IN, Directions.IN.direction());
        Assert.assertEquals(Direction.BOTH, Directions.BOTH.direction());
    }

    @Test
    public void testFromDirection() {
        Assert.assertEquals(Directions.OUT, Directions.convert(Direction.OUT));
        Assert.assertEquals(Directions.IN, Directions.convert(Direction.IN));
        Assert.assertEquals(Directions.BOTH,
                            Directions.convert(Direction.BOTH));
    }
}
