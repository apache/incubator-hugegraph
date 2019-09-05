/*
 * Copyright 2017 HugeGraph Authors
 *
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

package com.baidu.hugegraph.testutil;

import org.junit.Test;

public class WhiteboxTest {

    @Test
    public void testGetStaticInternalState() {
        Assert.assertEquals(1, Whitebox.getInternalState(Test1.class,
                                                         "staticValue"));
        Test1 test1 = newTest();
        Assert.assertEquals(1, Whitebox.getInternalState(test1, "staticValue"));
        Assert.assertEquals(2, Whitebox.getInternalState(test1,
                                                         "test2.staticValue"));
    }

    @Test
    public void testSetStaticInternalState() {
        try {
            Whitebox.setInternalState(Test1.class, "staticValue", 11);
            Assert.assertEquals(11, Test1.staticValue);

            Test1 test1 = newTest();
            Whitebox.setInternalState(test1, "staticValue", 111);
            Assert.assertEquals(111, Test1.staticValue);

            Whitebox.setInternalState(test1, "test2.staticValue", 22);
            Assert.assertEquals(22, Test2.staticValue);
        } finally {
            Whitebox.setInternalState(Test1.class, "staticValue", 1);
            Whitebox.setInternalState(Test2.class, "staticValue", 2);
        }
    }

    @Test
    public void testGetInternalState() {
        Test1 test1 = newTest();
        Assert.assertEquals(1, Whitebox.getInternalState(test1, "ivalue"));
        Assert.assertEquals(2f, Whitebox.getInternalState(test1,
                                                          "test2.fvalue"));
        Assert.assertEquals("3",  Whitebox.getInternalState(test1,
                                                            "test2.test3.str"));

        Assert.assertThrows(RuntimeException.class, () -> {
            Whitebox.getInternalState(test1, "ivalue2");
        });

        Assert.assertThrows(RuntimeException.class, () -> {
            Whitebox.getInternalState(test1, "test2.fvalue2");
        });
    }

    @Test
    public void testSetInternalState() {
        Test1 test1 = newTest();

        Whitebox.setInternalState(test1, "ivalue", 11);
        Assert.assertEquals(11, Whitebox.getInternalState(test1, "ivalue"));

        Whitebox.setInternalState(test1, "test2.fvalue", 22f);
        Assert.assertEquals(22f, Whitebox.getInternalState(test1,
                                                          "test2.fvalue"));

        Whitebox.setInternalState(test1, "test2.test3.str", "33");
        Assert.assertEquals("33",  Whitebox.getInternalState(test1,
                                                            "test2.test3.str"));

        Assert.assertThrows(RuntimeException.class, () -> {
            Whitebox.setInternalState(test1, "ivalue2", 11);
        });

        Assert.assertThrows(RuntimeException.class, () -> {
            Whitebox.setInternalState(test1, "test2.fvalue2", 22f);
        });

        Assert.assertThrows(RuntimeException.class, () -> {
            Whitebox.setInternalState(test1, "test2.fvalue", 22d);
        });
    }

    @Test
    public void testInvokeStatic() {
        Assert.assertEquals(1, Whitebox.invokeStatic(Test1.class, "svalue"));
        Assert.assertEquals(2, Whitebox.invokeStatic(Test1.class, "svalue", 2));
        Assert.assertEquals(2, Whitebox.invokeStatic(Test1.class, "svalue",
                                                     new Integer(2)));
        Assert.assertEquals(2d, Whitebox.invokeStatic(Test1.class,
                                                      new Class[]{Object.class},
                                                      "svalue", 2d));

        Assert.assertThrows(RuntimeException.class, () -> {
            Whitebox.invokeStatic(Test1.class, "svalue2");
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            Whitebox.invokeStatic(Test1.class, "throwfunc1");
        });
        Assert.assertThrows(RuntimeException.class, () -> {
            Whitebox.invokeStatic(Test1.class, "throwfunc2");
        });
    }

    @Test
    public void testInvoke() {
        Test1 test1 = newTest();
        Assert.assertEquals(1, Whitebox.invoke(test1.getClass(),
                                               "value", test1));
        Assert.assertEquals(3, Whitebox.invoke(test1.getClass(),
                                               "addValue", test1, 2));
        Assert.assertEquals(2f, Whitebox.invoke(test1, "test2", "value"));
        Assert.assertEquals(2, Whitebox.invoke(test1, "test2",
                                               new Class[]{Object.class},
                                               "value", 2));
        Assert.assertEquals(3, Whitebox.invoke(test1, "test4", "addValue", 2));
        Assert.assertEquals(4, Whitebox.invoke(test1, "test4", "value"));

        Assert.assertThrows(RuntimeException.class, () -> {
            Whitebox.invoke(test1.getClass(), "value2", test1);
        });
        Assert.assertThrows(RuntimeException.class, () -> {
            Whitebox.invoke(test1, "test22", "value");
        });
        Assert.assertThrows(RuntimeException.class, () -> {
            Whitebox.invoke(test1, "test2", "value", 2);
        });
        Assert.assertThrows(RuntimeException.class, () -> {
            Whitebox.invoke(test1.getClass(), "addValue", test1, 2.0);
        });
    }

    private static Test1 newTest() {
        Test1 test1 = new Test1();
        test1.test2 = new Test2();
        test1.test2.test3 = new Test3();
        test1.test4 = new TestSubClass();
        return test1;
    }

    @SuppressWarnings("unused")
    private static class Test1 {

        private static int staticValue = 1;
        private int ivalue = 1;
        private Test2 test2;
        private TestSubClass test4;

        private int value() {
            return this.ivalue;
        }

        private int addValue(int i) {
            return this.ivalue + i;
        }

        private static int svalue() {
            return 1;
        }

        private static int svalue(int i) {
            return i;
        }

        private static <T> T svalue(T o) {
            return o;
        }

        private static int throwfunc1() {
            throw new IllegalArgumentException("fake runtime exception");
        }

        private static int throwfunc2() throws Exception {
            throw new Exception("fake exception");
        }
    }

    @SuppressWarnings("unused")
    private static class Test2 {

        private static int staticValue = 2;
        private final float fvalue = 2;
        private Test3 test3;

        private float value() {
            return this.fvalue;
        }

        private <T> T value(T o) {
            return o;
        }
    }

    @SuppressWarnings("unused")
    private static class Test3 {

        private String str = "3";

        private String value() {
            return this.str;
        }
    }

    @SuppressWarnings("unused")
    private static class TestSubClass extends Test1 {

        private int value() {
            return 4;
        }
    }
}
