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

package com.baidu.hugegraph.unit.config;

import static com.baidu.hugegraph.config.OptionChecker.disallowEmpty;

import org.junit.Test;

import com.baidu.hugegraph.config.ConfigException;
import com.baidu.hugegraph.config.ConfigOption;
import com.baidu.hugegraph.config.OptionHolder;
import com.baidu.hugegraph.config.OptionSpace;
import com.baidu.hugegraph.testutil.Assert;
import com.baidu.hugegraph.unit.BaseUnitTest;
import com.google.common.base.Predicate;

public class OptionSpaceTest extends BaseUnitTest {

    @Test
    public void tesRegister() {
        int oldSize = OptionSpace.keys().size();

        OptionSpace.register("testgroup1", OptionHolder1.class.getName());
        Assert.assertEquals(oldSize + 2, OptionSpace.keys().size());
        Assert.assertTrue(OptionSpace.containKey("testgroup1.text1"));
        Assert.assertTrue(OptionSpace.containKey("testgroup1.text2"));

        OptionSpace.register("testgroup1", new OptionHolder1());
        Assert.assertEquals(oldSize + 2, OptionSpace.keys().size());
        Assert.assertTrue(OptionSpace.containKey("testgroup1.text1"));
        Assert.assertTrue(OptionSpace.containKey("testgroup1.text2"));

        OptionSpace.register("testgroup2", OptionHolder2.class.getName());
        Assert.assertEquals(oldSize + 4, OptionSpace.keys().size());

        Assert.assertTrue(OptionSpace.containKey("testgroup1.text1"));
        Assert.assertTrue(OptionSpace.containKey("testgroup1.text2"));
        Assert.assertTrue(OptionSpace.containKey("testgroup2.text1"));
        Assert.assertTrue(OptionSpace.containKey("testgroup2.text2"));

        Assert.assertEquals("text1 value",
                            OptionSpace.get("testgroup1.text1").defaultValue());
        Assert.assertEquals("text2 value",
                            OptionSpace.get("testgroup1.text2").defaultValue());
        Assert.assertEquals("text1 value",
                            OptionSpace.get("testgroup2.text1").defaultValue());
        Assert.assertEquals("text2 value",
                            OptionSpace.get("testgroup2.text2").defaultValue());
    }

    @Test
    public void testRegisterWithError() {
        Assert.assertThrows(ConfigException.class, () -> {
            OptionSpace.register("test-error", "fake");
        });

        Assert.assertThrows(ConfigException.class, () -> {
            OptionSpace.register("test-error", Exception.class.getName());
        });

        Assert.assertThrows(ConfigException.class, () -> {
            OptionSpace.register("test-error",
                                 OptionHolderWithoutInstance.class.getName());
        });

        Assert.assertThrows(ConfigException.class, () -> {
            OptionSpace.register("test-error", OptionHolderWithNonStaticInstance
                                               .class.getName());
        });

        Assert.assertThrows(ConfigException.class, () -> {
            OptionSpace.register("test-error",
                                 OptionHolderWithInstanceNull.class.getName());
        });

        Assert.assertThrows(ConfigException.class, () -> {
            OptionSpace.register("test-error",
                                 OptionHolderWithInstanceThrow.class.getName());
        });

        Assert.assertThrows(ConfigException.class, () -> {
            OptionSpace.register("test-error",
                                 OptionHolderWithInvalidOption.class.getName());
        });
    }

    public static class OptionHolderWithoutInstance extends OptionHolder {
        // no instance()
    }

    public static class OptionHolderWithNonStaticInstance extends OptionHolder {

        // not static instance()
        public OptionHolderWithNonStaticInstance instance() {
            return new OptionHolderWithNonStaticInstance();
        }
    }

    public static class OptionHolderWithInstanceNull extends OptionHolder {

        public static OptionHolderWithInstanceNull instance() {
            return null;
        }
    }

    public static class OptionHolderWithInstanceThrow extends OptionHolder {

        public static OptionHolderWithInstanceNull instance() {
            throw new RuntimeException("test error");
        }
    }

    public static class OptionHolderWithInvalidOption extends OptionHolder {

        public static OptionHolderWithInvalidOption instance() {
            return new OptionHolderWithInvalidOption();
        }

        private OptionHolderWithInvalidOption() {
            this.registerOptions();
        }

        public static final String fake = "fake";

        public static final ConfigOption<String> invalid =
                new InvalidOption<>(
                        "group1.text1",
                        "description of group1.text1",
                        disallowEmpty(),
                        "value"
                );

        public static class InvalidOption<T> extends ConfigOption<T> {

            public InvalidOption(String name, String desc,
                                 Predicate<T> pred, T value) {
                super(name, desc, pred, value);
            }

            @Override
            public String name() {
                throw new RuntimeException("fake");
            }
        }
    }

    public static class OptionHolder1 extends OptionHolder {

        public static OptionHolder1 instance() {
            return new OptionHolder1();
        }

        OptionHolder1() {
            this.registerOptions();
        }

        public static final ConfigOption<String> text1 =
                new ConfigOption<>(
                        "testgroup1.text1",
                        "description of testgroup1.text1",
                        disallowEmpty(),
                        "text1 value"
                );

        public static final ConfigOption<String> text2 =
                new ConfigOption<>(
                        "testgroup1.text2",
                        "description of testgroup1.text2",
                        disallowEmpty(),
                        "text2 value"
                );
    }

    public static class OptionHolder2 extends OptionHolder {

        public static OptionHolder2 instance() {
            return new OptionHolder2();
        }

        OptionHolder2() {
            this.registerOptions();
        }

        public static final ConfigOption<String> text1 =
                new ConfigOption<>(
                        "testgroup2.text1",
                        "description of testgroup2.text1",
                        disallowEmpty(),
                        "text1 value"
                );

        public static final ConfigOption<String> text2 =
                new ConfigOption<>(
                        "testgroup2.text2",
                        "description of testgroup2.text2",
                        disallowEmpty(),
                        "text2 value"
                );
    }
}
