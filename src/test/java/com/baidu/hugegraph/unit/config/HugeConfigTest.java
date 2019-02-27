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

import static com.baidu.hugegraph.config.OptionChecker.allowValues;
import static com.baidu.hugegraph.config.OptionChecker.disallowEmpty;
import static com.baidu.hugegraph.config.OptionChecker.nonNegativeInt;
import static com.baidu.hugegraph.config.OptionChecker.positiveInt;
import static com.baidu.hugegraph.config.OptionChecker.rangeDouble;
import static com.baidu.hugegraph.config.OptionChecker.rangeInt;

import java.util.Arrays;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.junit.BeforeClass;
import org.junit.Test;

import com.baidu.hugegraph.config.ConfigListOption;
import com.baidu.hugegraph.config.ConfigOption;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.config.OptionHolder;
import com.baidu.hugegraph.config.OptionSpace;
import com.baidu.hugegraph.testutil.Assert;
import com.baidu.hugegraph.testutil.Whitebox;
import com.baidu.hugegraph.unit.BaseUnitTest;
import com.google.common.collect.ImmutableMap;

public class HugeConfigTest extends BaseUnitTest {

    private static final String CONF =
            "src/test/java/com/baidu/hugegraph/unit/config/test.conf";

    @BeforeClass
    public static void init() {
        OptionSpace.register("test", TestOptions.class.getName());
    }

    @Test
    public void testHugeConfig() throws Exception {
        Configuration conf = new PropertiesConfiguration();
        Whitebox.setInternalState(conf, "delimiterParsingDisabled", true);

        HugeConfig config = new HugeConfig(conf);

        Assert.assertEquals("text1-value", config.get(TestOptions.text1));
        Assert.assertEquals("text2-value", config.get(TestOptions.text2));
        Assert.assertEquals("CHOICE-1", config.get(TestOptions.text3));

        Assert.assertEquals(1, (int) config.get(TestOptions.int1));
        Assert.assertEquals(10, (int) config.get(TestOptions.int2));
        Assert.assertEquals(10, (int) config.get(TestOptions.int3));

        Assert.assertEquals(100L, (long) config.get(TestOptions.long1));

        Assert.assertEquals(100.0f, config.get(TestOptions.float1), 0f);
        Assert.assertEquals(100.0f, config.get(TestOptions.double1), 0d);

        Assert.assertEquals(true, config.get(TestOptions.bool));

        Assert.assertEquals(Arrays.asList("list-value1", "list-value2"),
                            config.get(TestOptions.list));

        Assert.assertEquals(ImmutableMap.of("key1", "value1", "key2", "value2"),
                            config.getMap(TestOptions.map));
    }

    @Test
    public void testHugeConfigWithFile() throws Exception {
        HugeConfig config = new HugeConfig(CONF);

        Assert.assertEquals("file-text1-value", config.get(TestOptions.text1));
        Assert.assertEquals("file-text2-value", config.get(TestOptions.text2));
        Assert.assertEquals("CHOICE-3", config.get(TestOptions.text3));

        Assert.assertEquals(2, (int) config.get(TestOptions.int1));
        Assert.assertEquals(0, (int) config.get(TestOptions.int2));
        Assert.assertEquals(1, (int) config.get(TestOptions.int3));

        Assert.assertEquals(99L, (long) config.get(TestOptions.long1));

        Assert.assertEquals(66.0f, config.get(TestOptions.float1), 0f);
        Assert.assertEquals(66.0f, config.get(TestOptions.double1), 0d);

        Assert.assertEquals(false, config.get(TestOptions.bool));

        Assert.assertEquals(Arrays.asList("file-v1", "file-v2", "file-v3"),
                            config.get(TestOptions.list));

        Assert.assertEquals(ImmutableMap.of("key1", "value1", "key3", "value3"),
                            config.getMap(TestOptions.map));
    }

    @Test
    public void testHugeConfigWithConfiguration() throws Exception {
        HugeConfig config = new HugeConfig(new PropertiesConfiguration(CONF));

        Assert.assertEquals("file-text1-value", config.get(TestOptions.text1));
        Assert.assertEquals("file-text2-value", config.get(TestOptions.text2));
        Assert.assertEquals("CHOICE-3", config.get(TestOptions.text3));
    }

    public static final class TestOptions extends OptionHolder {

        private static volatile TestOptions instance;

        public static synchronized TestOptions instance() {
            if (instance == null) {
                instance = new TestOptions();
                instance.registerOptions();
            }
            return instance;
        }

        public static final ConfigOption<String> text1 =
                new ConfigOption<>(
                        "group1.text1",
                        "description of group1.text1",
                        disallowEmpty(),
                        "text1-value"
                );

        public static final ConfigOption<String> text2 =
                new ConfigOption<>(
                        "group1.text2",
                        "description of group1.text2",
                        disallowEmpty(),
                        "text2-value"
                );

        public static final ConfigOption<String> text3 =
                new ConfigOption<>(
                        "group1.text3",
                        "description of group1.text3",
                        allowValues("CHOICE-1", "CHOICE-2", "CHOICE-3"),
                        "CHOICE-1"
                );

        public static final ConfigOption<Integer> int1 =
                new ConfigOption<>(
                        "group1.int1",
                        "description of group1.int1",
                        rangeInt(1, 100),
                        1
                );

        public static final ConfigOption<Integer> int2 =
                new ConfigOption<>(
                        "group1.int2",
                        "description of group1.int2",
                        nonNegativeInt(),
                        10
                );

        public static final ConfigOption<Integer> int3 =
                new ConfigOption<>(
                        "group1.int3",
                        "description of group1.int3",
                        positiveInt(),
                        10
                );

        public static final ConfigOption<Long> long1 =
                new ConfigOption<>(
                        "group1.long1",
                        "description of group1.long1",
                        rangeInt(1L, 100L),
                        100L
                );

        public static final ConfigOption<Float> float1 =
                new ConfigOption<>(
                        "group1.float1",
                        "description of group1.float1",
                        rangeDouble(1.0f, 100.0f),
                        100.0f
                );

        public static final ConfigOption<Double> double1 =
                new ConfigOption<>(
                        "group1.double1",
                        "description of group1.double1",
                        rangeDouble(1.0, 100.0),
                        100.0
                );

        public static final ConfigOption<Boolean> bool =
                new ConfigOption<>(
                        "group1.bool",
                        "description of group1.bool",
                        disallowEmpty(),
                        true
                );

        public static final ConfigListOption<String> list =
                new ConfigListOption<>(
                        "group1.list",
                        false,
                        "description of group1.list",
                        disallowEmpty(),
                        String.class,
                        "list-value1", "list-value2"
                );

        public static final ConfigListOption<String> map =
                new ConfigListOption<>(
                        "group1.map",
                        false,
                        "description of group1.map",
                        disallowEmpty(),
                        String.class,
                        "key1:value1", "key2:value2"
                );
    }
}
