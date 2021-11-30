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
import static com.baidu.hugegraph.config.OptionChecker.inValues;
import static com.baidu.hugegraph.config.OptionChecker.nonNegativeInt;
import static com.baidu.hugegraph.config.OptionChecker.positiveInt;
import static com.baidu.hugegraph.config.OptionChecker.rangeDouble;
import static com.baidu.hugegraph.config.OptionChecker.rangeInt;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.IteratorUtils;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.MapConfiguration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.io.FileUtils;
import org.junit.BeforeClass;
import org.junit.Test;

import com.baidu.hugegraph.config.ConfigConvOption;
import com.baidu.hugegraph.config.ConfigException;
import com.baidu.hugegraph.config.ConfigListConvOption;
import com.baidu.hugegraph.config.ConfigListOption;
import com.baidu.hugegraph.config.ConfigOption;
import com.baidu.hugegraph.config.HugeConfig;
import com.baidu.hugegraph.config.OptionHolder;
import com.baidu.hugegraph.config.OptionSpace;
import com.baidu.hugegraph.testutil.Assert;
import com.baidu.hugegraph.testutil.Whitebox;
import com.baidu.hugegraph.unit.BaseUnitTest;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class HugeConfigTest extends BaseUnitTest {

    private static final String PATH =
                         "src/test/java/com/baidu/hugegraph/unit/config/";
    private static final String CONF = PATH + "test.conf";

    @BeforeClass
    public static void init() {
        OptionSpace.register("test", TestOptions.class.getName());
    }

    @Test
    public void testOptionDataType() {
        Assert.assertEquals(String.class, TestOptions.text1.dataType());
        Assert.assertEquals(Integer.class, TestOptions.int1.dataType());
        Assert.assertEquals(Long.class, TestOptions.long1.dataType());
        Assert.assertEquals(Float.class, TestOptions.float1.dataType());
        Assert.assertEquals(Double.class, TestOptions.double1.dataType());
        Assert.assertEquals(Boolean.class, TestOptions.bool.dataType());

        Assert.assertEquals(Class.class, TestOptions.clazz.dataType());

        Assert.assertEquals(List.class, TestOptions.list.dataType());
        Assert.assertEquals(List.class, TestOptions.map.dataType());

        Assert.assertEquals(String.class, TestOptions.weekday.dataType());
        Assert.assertEquals(List.class, TestOptions.weekdays.dataType());
    }

    @Test
    public void testOptionDesc() {
        Assert.assertEquals("description of group1.text1",
                            TestOptions.text1.desc());
        Assert.assertEquals("description of group1.text2 sub",
                            TestSubOptions.text2.desc());
    }

    @Test
    public void testOptionRequired() {
        Assert.assertEquals(false, TestOptions.text1.required());
        Assert.assertEquals(true, TestSubOptions.text2.required());
    }

    @Test
    public void testOptionsToString() {
        Assert.assertEquals("[String]group1.text1=text1-value",
                            TestOptions.text1.toString());
        Assert.assertEquals("[Integer]group1.int1=1",
                            TestOptions.int1.toString());
        Assert.assertEquals("[Long]group1.long1=100",
                            TestOptions.long1.toString());
        Assert.assertEquals("[Float]group1.float1=100.0",
                            TestOptions.float1.toString());
        Assert.assertEquals("[Double]group1.double1=100.0",
                            TestOptions.double1.toString());
        Assert.assertEquals("[Boolean]group1.bool=true",
                            TestOptions.bool.toString());
        Assert.assertEquals("[Class]group1.class=class java.lang.Object",
                            TestOptions.clazz.toString());
        Assert.assertEquals("[List]group1.list=[list-value1, list-value2]",
                            TestOptions.list.toString());
        Assert.assertEquals("[List]group1.map=[key1:value1, key2:value2]",
                            TestOptions.map.toString());

        Assert.assertEquals("[String]group1.text1=text1-value",
                            TestSubOptions.text1.toString());
        Assert.assertEquals("[String]group1.text2=text2-value-override",
                            TestSubOptions.text2.toString());
        Assert.assertEquals("[String]group1.textsub=textsub-value",
                            TestSubOptions.textsub.toString());
    }

    @Test
    public void testOptionWithError() {
        Assert.assertThrows(ConfigException.class, () -> {
            new ConfigOption<>(
                    "group1.text",
                    "description of group1.text",
                    disallowEmpty(),
                    ""
            );
        });

        Assert.assertThrows(ConfigException.class, () -> {
            new ConfigOption<>(
                    "group1.choice",
                    "description of group1.choice",
                    allowValues("CHOICE-1", "CHOICE-2", "CHOICE-3"),
                    "CHOICE-4"
            );
        });

        Assert.assertThrows(ConfigException.class, () -> {
            new ConfigListOption<>(
                    "group1.list",
                    true,
                    "description of group1.list",
                    disallowEmpty(),
                    String.class,
                    ImmutableList.of()
            );
        });

        Assert.assertThrows(ConfigException.class, () -> {
            new ConfigOption<>(
                    "group1.int",
                    "description of group1.int",
                    positiveInt(),
                    0
            );
        });

        Assert.assertThrows(ConfigException.class, () -> {
            new ConfigOption<>(
                    "group1.int",
                    "description of group1.int",
                    nonNegativeInt(),
                    -1
            );
        });

        Assert.assertThrows(ConfigException.class, () -> {
            new ConfigOption<>(
                    "group1.long",
                    "description of group1.long",
                    rangeInt(1L, 100L),
                    0L
            );
        });

        Assert.assertThrows(ConfigException.class, () -> {
            new ConfigOption<>(
                    "group1.long",
                    "description of group1.long",
                    rangeInt(1L, 100L),
                    101L
            );
        });

        Assert.assertThrows(ConfigException.class, () -> {
            new ConfigOption<>(
                    "group1.double",
                    "description of group1.double",
                    rangeDouble(1D, 10D),
                    0D
            );
        });

        Assert.assertThrows(ConfigException.class, () -> {
            new ConfigOption<>(
                    "group1.double",
                    "description of group1.double",
                    rangeDouble(1D, 10D),
                    11D
            );
        });

        Assert.assertThrows(ConfigException.class, () -> {
            new ConfigOption<>(
                    "group1.class",
                    "description of group1.class",
                    input -> input != null && input.equals(Long.class),
                    Integer.class
            );
        });

        Assert.assertThrows(ConfigException.class, () -> {
            new ConfigListOption<>(
                    "group1.list",
                    "description of list with invalid default values",
                    disallowEmpty()
            );
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            new ConfigListOption<>(
                    "group1.list",
                    "description of list with invalid default values",
                    null
            );
        });

        Assert.assertThrows(ConfigException.class, () -> {
            new ConfigListConvOption<String, WeekDay>(
                    "group1.list_conv",
                    "description of list_conv with invalid default values",
                    disallowEmpty(),
                    s -> WeekDay.valueOf(s)
            );
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            new ConfigListConvOption<String, WeekDay>(
                    "group1.list_conv",
                    "description of list_conv with invalid default values",
                    null,
                    s -> WeekDay.valueOf(s)
            );
        });
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

        Assert.assertEquals(Object.class, config.get(TestOptions.clazz));
        Assert.assertThrows(ConfigException.class, () -> {
            config.setProperty(TestOptions.clazz.name(),
                               "com.baidu.hugegraph.HugeGraph");
        }, e -> {
            Assert.assertTrue(e.getCause() instanceof ClassNotFoundException);
        });

        Assert.assertEquals(Arrays.asList("list-value1", "list-value2"),
                            config.get(TestOptions.list));

        Assert.assertEquals(ImmutableMap.of("key1", "value1", "key2", "value2"),
                            config.getMap(TestOptions.map));

        Assert.assertEquals(WeekDay.WEDNESDAY, config.get(TestOptions.weekday));
        Assert.assertEquals(Arrays.asList(WeekDay.SATURDAY, WeekDay.SUNDAY),
                            config.get(TestOptions.weekdays));

        Assert.assertThrows(ConfigException.class, () -> {
            new HugeConfig((Configuration) null);
        });
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

        Assert.assertEquals(String.class, config.get(TestOptions.clazz));

        Assert.assertEquals(Arrays.asList("file-v1", "file-v2", "file-v3"),
                            config.get(TestOptions.list));

        Assert.assertEquals(ImmutableMap.of("key1", "value1", "key3", "value3"),
                            config.getMap(TestOptions.map));

        Assert.assertEquals(WeekDay.SUNDAY, config.get(TestOptions.weekday));
        Assert.assertEquals(Arrays.asList(WeekDay.SATURDAY, WeekDay.FRIDAY),
                            config.get(TestOptions.weekdays));
    }

    @Test
    public void testHugeConfigWithConfiguration() throws Exception {
        HugeConfig config = new HugeConfig(new PropertiesConfiguration(CONF));

        Assert.assertEquals("file-text1-value", config.get(TestOptions.text1));
        Assert.assertEquals("file-text2-value", config.get(TestOptions.text2));
        Assert.assertEquals("CHOICE-3", config.get(TestOptions.text3));
    }

    @Test
    public void testHugeConfigWithOverride() throws Exception {
        Configuration conf = new PropertiesConfiguration();
        Whitebox.setInternalState(conf, "delimiterParsingDisabled", true);

        HugeConfig config = new HugeConfig(conf);

        Assert.assertEquals("text1-value", config.get(TestSubOptions.text1));

        Assert.assertEquals("text2-value-override",
                            config.get(TestSubOptions.text2));
        Assert.assertEquals("textsub-value",
                            config.get(TestSubOptions.textsub));
    }

    @Test
    public void testHugeConfigWithTypeError() {
        OptionSpace.register("test-type-error",
                             TestOptionsWithTypeError.class.getName());

        Assert.assertThrows(ConfigException.class, () -> {
            new HugeConfig(PATH + "test-type-error.conf");
        });
    }

    @Test
    public void testHugeConfigWithCheckError() throws Exception {
        OptionSpace.register("test-check-error",
                             TestOptionsWithCheckError.class.getName());

        Assert.assertThrows(ConfigException.class, () -> {
            new HugeConfig(PATH + "test-check-error.conf");
        });
    }

    @Test
    public void testHugeConfigWithListOptionError() throws Exception {
        OptionSpace.register("test-list-error",
                             TestOptionsWithListError.class.getName());

        Assert.assertThrows(IllegalStateException.class, () -> {
            new HugeConfig(PATH + "test-list-error.conf");
        });
    }

    @Test
    public void testSaveHugeConfig() throws ConfigurationException,
                                            IOException {
        HugeConfig config = new HugeConfig(CONF);
        Assert.assertEquals("file-text1-value", config.get(TestOptions.text1));

        File copiedFile = new File("copied.conf");
        config.save(copiedFile);
        Assert.assertTrue(copiedFile.exists());
        Assert.assertTrue(copiedFile.length() > 0);

        try {
            HugeConfig copiedConfig = new HugeConfig(copiedFile.getPath());
            Assert.assertEquals(IteratorUtils.toList(config.getKeys()),
                                IteratorUtils.toList(copiedConfig.getKeys()));
            Assert.assertEquals(config.get(TestOptions.text1),
                                copiedConfig.get(TestOptions.text1));
        } finally {
            FileUtils.forceDelete(copiedFile);
        }
    }

    @Test
    public void testFromMapConfigurationWithList() {
        Map<String, String> options = new HashMap<>();
        options.put(TestOptions.list.name(), "[a, b]");
        MapConfiguration mapConfiguration = new MapConfiguration(options);
        HugeConfig hugeConfig = new HugeConfig(mapConfiguration);
        List<String> values = hugeConfig.get(TestOptions.list);
        Assert.assertEquals(2, values.size());
        Assert.assertTrue(values.contains("a"));
        Assert.assertTrue(values.contains("b"));
    }

    public static class TestOptions extends OptionHolder {

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

        public static final ConfigOption<Class<?>> clazz =
                new ConfigOption<>(
                        "group1.class",
                        "description of group1.class",
                        disallowEmpty(),
                        Object.class
                );

        public static final ConfigConvOption<String, WeekDay> weekday =
                new ConfigConvOption<>(
                        "group1.weekday",
                        "description of group1.weekday",
                        allowValues("SUNDAY", "MONDAY", "TUESDAY", "WEDNESDAY",
                                    "THURSDAY", "FRIDAY", "SATURDAY"),
                        WeekDay::valueOf,
                        "WEDNESDAY"
                );

        public static final ConfigListConvOption<String, WeekDay> weekdays =
                new ConfigListConvOption<>(
                        "group1.weekdays",
                        "description of group1.weekdays",
                        inValues("SUNDAY", "MONDAY", "TUESDAY", "WEDNESDAY",
                                 "THURSDAY", "FRIDAY", "SATURDAY"),
                        WeekDay::valueOf,
                        "SATURDAY", "SUNDAY"
                );

        public static final ConfigListOption<String> list =
                new ConfigListOption<>(
                        "group1.list",
                        "description of group1.list",
                        disallowEmpty(),
                        "list-value1", "list-value2"
                );

        public static final ConfigListOption<String> map =
                new ConfigListOption<>(
                        "group1.map",
                        "description of group1.map",
                        disallowEmpty(),
                        "key1:value1", "key2:value2"
                );
    }

    public static class TestSubOptions extends TestOptions {

        public static final ConfigOption<String> text2 =
                new ConfigOption<>(
                        "group1.text2",
                        true,
                        "description of group1.text2 sub",
                        disallowEmpty(),
                        String.class,
                        "text2-value-override"
                );

        public static final ConfigOption<String> textsub =
                new ConfigOption<>(
                        "group1.textsub",
                        "description of group1.textsub",
                        disallowEmpty(),
                        "textsub-value"
                );
    }

    public static class TestOptionsWithTypeError extends OptionHolder {

        private static volatile TestOptionsWithTypeError instance;

        public static synchronized TestOptionsWithTypeError instance() {
            if (instance == null) {
                instance = new TestOptionsWithTypeError();
                instance.registerOptions();
            }
            return instance;
        }

        public static final ConfigOption<Integer> intError =
                new ConfigOption<>(
                        "group1.int_type_error",
                        "description of group1.int_type_error",
                        rangeInt(1, 100),
                        1
                );
    }

    public static class TestOptionsWithCheckError extends OptionHolder {

        private static volatile TestOptionsWithCheckError instance;

        public static synchronized TestOptionsWithCheckError instance() {
            if (instance == null) {
                instance = new TestOptionsWithCheckError();
                instance.registerOptions();
            }
            return instance;
        }

        public static final ConfigOption<Integer> intError =
                new ConfigOption<>(
                        "group1.int_check_error",
                        "description of group1.int_check_error",
                        rangeInt(1, 100),
                        1
                );
    }

    public static class TestOptionsWithListError extends OptionHolder {

        private static volatile TestOptionsWithListError instance;

        public static synchronized TestOptionsWithListError instance() {
            if (instance == null) {
                instance = new TestOptionsWithListError();
                instance.registerOptions();
            }
            return instance;
        }

        public static final InvalidConfigListOption<Integer> listError =
                new InvalidConfigListOption<>(
                        "group1.list_for_list_error",
                        "description of group1.list_for_list_error",
                        disallowEmpty(),
                        1
                );

        static class InvalidConfigListOption<T> extends ConfigOption<List<T>> {

            @SuppressWarnings("unchecked")
            public InvalidConfigListOption(String name, String desc,
                                           Predicate<List<T>> pred,
                                           T... values) {
                super(name, false, desc, pred,
                      (Class<List<T>>) Arrays.asList(values).getClass(),
                      Arrays.asList(values));
            }

            @Override
            protected boolean forList() {
                return false;
            }
        }
    }

    public enum WeekDay {

        SUNDAY, MONDAY, TUESDAY, WEDNESDAY, THURSDAY, FRIDAY, SATURDAY;
    }
}
