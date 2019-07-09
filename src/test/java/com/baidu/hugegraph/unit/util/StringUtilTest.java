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

package com.baidu.hugegraph.unit.util;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import com.baidu.hugegraph.testutil.Assert;
import com.baidu.hugegraph.util.StringUtil;
import com.baidu.hugegraph.util.StringUtil.Chars;
import com.google.common.base.Splitter;

public class StringUtilTest {

    @Test
    public void testSplit() {
        Assert.assertArrayEquals(new String[]{"1", "2", "3"},
                                 StringUtil.split("1, 2, 3", ", "));
        Assert.assertArrayEquals(new String[]{"1", "1", "1"},
                                 StringUtil.split("1 1 1", " "));
        Assert.assertArrayEquals(new String[]{"", "", ""},
                                 StringUtil.split("111", "1"));

        Assert.assertEquals(guavaSplit("123", " "),
                            toStringList(StringUtil.split("123", " ")));
        Assert.assertEquals(guavaSplit("1 2 3", " "),
                            toStringList(StringUtil.split("1 2 3", " ")));
        Assert.assertEquals(guavaSplit("1:2:3", ":"),
                            toStringList(StringUtil.split("1:2:3", ":")));
        Assert.assertEquals(guavaSplit("1::2:3", ":"),
                            toStringList(StringUtil.split("1::2:3", ":")));
        Assert.assertEquals(guavaSplit("1::2::3", ":"),
                            toStringList(StringUtil.split("1::2::3", ":")));
        Assert.assertEquals(guavaSplit("1::2::3", "::"),
                            toStringList(StringUtil.split("1::2::3", "::")));
        Assert.assertEquals(guavaSplit("1:|2|:3", "|"),
                            toStringList(StringUtil.split("1:|2|:3", "|")));
        Assert.assertEquals(guavaSplit("1\t2\t3", "\t"),
                            toStringList(StringUtil.split("1\t2\t3", "\t")));

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            StringUtil.split("123", "");
        });
    }

    @Test
    public void testSplitToCharsArray() {
        Assert.assertArrayEquals(Chars.of("1", "2", "3"),
                                 StringUtil.splitToCharsArray("1, 2, 3", ", "));
        Assert.assertArrayEquals(Chars.of("1", "1", "1"),
                                 StringUtil.splitToCharsArray("1 1 1", " "));
        Assert.assertArrayEquals(Chars.of("", "", ""),
                                 StringUtil.splitToCharsArray("111", "1"));

        Assert.assertArrayEquals(new Chars[]{Chars.of("123")},
                                 StringUtil.splitToCharsArray("123", " "));
        Assert.assertArrayEquals(Chars.of("1", "", "2", "3"),
                                 StringUtil.splitToCharsArray("1::2:3", ":"));
        Assert.assertArrayEquals(Chars.of("1", "", "2", "", "3"),
                                 StringUtil.splitToCharsArray("1::2::3", ":"));

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            StringUtil.splitToCharsArray("123", "");
        });
    }

    private static List<String> guavaSplit(String line, String delimiter) {
        return Splitter.on(delimiter).splitToList(line);
    }

    private static List<String> toStringList(String[] stringArray) {
        List<String> results = new ArrayList<>(stringArray.length);
        for (String str : stringArray) {
            results.add(str);
        }
        return results;
    }
}
