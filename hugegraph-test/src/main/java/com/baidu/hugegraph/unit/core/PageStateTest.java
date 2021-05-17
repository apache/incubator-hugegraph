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

package com.baidu.hugegraph.unit.core;

import java.util.Arrays;

import org.junit.Test;

import com.baidu.hugegraph.backend.page.PageState;
import com.baidu.hugegraph.testutil.Assert;

public class PageStateTest {

    @Test
    public void testDecodePageWithSpecialBase64Chars() {
        // Assert decode '+' and '/' and '=' and space successfully
        final String pageWith3Base64Chars = "AAAAADsyABwAEAqI546LS6WW57unBgA" +
                                            "EAAAAAPB////+8H////4alhxAZS8va6" +
                                            "opcAKpklipAAQAAAAAAAAAAQ==";

        final String pageWithSpace = "AAAAADsyABwAEAqI546LS6WW57unBgAEAAAAAP" +
                                     "B//// 8H////4alhxAZS8va6opcAKpklipAAQA" +
                                     "AAAAAAAAAQ==";
        Assert.assertNotNull(PageState.fromString(pageWith3Base64Chars));

        byte[] decodePlus = PageState.fromString(pageWith3Base64Chars)
                                     .position();
        byte[] decodeSpace = PageState.fromString(pageWithSpace).position();

        Assert.assertTrue(Arrays.equals(decodePlus, decodeSpace));
    }
}
