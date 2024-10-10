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

package org.apache.hugegraph.ct.base;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class HGTestLogger {

    public static Logger UTIL_LOG = LoggerFactory.getLogger(HGTestLogger.class);
    public static Logger ENV_LOG = LoggerFactory.getLogger(HGTestLogger.class);
    public static Logger CONFIG_LOG = LoggerFactory.getLogger(HGTestLogger.class);
    public static Logger NODE_LOG = LoggerFactory.getLogger(HGTestLogger.class);
}
