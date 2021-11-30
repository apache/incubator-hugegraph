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

package com.baidu.hugegraph.date;

import java.util.Date;
import java.util.TimeZone;

import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/**
 * The SafeDateFormat actually is a proxy for joda DateTimeFormatter
 */
public class SafeDateFormat {

    private static final int ONE_HOUR_MS = 3600 * 1000;

    private final String pattern;
    private DateTimeFormatter formatter;

    public SafeDateFormat(String pattern) {
        this.pattern = pattern;
        this.formatter = DateTimeFormat.forPattern(pattern);
    }

    public synchronized void setTimeZone(String zoneId) {
        int hoursOffset = TimeZone.getTimeZone(zoneId).getRawOffset() /
                          ONE_HOUR_MS;
        DateTimeZone zone = DateTimeZone.forOffsetHours(hoursOffset);
        this.formatter = this.formatter.withZone(zone);
    }

    public TimeZone getTimeZome() {
        return this.formatter.getZone().toTimeZone();
    }

    public Date parse(String source) {
        return this.formatter.parseDateTime(source).toDate();
    }

    public String format(Date date) {
        return this.formatter.print(date.getTime());
    }

    public Object toPattern() {
        return this.pattern;
    }
}
