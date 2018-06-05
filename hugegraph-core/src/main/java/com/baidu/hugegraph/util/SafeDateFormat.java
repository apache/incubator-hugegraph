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

package com.baidu.hugegraph.util;

import java.text.DateFormat;
import java.text.FieldPosition;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * SafeDateFormat is a thread safe DateFormat
 * NOTE: DateFormat is not thread safe, need synchronized manually.
 * http://blog.jrwang.me/2016/java-simpledateformat-multithread-threadlocal
 */
public class SafeDateFormat extends DateFormat {

    private static final long serialVersionUID = -4838048315029312489L;

    private final ThreadLocal<SimpleDateFormat> formatter;

    public SafeDateFormat(String template) {
        this.formatter = ThreadLocal.withInitial(() -> {
            return new SimpleDateFormat(template);
        });
        this.setCalendar(this.formatter.get().getCalendar());
        this.setNumberFormat(this.formatter.get().getNumberFormat());
    }

    @Override
    public StringBuffer format(Date date, StringBuffer toAppendTo,
                               FieldPosition fieldPosition) {
        return this.formatter.get().format(date, toAppendTo, fieldPosition);
    }

    @Override
    public Date parse(String source, ParsePosition pos) {
        return this.formatter.get().parse(source, pos);
    }

    @Override
    public Object clone() {
        // No need to clone due to itself is thread safe
        return this;
    }

    public Object toPattern() {
        return this.formatter.get().toPattern();
    }
}
