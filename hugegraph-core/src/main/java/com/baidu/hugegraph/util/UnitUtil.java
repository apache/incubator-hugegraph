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

import java.math.BigDecimal;
import java.time.Duration;

public final class UnitUtil {

    public static double bytesToMB(long bytes) {
        return doubleWith2Scale(bytes / (double) Bytes.MB);
    }

    public static double bytesToGB(long bytes) {
        return doubleWith2Scale(bytes / (double) Bytes.GB);
    }

    public static String bytesToReadableString(long bytes) {
        // NOTE: FileUtils.byteCountToDisplaySize() lost decimal precision
        final String[] units = {"B", "KB", "MB", "GB", "TB", "PB", "EB"};
        if (bytes <= 0) {
            return "0 B";
        }
        int i = (int) (Math.log(bytes) / Math.log(1024));
        E.checkArgument(i < units.length,
                        "The bytes parameter is out of %s unit: %s",
                        units[units.length - 1], bytes);
        double value = bytes / Math.pow(1024, i);
        return doubleWith2Scale(value) + " " + units[i];
    }

    public static double doubleWith2Scale(double value) {
        BigDecimal decimal = new BigDecimal(value);
        return decimal.setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue();
    }

    public static long bytesFromReadableString(String valueWithUnit) {
        int spacePos = valueWithUnit.indexOf(" ");
        E.checkArgument(spacePos >= 0,
                        "Invalid readable bytes '%s', " +
                        "expect format like '10 MB'", valueWithUnit);
        String unit = valueWithUnit.substring(spacePos + 1);

        long factor = 1L;
        switch (unit) {
            case "B":
            case "Byte":
            case "Bytes":
                factor = 1L;
                break;
            case "KB":
            case "KiB":
                factor = Bytes.KB;
                break;
            case "MB":
            case "MiB":
                factor = Bytes.MB;
                break;
            case "GB":
            case "GiB":
                factor = Bytes.GB;
                break;
            case "TB":
            case "TiB":
                factor = Bytes.GB * Bytes.KB;
                break;
            case "PB":
            case "PiB":
                factor = Bytes.GB * Bytes.MB;
                break;
            case "EB":
            case "EiB":
                factor = Bytes.GB * Bytes.GB;
                break;
            default:
                throw new IllegalArgumentException("Unrecognized unit " + unit);
        }

        double value = Double.parseDouble(valueWithUnit.substring(0, spacePos));
        return (long) (value * factor);
    }

    public static String timestampToReadableString(long time) {
        Duration duration = Duration.ofMillis(time);
        long days = duration.toDays();
        if (days > 0) {
            return String.format("%dd%dh%dm%ds",
                                 days,
                                 duration.toHours() % 24,
                                 duration.toMinutes() % 60,
                                 duration.getSeconds() % 60);
        } else {
            return String.format("%dh%dm%ds",
                                 duration.toHours(),
                                 duration.toMinutes() % 60,
                                 duration.getSeconds() % 60);
        }

    }

    public static long timestampFromReadableString(String valueWithUnit) {
        return 0;
    }
}
