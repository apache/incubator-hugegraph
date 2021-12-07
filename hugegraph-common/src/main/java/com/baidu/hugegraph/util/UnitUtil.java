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

    public static double doubleWith2Scale(double value) {
        BigDecimal decimal = new BigDecimal(value);
        return decimal.setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue();
    }

    public static String bytesToReadableString(long bytes) {
        // NOTE: FileUtils.byteCountToDisplaySize() lost decimal precision
        final String[] units = {"B", "KB", "MB", "GB", "TB", "PB", "EB"};
        if (bytes <= 0L) {
            return "0 B";
        }
        int i = (int) (Math.log(bytes) / Math.log(1024));
        E.checkArgument(i < units.length,
                        "The bytes parameter is out of %s unit: %s",
                        units[units.length - 1], bytes);
        double value = bytes / Math.pow(1024, i);
        if (value % 1L == 0L) {
            return ((long) value) + " " + units[i];
        } else {
            return doubleWith2Scale(value) + " " + units[i];
        }
    }

    public static long bytesFromReadableString(String valueWithUnit) {
        int spacePos = valueWithUnit.indexOf(" ");
        E.checkArgument(spacePos >= 0,
                        "Invalid readable bytes '%s', " +
                        "expect format like '10 MB'", valueWithUnit);
        String unit = valueWithUnit.substring(spacePos + 1);

        long factor = 0L;
        switch (unit.trim().toUpperCase()) {
            case "B":
            case "BYTE":
            case "BYTES":
                factor = 1L;
                break;
            case "KB":
            case "KIB":
                factor = Bytes.KB;
                break;
            case "MB":
            case "MIB":
                factor = Bytes.MB;
                break;
            case "GB":
            case "GIB":
                factor = Bytes.GB;
                break;
            case "TB":
            case "TIB":
                factor = Bytes.TB;
                break;
            case "PB":
            case "PIB":
                factor = Bytes.PB;
                break;
            case "EB":
            case "EIB":
                factor = Bytes.EB;
                break;
            default:
                throw new IllegalArgumentException("Unrecognized unit " + unit);
        }

        double value;
        try {
            value = Double.parseDouble(valueWithUnit.substring(0, spacePos));
        } catch (Exception e) {
            throw new IllegalArgumentException(String.format(
                      "Invalid parameter(not number): '%s'", valueWithUnit), e);
        }
        value = value * factor;
        E.checkArgument(value <= Long.MAX_VALUE,
                        "The value %s from parameter '%s' is out of range",
                        value, valueWithUnit);
        return (long) value;
    }

    public static String timestampToReadableString(long time) {
        Duration duration = Duration.ofMillis(time);
        long days = duration.toDays();
        long hours = duration.toHours();
        long minutes = duration.toMinutes();
        long seconds = duration.getSeconds();

        if (days > 0) {
            return String.format("%dd%dh%dm%ds",
                                 days,
                                 hours % 24,
                                 minutes % 60,
                                 seconds % 60);
        } else if (hours > 0) {
            return String.format("%dh%dm%ds",
                                 hours,
                                 minutes % 60,
                                 seconds % 60);
        } else if (minutes > 0) {
            return String.format("%dm%ds",
                                 minutes,
                                 seconds % 60);
        } else if (seconds > 0) {
            long ms = duration.toMillis() % 1000L;
            if (ms > 0L) {
                return String.format("%ds%dms", seconds, ms);
            } else {
                return String.format("%ds", seconds);
            }
        } else {
            return String.format("%dms", duration.toMillis());
        }
    }

    public static long timestampFromReadableString(String valueWithUnit) {
        long ms = 0L;
        // Adapt format 'nDnHnMnS' to 'PnYnMnDTnHnMnS'
        String formatDuration = valueWithUnit.toUpperCase();
        if (formatDuration.indexOf('D') >= 0) {
            // Contains days
            assert !formatDuration.contains("MS");
            formatDuration = "P" + formatDuration.replace("D", "DT");
        } else {
            // Not exists days
            int msPos = formatDuration.indexOf("MS");
            // If contains ms, rmove the ms part
            if (msPos >= 0) {
                int sPos = formatDuration.indexOf("S");
                if (0 <= sPos && sPos < msPos) {
                    // If contains second part
                    sPos += 1;
                    ms = Long.parseLong(formatDuration.substring(sPos, msPos));
                    ms %= 1000L;
                    formatDuration = formatDuration.substring(0, sPos);
                } else {
                    // Not contains second part, only exists ms
                    ms = Long.parseLong(formatDuration.substring(0, msPos));
                    return ms;
                }
            } else {
                assert formatDuration.endsWith("S");
            }
            formatDuration = "PT" + formatDuration;
        }

        Duration duration = Duration.parse(formatDuration);
        return duration.toMillis() + ms;
    }
}
