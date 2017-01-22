/*
 * Copyright (C) 2017 Baidu, Inc. All Rights Reserved.
 */
package com.baidu.hugegraph.utils;

public enum ValueType {
    NULL(1),
    BOOLEAN(2),
    STRING(3),
    BYTE(4),
    SHORT(5),
    INT(6),
    LONG(7),
    FLOAT(8),
    DOUBLE(9),
    DECIMAL(10),
    /**
     * 32-bit integer representing the number of DAYS since Unix epoch,
     * i.e. January 1, 1970 00:00:00 UTC. The value is absolute and
     * is time-zone independent. Negative values represents dates before
     * epoch.
     */
    DATE(11),
    /**
     * 32-bit integer representing time of the day in milliseconds.
     * The value is absolute and is time-zone independent.
     */
    TIME(12),
    /**
     * 64-bit integer representing the number of milliseconds since epoch,
     * i.e. January 1, 1970 00:00:00 UTC. Negative values represent dates
     * before epoch.
     */
    TIMESTAMP(13),
    /**
     * A value representing a period of time between two instants.
     */
    INTERVAL(14),
    BINARY(15),
    ENUM(16),
    KRYO_SERIALIZABLE(17),
    SERIALIZABLE(18),
    UUID(19),
    ANY(20),
    COUNTER(21);

    private final byte code;

    ValueType(int code) {
        this.code = (byte) code;
    }

    public static ValueType valueOf(int typeCode) {
        switch (typeCode) {
            case 1:
                return NULL;
            case 2:
                return BOOLEAN;
            case 3:
                return STRING;
            case 4:
                return BYTE;
            case 5:
                return SHORT;
            case 6:
                return INT;
            case 7:
                return LONG;
            case 8:
                return FLOAT;
            case 9:
                return DOUBLE;
            case 10:
                return DECIMAL;
            case 11:
                return DATE;
            case 12:
                return TIME;
            case 13:
                return TIMESTAMP;
            case 14:
                return INTERVAL;
            case 15:
                return BINARY;
            case 16:
                return ENUM;
            case 17:
                return KRYO_SERIALIZABLE;
            case 18:
                return SERIALIZABLE;
            case 19:
                return UUID;
            case 20:
                return ANY;
            case 21:
                return COUNTER;
            default:
                return null;
        }
    }

    public byte getCode() {
        return code;
    }

}
