/*
 * Copyright (C) 2017 Baidu, Inc. All Rights Reserved.
 */
package com.baidu.hugegraph.utils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.UUID;

import org.apache.hadoop.hbase.util.Order;
import org.apache.hadoop.hbase.util.OrderedBytes;
import org.apache.hadoop.hbase.util.PositionedByteRange;
import org.apache.hadoop.hbase.util.SimplePositionedByteRange;
import org.apache.hadoop.hbase.util.SimplePositionedMutableByteRange;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoException;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

/**
 * Created by zhangsuochao on 17/2/7.
 */
public class ValueUtils {

    public static final int DEFAULT_NUM_BUCKETS = 256;

    public static ValueType getValueType(Object o) {
        if (o == null) {
            return ValueType.NULL;
        } else if (o instanceof Boolean) {
            return ValueType.BOOLEAN;
        } else if (o instanceof String) {
            return ValueType.STRING;
        } else if (o instanceof Byte) {
            return ValueType.BYTE;
        } else if (o instanceof Short) {
            return ValueType.SHORT;
        } else if (o instanceof Integer) {
            return ValueType.INT;
        } else if (o instanceof Long) {
            return ValueType.LONG;
        } else if (o instanceof Float) {
            return ValueType.FLOAT;
        } else if (o instanceof Double) {
            return ValueType.DOUBLE;
        } else if (o instanceof BigDecimal) {
            return ValueType.DECIMAL;
        } else if (o instanceof LocalDate) {
            return ValueType.DATE;
        } else if (o instanceof LocalTime) {
            return ValueType.TIME;
        } else if (o instanceof LocalDateTime) {
            return ValueType.TIMESTAMP;
        } else if (o instanceof Duration) {
            return ValueType.INTERVAL;
        } else if (o instanceof byte[]) {
            return ValueType.BINARY;
        } else if (o instanceof Enum) {
            return ValueType.ENUM;
        } else if (o instanceof UUID) {
            return ValueType.UUID;
        } else if (o instanceof KryoSerializable) {
            return ValueType.KRYO_SERIALIZABLE;
        } else if (o instanceof Serializable) {
            return ValueType.SERIALIZABLE;
        } else {
            throw new IllegalArgumentException("Unexpected data of type : " + o.getClass().getName());
        }
    }

    public static byte[] serialize(Object o) {
        PositionedByteRange buffer = new SimplePositionedMutableByteRange(4096);
        serialize(buffer, o);
        buffer.setLength(buffer.getPosition());
        buffer.setPosition(0);
        byte[] bytes = new byte[buffer.getRemaining()];
        buffer.get(bytes);
        return bytes;
    }

    public static void serialize(PositionedByteRange buffer, Object o) {
        final Order order = Order.ASCENDING;
        ValueType type = getValueType(o);
        OrderedBytes.encodeInt8(buffer, type.getCode(), order);
        switch (type) {
            case NULL:
                break;
            case BOOLEAN:
                OrderedBytes.encodeInt8(buffer, (Boolean) o ? (byte) 1 : (byte) 0, order);
                break;
            case STRING:
                OrderedBytes.encodeString(buffer, (String) o, order);
                break;
            case BYTE:
                OrderedBytes.encodeInt8(buffer, (Byte) o, order);
                break;
            case SHORT:
                OrderedBytes.encodeInt16(buffer, (Short) o, order);
                break;
            case INT:
                OrderedBytes.encodeInt32(buffer, (Integer) o, order);
                break;
            case LONG:
                OrderedBytes.encodeInt64(buffer, (Long) o, order);
                break;
            case FLOAT:
                OrderedBytes.encodeFloat32(buffer, (Float) o, order);
                break;
            case DOUBLE:
                OrderedBytes.encodeFloat64(buffer, (Double) o, order);
                break;
            case DECIMAL:
                OrderedBytes.encodeNumeric(buffer, (BigDecimal) o, order);
                break;
            case DATE:
                OrderedBytes.encodeInt64(buffer, ((LocalDate) o).toEpochDay(), order);
                break;
            case TIME:
                OrderedBytes.encodeInt64(buffer, ((LocalTime) o).toNanoOfDay(), order);
                break;
            case TIMESTAMP:
                OrderedBytes.encodeInt64(buffer, ((LocalDateTime) o).toEpochSecond(ZoneOffset.UTC), order);
                break;
            case INTERVAL:
                OrderedBytes.encodeInt64(buffer, ((Duration) o).toNanos(), order);
                break;
            case BINARY:
                OrderedBytes.encodeBlobVar(buffer, (byte[]) o, order);
                break;
            case ENUM:
                OrderedBytes.encodeString(buffer, o.getClass().getName(), order);
                OrderedBytes.encodeString(buffer, o.toString(), order);
                break;
            case UUID:
                OrderedBytes.encodeInt64(buffer, ((UUID) o).getMostSignificantBits(), order);
                OrderedBytes.encodeInt64(buffer, ((UUID) o).getLeastSignificantBits(), order);
                break;
            case KRYO_SERIALIZABLE:
                try {
                    ByteArrayOutputStream baos = new ByteArrayOutputStream(32);
                    Output output = new Output(baos);
                    new Kryo().writeClassAndObject(output, o);
                    output.close();
                    OrderedBytes.encodeBlobVar(buffer, baos.toByteArray(), order);
                } catch (KryoException io) {
                    throw new RuntimeException("Unexpected error serializing object.", io);
                }
                break;
            case SERIALIZABLE:
                try {
                    ByteArrayOutputStream baos = new ByteArrayOutputStream(32);
                    ObjectOutputStream oos = new ObjectOutputStream(baos);
                    oos.writeObject(o);
                    oos.close();
                    OrderedBytes.encodeBlobVar(buffer, baos.toByteArray(), order);
                } catch (IOException io) {
                    throw new RuntimeException("Unexpected error serializing object.", io);
                }
                break;
            default:
                throw new IllegalArgumentException("Unexpected data of type : " + o.getClass().getName());
        }
    }

    public static byte[] serializeWithSalt(Object o) {
        PositionedByteRange buffer = new SimplePositionedMutableByteRange(4096);
        serializeWithSalt(buffer, o);
        buffer.setLength(buffer.getPosition());
        buffer.setPosition(0);
        byte[] bytes = new byte[buffer.getRemaining()];
        buffer.get(bytes);
        return bytes;
    }

    public static void serializeWithSalt(PositionedByteRange buffer, Object o) {
        byte[] bytes = serialize(o);
        byte saltingByte = getSaltingByte(bytes);
        buffer.put(saltingByte);
        buffer.put(bytes);
    }

    @SuppressWarnings("unchecked")
    public static <T> T deserialize(byte[] target) {
        if (target == null) {
            return null;
        }
        PositionedByteRange buffer = new SimplePositionedByteRange(target);
        return deserialize(buffer);
    }

    @SuppressWarnings("unchecked")
    public static <T> T deserialize(PositionedByteRange buffer) {
        ValueType type = ValueType.valueOf(OrderedBytes.decodeInt8(buffer));

        switch (type) {
            case NULL:
                return null;
            case BOOLEAN:
                return (T) Boolean.valueOf(OrderedBytes.decodeInt8(buffer) == 1);
            case STRING:
                return (T) OrderedBytes.decodeString(buffer);
            case BYTE:
                return (T) Byte.valueOf(OrderedBytes.decodeInt8(buffer));
            case SHORT:
                return (T) Short.valueOf(OrderedBytes.decodeInt16(buffer));
            case INT:
                return (T) Integer.valueOf(OrderedBytes.decodeInt32(buffer));
            case LONG:
                return (T) Long.valueOf(OrderedBytes.decodeInt64(buffer));
            case FLOAT:
                return (T) Float.valueOf(OrderedBytes.decodeFloat32(buffer));
            case DOUBLE:
                return (T) Double.valueOf(OrderedBytes.decodeFloat64(buffer));
            case DECIMAL:
                return (T) OrderedBytes.decodeNumericAsBigDecimal(buffer);
            case DATE:
                return (T) LocalDate.ofEpochDay(OrderedBytes.decodeInt64(buffer));
            case TIME:
                return (T) LocalTime.ofNanoOfDay(OrderedBytes.decodeInt64(buffer));
            case TIMESTAMP:
                return (T) LocalDateTime.ofEpochSecond(OrderedBytes.decodeInt64(buffer), 0, ZoneOffset.UTC);
            case INTERVAL:
                return (T) Duration.ofNanos(OrderedBytes.decodeInt64(buffer));
            case BINARY:
                return (T) OrderedBytes.decodeBlobVar(buffer);
            case ENUM:
                try {
                    String clsName = OrderedBytes.decodeString(buffer);
                    @SuppressWarnings("rawtypes")
                    Class<? extends Enum> cls = (Class<? extends Enum>) Class.forName(clsName);
                    String val = OrderedBytes.decodeString(buffer);
                    return (T) Enum.valueOf(cls, val);
                } catch (ClassNotFoundException cnfe) {
                    throw new RuntimeException("Unexpected error deserializing enum.", cnfe);
                }
            case UUID:
                long mostSignificantBits = OrderedBytes.decodeInt64(buffer);
                long leastSignificantBits = OrderedBytes.decodeInt64(buffer);
                return (T) new UUID(mostSignificantBits, leastSignificantBits);
            case KRYO_SERIALIZABLE:
                try {
                    byte[] blob = OrderedBytes.decodeBlobVar(buffer);
                    ByteArrayInputStream bin = new ByteArrayInputStream(blob);
                    Input input = new Input(bin);
                    return (T) new Kryo().readClassAndObject(input);
                } catch (KryoException e) {
                    throw new RuntimeException("Unexpected error deserializing object.", e);
                }
            case SERIALIZABLE:
                try {
                    byte[] blob = OrderedBytes.decodeBlobVar(buffer);
                    ByteArrayInputStream bin = new ByteArrayInputStream(blob);
                    ObjectInputStream ois = new ObjectInputStream(bin);
                    return (T) ois.readObject();
                } catch (IOException | ClassNotFoundException e) {
                    throw new RuntimeException("Unexpected error deserializing object.", e);
                }
            default:
                throw new IllegalArgumentException("Unexpected data type: " + type);
        }
    }

    public static <T> T deserializeWithSalt(byte[] target) {
        PositionedByteRange buffer = new SimplePositionedByteRange(target);
        return deserializeWithSalt(buffer);
    }

    @SuppressWarnings("unchecked")
    public static <T> T deserializeWithSalt(PositionedByteRange buffer) {
        buffer.get();  // discard salt
        return deserialize(buffer);
    }

    /**
     * Returns the salt for a given value.
     *
     * @param value the value
     *
     * @return the salt to prepend to {@code value}
     */
    public static byte getSaltingByte(byte[] value) {
        int hash = calculateHashCode(value);
        return (byte) (Math.abs(hash) % DEFAULT_NUM_BUCKETS);
    }

    private static int calculateHashCode(byte a[]) {
        if (a == null) {
            return 0;
        }
        int result = 1;
        for (byte b : a) {
            result = 31 * result + b;
        }
        return result;
    }

}
