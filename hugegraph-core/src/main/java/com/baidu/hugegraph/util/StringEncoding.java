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
// Copyright 2017 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.baidu.hugegraph.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.UUID;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.mindrot.jbcrypt.BCrypt;

import com.baidu.hugegraph.HugeException;
import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.backend.serializer.BytesBuffer;
import com.google.common.base.CharMatcher;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 * @author HugeGraph Authors
 */
public final class StringEncoding {

    private static final MessageDigest DIGEST;

    static {
        final String ALG = "SHA-256";
        try {
            DIGEST = MessageDigest.getInstance(ALG);
        } catch (NoSuchAlgorithmException e) {
            throw new HugeException("Failed to load algorithm %s", e, ALG);
        }
    }

    private static final Base64.Encoder BASE64_ENCODER = Base64.getEncoder();
    private static final Base64.Decoder BASE64_DECODER = Base64.getDecoder();

    // Similar to {@link StringSerializer}
    public static int writeAsciiString(byte[] array, int offset, String value) {
        E.checkArgument(CharMatcher.ascii().matchesAllOf(value),
                        "'%s' must be ASCII string", value);
        int len = value.length();
        if (len == 0) {
            array[offset++] = (byte) 0x80;
            return offset;
        }

        int i = 0;
        do {
            int c = value.charAt(i);
            assert c <= 127;
            byte b = (byte) c;
            if (++i == len) {
                b |= 0x80; // End marker
            }
            array[offset++] = b;
        } while (i < len);

        return offset;
    }

    public static String readAsciiString(byte[] array, int offset) {
        StringBuilder sb = new StringBuilder();
        int c = 0;
        do {
            c = 0xFF & array[offset++];
            if (c != 0x80) {
                sb.append((char) (c & 0x7F));
            }
        } while ((c & 0x80) <= 0);
        return sb.toString();
    }

    public static int getAsciiByteLength(String value) {
        E.checkArgument(CharMatcher.ascii().matchesAllOf(value),
                        "'%s' must be ASCII string", value);
        return value.isEmpty() ? 1 : value.length();
    }

    public static byte[] encode(String value) {
        try {
            return value.getBytes("UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new HugeException("Failed to encode string", e);
        }
    }

    public static String decode(byte[] bytes) {
        try {
            return new String(bytes, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new HugeException("Failed to decode string", e);
        }
    }

    public static String encodeBase64(byte[] bytes) {
        return BASE64_ENCODER.encodeToString(bytes);
    }

    public static byte[] decodeBase64(String value) {
        return BASE64_DECODER.decode(value);
    }

    public static byte[] compress(String value) {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
             GZIPOutputStream out = new GZIPOutputStream(bos, 256)) {
            byte[] bytes = encode(value);
            out.write(bytes);
            out.finish();
            return bos.toByteArray();
        } catch (IOException e) {
            throw new BackendException("Failed to compress: %s", e, value);
        }
    }

    public static String decompress(byte[] value) {
        BytesBuffer buf = BytesBuffer.allocate(value.length * 2);
        try (ByteArrayInputStream bis = new ByteArrayInputStream(value);
             GZIPInputStream in = new GZIPInputStream(bis)) {
            byte[] bytes = new byte[value.length];
            int len;
            while ((len = in.read(bytes)) > 0) {
                buf.write(bytes, 0, len);
            }
            return decode(buf.bytes());
        } catch (IOException e) {
            throw new BackendException("Failed to decompress: %s", e, value);
        }
    }

    public static String hashPassword(String password) {
        return BCrypt.hashpw(password, BCrypt.gensalt(4));
    }

    public static boolean checkPassword(String candidatePassword,
                                        String dbPassword) {
        return BCrypt.checkpw(candidatePassword, dbPassword);
    }

    public static String sha256(String string) {
        byte[] stringBytes = encode(string);
        DIGEST.reset();
        return StringEncoding.encodeBase64(DIGEST.digest(stringBytes));
    }

    public static String format(byte[] bytes) {
        return String.format("%s[0x%s]", decode(bytes), Bytes.toHex(bytes));
    }

    public static UUID uuid(String value) {
        E.checkArgument(value != null, "The UUID can't be null");
        try {
            if (value.contains("-") && value.length() == 36) {
                return UUID.fromString(value);
            }
            // UUID represented by hex string
            E.checkArgument(value.length() == 32,
                            "Invalid UUID string: %s", value);
            String high = value.substring(0, 16);
            String low = value.substring(16);
            return new UUID(Long.parseUnsignedLong(high, 16),
                            Long.parseUnsignedLong(low, 16));
        } catch (NumberFormatException ignored) {
            throw new IllegalArgumentException("Invalid UUID string: " + value);
        }
    }
}
