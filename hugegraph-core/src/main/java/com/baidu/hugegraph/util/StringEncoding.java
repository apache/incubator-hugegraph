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
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import com.baidu.hugegraph.HugeException;
import com.baidu.hugegraph.backend.BackendException;
import com.baidu.hugegraph.backend.serializer.BytesBuffer;
import com.google.common.base.CharMatcher;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public final class StringEncoding {

    // Similar to {@link StringSerializer}
    public static int writeAsciiString(byte[] array,
                                       int startPos,
                                       String attribute) {
        E.checkArgument(CharMatcher.ascii().matchesAllOf(attribute),
                        "'%s' must be ASCII string", attribute);
        int len = attribute.length();
        if (len == 0) {
            array[startPos++] = (byte) 0x80;
            return startPos;
        }

        int i = 0;
        do {
            int c = attribute.charAt(i);
            assert c <= 127;
            byte b = (byte) c;
            if (++i == len) {
                b |= 0x80; // End marker
            }
            array[startPos++] = b;
        } while (i < len);

        return startPos;
    }

    public static String readAsciiString(byte[] array, int startPos) {
        StringBuilder sb = new StringBuilder();
        int c = 0;
        do {
            c = 0xFF & array[startPos++];
            if (c != 0x80) {
                sb.append((char) (c & 0x7F));
            }
        } while ((c & 0x80) <= 0);
        return sb.toString();
    }

    public static int getAsciiByteLength(String attribute) {
        E.checkArgument(CharMatcher.ascii().matchesAllOf(attribute),
                        "'%s' must be ASCII string", attribute);
        return attribute.isEmpty() ? 1 : attribute.length();
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

    public static byte[] compress(String value) {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
             GZIPOutputStream out = new GZIPOutputStream(bos, 256)) {
            byte[] bytes = StringEncoding.encode(value);
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
            byte[] bytes = new byte[64];
            int len;
            while ((len = in.read(bytes)) > 0) {
                buf.write(bytes, 0, len);
            }
            return decode(buf.bytes());
        } catch (IOException e) {
            throw new BackendException("Failed to decompress: %s", e, value);
        }
    }

    public static String format(byte[] bytes) {
        return String.format("%s[0x%s]",
                             StringEncoding.decode(bytes),
                             Bytes.toHex(bytes));
    }
}
