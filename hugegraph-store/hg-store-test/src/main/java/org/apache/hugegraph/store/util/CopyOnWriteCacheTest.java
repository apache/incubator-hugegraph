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

package org.apache.hugegraph.store.util;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.caucho.hessian.io.Hessian2Input;
import com.caucho.hessian.io.Hessian2Output;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Set;

import org.apache.hugegraph.store.util.CopyOnWriteCache;
import org.junit.Before;
import org.junit.Test;

public class CopyOnWriteCacheTest {

    private CopyOnWriteCache<String, String> writeCache;

    @Before
    public void setUp() {
        writeCache = new CopyOnWriteCache<>(5000L);
    }

    @Test
    public void testContainsKey() {
        // Setup
        // Run the test
        writeCache.put("k", "v");
        final boolean result = writeCache.containsKey("k");
        Map<? extends String, ? extends String> allKeys =
                Map.ofEntries(Map.entry("key1", "value1"));
        writeCache.putAll(allKeys);
        // Verify the results
        assertTrue(result);
        final Set<Map.Entry<String, String>> entries = writeCache.entrySet();
        Set<String> keySet = writeCache.keySet();
        String val = writeCache.get("k");
        boolean isEmpty = writeCache.isEmpty();
        writeCache.size();
        writeCache.values();
        // Verify the results
        assertFalse(isEmpty);
        writeCache.remove("k");
        writeCache.putIfAbsent("k", "v");
        writeCache.replace("k", "original", "replacement");
        writeCache.replace("k", "v");
        writeCache.clear();
        assertTrue(writeCache.isEmpty());
    }

    // @Test
    public void testCache() throws InterruptedException {
        Map<String, String> cache = new CopyOnWriteCache<>(1000);
        cache.put("1", "1");
        Thread.sleep(2000);
        Asserts.isTrue(!cache.containsKey("1"), "cache do not clear");
    }

    // @Test
    public void test() {

        byte[] bytes;
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
            long[] l = new long[]{1, 2};
            Hessian2Output output = new Hessian2Output(bos);
            output.writeObject(l);
            output.flush();
            bytes = bos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        try (ByteArrayInputStream bis = new ByteArrayInputStream(bytes)) {
            Hessian2Input input = new Hessian2Input(bis);
            long[] obj = (long[]) input.readObject();
            input.close();

            for (long l : obj) {
                System.out.println(l);
            }

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
