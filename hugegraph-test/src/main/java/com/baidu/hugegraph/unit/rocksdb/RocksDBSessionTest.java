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

package com.baidu.hugegraph.unit.rocksdb;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.junit.Assume;
import org.junit.Test;
import org.rocksdb.RocksDBException;

import com.baidu.hugegraph.backend.store.BackendEntry.BackendColumn;
import com.baidu.hugegraph.backend.store.rocksdb.RocksDBSessions.Session;
import com.baidu.hugegraph.testutil.Assert;

public class RocksDBSessionTest extends BaseRocksDBUnitTest {

    @Test
    public void testPutGet() throws RocksDBException {
        String value = s(this.rocks.session().get(TABLE, b("person:1gname")));
        Assert.assertEquals(null, value);

        this.rocks.session().put(TABLE, b("person:1gname"), b("James"));
        this.rocks.session().put(TABLE, b("person:1gage"), b("19"));
        this.rocks.session().put(TABLE, b("person:1gcity"), b("Beijing"));
        this.commit();

        value = s(this.rocks.session().get(TABLE, b("person:1gname")));
        Assert.assertEquals("James", value);

        value = s(this.rocks.session().get(TABLE, b("person:1gage")));
        Assert.assertEquals("19", value);

        value = s(this.rocks.session().get(TABLE, b("person:1gcity")));
        Assert.assertEquals("Beijing", value);
    }

    @Test
    public void testPutGetWithMultiTables() throws RocksDBException {
        final String TABLE2 = "test-table2";

        this.rocks.createTable(TABLE2);
        this.rocks.session().put(TABLE, b("person:1gname"), b("James"));
        this.rocks.session().put(TABLE2, b("person:1gname"), b("James2"));
        this.commit();

        String value = s(this.rocks.session().get(TABLE, b("person:1gname")));
        Assert.assertEquals("James", value);

        String value2 = s(this.rocks.session().get(TABLE2, b("person:1gname")));
        Assert.assertEquals("James2", value2);
    }

    @Test
    public void testMergeWithCounter() throws RocksDBException {
        this.rocks.session().put(TABLE, b("person:1gage"), b(19));
        this.commit();

        this.rocks.session().merge(TABLE, b("person:1gage"), b(1));
        this.commit();

        byte[] value = this.rocks.session().get(TABLE, b("person:1gage"));
        Assert.assertEquals(20L, l(value));

        this.rocks.session().merge(TABLE, b("person:1gage"), b(123456789000L));
        this.commit();

        value = this.rocks.session().get(TABLE, b("person:1gage"));
        Assert.assertEquals(123456789020L, l(value));

        this.rocks.session().put(TABLE, b("person:1gage"), b(250));
        this.commit();

        this.rocks.session().merge(TABLE, b("person:1gage"), b(10));
        this.commit();

        value = this.rocks.session().get(TABLE, b("person:1gage"));
        Assert.assertEquals(260L, l(value));
    }

    @Test
    public void testMergeWithStringList() throws RocksDBException {
        Assume.assumeTrue("Not support string append now", false);

        this.rocks.session().put(TABLE, b("person:1gphoneno"), b("12306"));
        this.commit();

        this.rocks.session().merge(TABLE, b("person:1gphoneno"), b("12315"));
        this.commit();

        Assert.assertEquals("12306,12315", get("person:1gphoneno"));
    }

    @Test
    public void testScanByPrefix() throws RocksDBException {
        put("person:1gname", "James");
        put("person:1gage", "19");
        put("person:1gcity", "Beijing");

        put("person:2gname", "Lisa");
        put("person:2gage", "20");
        put("person:2gcity", "Beijing");

        Map<String, String> results = new HashMap<>();
        Session session = this.rocks.session();
        Iterator<BackendColumn> iter = session.scan(TABLE, b("person:1"));
        while (iter.hasNext()) {
            BackendColumn col = iter.next();
            results.put(s(col.name), s(col.value));
        }

        Assert.assertEquals(3, results.size());
        Assert.assertEquals("James", results.get("person:1gname"));
        Assert.assertEquals("19", results.get("person:1gage"));
        Assert.assertEquals("Beijing", results.get("person:1gcity"));

        Assert.assertEquals("Lisa", get("person:2gname"));
    }

    @Test
    public void testScanByRange() throws RocksDBException {
        put("person:1gname", "James");
        put("person:1gage", "19");
        put("person:1gcity", "Beijing");

        put("person:2gname", "Lisa");
        put("person:2gage", "20");
        put("person:2gcity", "Beijing");

        put("person:3gname", "Hebe");
        put("person:3gage", "21");
        put("person:3gcity", "Taipei");

        Map<String, String> results = new HashMap<>();
        Session session = this.rocks.session();
        Iterator<BackendColumn> iter = session.scan(TABLE,
                                                    b("person:1"),
                                                    b("person:3"));
        while (iter.hasNext()) {
            BackendColumn col = iter.next();
            results.put(s(col.name), s(col.value));
        }

        Assert.assertEquals(6, results.size());
        Assert.assertEquals("James", results.get("person:1gname"));
        Assert.assertEquals("19", results.get("person:1gage"));
        Assert.assertEquals("Beijing", results.get("person:1gcity"));

        Assert.assertEquals("Lisa", results.get("person:2gname"));
        Assert.assertEquals("20", results.get("person:2gage"));
        Assert.assertEquals("Beijing", results.get("person:2gcity"));

        Assert.assertEquals("Hebe", get("person:3gname"));
    }

    @Test
    public void testScanByRangeWithBytes() throws RocksDBException {
        Session session = this.rocks.session();

        byte[] key11 = new byte[]{1, 1};
        byte[] value11 = b("value-1-1");
        session.put(TABLE, key11, value11);

        byte[] key12 = new byte[]{1, 2};
        byte[] value12 = b("value-1-2");
        session.put(TABLE, key12, value12);

        byte[] key21 = new byte[]{2, 1};
        byte[] value21 = b("value-2-1");
        session.put(TABLE, key21, value21);

        byte[] key22 = new byte[]{2, 2};
        byte[] value22 = b("value-2-2");
        session.put(TABLE, key22, value22);

        byte[] key23 = new byte[]{2, 3};
        byte[] value23 = b("value-2-3");
        session.put(TABLE, key23, value23);

        this.commit();

        Map<ByteBuffer, byte[]> results = new HashMap<>();
        Iterator<BackendColumn> iter = session.scan(TABLE,
                                                    new byte[]{1, 0},
                                                    new byte[]{2, 3});
        while (iter.hasNext()) {
            BackendColumn col = iter.next();
            results.put(ByteBuffer.wrap(col.name), col.value);
        }

        Assert.assertEquals(4, results.size());
        Assert.assertArrayEquals(value11, results.get(ByteBuffer.wrap(key11)));
        Assert.assertArrayEquals(value12, results.get(ByteBuffer.wrap(key12)));
        Assert.assertArrayEquals(value21, results.get(ByteBuffer.wrap(key21)));
        Assert.assertArrayEquals(value22, results.get(ByteBuffer.wrap(key22)));

        Assert.assertArrayEquals(value23, session.get(TABLE, key23));
    }

    @Test
    public void testScanByRangeWithSignedBytes() throws RocksDBException {
        Session session = this.rocks.session();

        byte[] key11 = new byte[]{1, 1};
        byte[] value11 = b("value-1-1");
        session.put(TABLE, key11, value11);

        byte[] key12 = new byte[]{1, 2};
        byte[] value12 = b("value-1-2");
        session.put(TABLE, key12, value12);

        byte[] key13 = new byte[]{1, -3};
        byte[] value13 = b("value-1-3");
        session.put(TABLE, key13, value13);

        byte[] key21 = new byte[]{2, 1};
        byte[] value21 = b("value-2-1");
        session.put(TABLE, key21, value21);

        this.commit();

        Iterator<BackendColumn> iter;

        iter = session.scan(TABLE, new byte[]{1, -1}, new byte[]{1, 3});
        Assert.assertFalse(iter.hasNext());

        iter = session.scan(TABLE, new byte[]{1, 1}, new byte[]{1, -1});
        Map<ByteBuffer, byte[]> results = new HashMap<>();
        while (iter.hasNext()) {
            BackendColumn col = iter.next();
            results.put(ByteBuffer.wrap(col.name), col.value);
        }

        Assert.assertEquals(3, results.size());
        Assert.assertArrayEquals(value11, results.get(ByteBuffer.wrap(key11)));
        Assert.assertArrayEquals(value12, results.get(ByteBuffer.wrap(key12)));
        Assert.assertArrayEquals(value13, results.get(ByteBuffer.wrap(key13)));

        Assert.assertArrayEquals(value21, session.get(TABLE, key21));
    }

    @Test
    public void testUpdate() throws RocksDBException {
        put("person:1gname", "James");
        put("person:1gage", "19");
        put("person:1gcity", "Beijing");

        Assert.assertEquals("James", get("person:1gname"));
        Assert.assertEquals("19", get("person:1gage"));
        Assert.assertEquals("Beijing", get("person:1gcity"));

        put("person:1gage", "20");

        Assert.assertEquals("James", get("person:1gname"));
        Assert.assertEquals("20", get("person:1gage"));
        Assert.assertEquals("Beijing", get("person:1gcity"));
    }

    @Test
    public void testDeleteByKey() throws RocksDBException {
        put("person:1gname", "James");
        put("person:1gage", "19");
        put("person:1gcity", "Beijing");

        Assert.assertEquals("James", get("person:1gname"));
        Assert.assertEquals("19", get("person:1gage"));
        Assert.assertEquals("Beijing", get("person:1gcity"));

        this.rocks.session().delete(TABLE, b("person:1gage"));
        this.commit();

        Assert.assertEquals("James", get("person:1gname"));
        Assert.assertEquals(null, get("person:1gage"));
        Assert.assertEquals("Beijing", get("person:1gcity"));
    }

    @Test
    public void testDeleteByKeyButNotExist() throws RocksDBException {
        put("person:1gname", "James");
        put("person:1gage", "19");
        put("person:1gcity", "Beijing");

        Assert.assertEquals("James", get("person:1gname"));
        Assert.assertEquals("19", get("person:1gage"));
        Assert.assertEquals("Beijing", get("person:1gcity"));

        this.rocks.session().delete(TABLE, b("person:1"));
        this.commit();

        Assert.assertEquals("James", get("person:1gname"));
        Assert.assertEquals("19", get("person:1gage"));
        Assert.assertEquals("Beijing", get("person:1gcity"));
    }

    @Test
    public void testDeleteByPrefix() throws RocksDBException {
        put("person:1gname", "James");
        put("person:1gage", "19");
        put("person:1gcity", "Beijing");

        put("person:2gname", "Lisa");
        put("person:2gage", "20");
        put("person:2gcity", "Beijing");

        Assert.assertEquals("James", get("person:1gname"));
        Assert.assertEquals("19", get("person:1gage"));
        Assert.assertEquals("Beijing", get("person:1gcity"));

        this.rocks.session().deletePrefix(TABLE, b("person:1"));
        this.commit();

        Assert.assertEquals(null, get("person:1gname"));
        Assert.assertEquals(null, get("person:1gage"));
        Assert.assertEquals(null, get("person:1gcity"));

        Assert.assertEquals("Lisa", get("person:2gname"));
    }

    @Test
    public void testDeleteByRange() throws RocksDBException {
        put("person:1gname", "James");
        put("person:1gage", "19");
        put("person:1gcity", "Beijing");

        put("person:2gname", "Lisa");
        put("person:2gage", "20");
        put("person:2gcity", "Beijing");

        put("person:3gname", "Hebe");
        put("person:3gage", "21");
        put("person:3gcity", "Taipei");

        Assert.assertEquals("James", get("person:1gname"));
        Assert.assertEquals("Lisa", get("person:2gname"));
        Assert.assertEquals("Hebe", get("person:3gname"));

        this.rocks.session().deleteRange(TABLE, b("person:1"), b("person:3"));
        this.commit();

        Assert.assertEquals(null, get("person:1gname"));
        Assert.assertEquals(null, get("person:1gage"));
        Assert.assertEquals(null, get("person:1gcity"));

        Assert.assertEquals(null, get("person:2gname"));
        Assert.assertEquals(null, get("person:2gage"));
        Assert.assertEquals(null, get("person:2gcity"));

        Assert.assertEquals("Hebe", get("person:3gname"));
        Assert.assertEquals("21", get("person:3gage"));
        Assert.assertEquals("Taipei", get("person:3gcity"));
    }

    @Test
    public void testDeleteByRangeWithBytes() throws RocksDBException {
        Session session = this.rocks.session();

        byte[] key11 = new byte[]{1, 1};
        byte[] value11 = b("value-1-1");
        session.put(TABLE, key11, value11);

        byte[] key12 = new byte[]{1, 2};
        byte[] value12 = b("value-1-2");
        session.put(TABLE, key12, value12);

        byte[] key21 = new byte[]{2, 1};
        byte[] value21 = b("value-2-1");
        session.put(TABLE, key21, value21);

        session.deleteRange(TABLE, key11, new byte[]{1, 3});
        this.commit();

        Assert.assertArrayEquals(null, session.get(TABLE, key11));
        Assert.assertArrayEquals(null, session.get(TABLE, key12));
        Assert.assertArrayEquals(value21, session.get(TABLE, key21));
    }

    @Test
    public void testDeleteByRangeWithSignedBytes() throws RocksDBException {
        Session session = this.rocks.session();

        byte[] key11 = new byte[]{1, 1};
        byte[] value11 = b("value-1-1");
        session.put(TABLE, key11, value11);

        byte[] key12 = new byte[]{1, -2};
        byte[] value12 = b("value-1-2");
        session.put(TABLE, key12, value12);

        byte[] key21 = new byte[]{2, 1};
        byte[] value21 = b("value-2-1");
        session.put(TABLE, key21, value21);

        session.deleteRange(TABLE, new byte[]{1, -3}, new byte[]{1, 3});
        this.commit();

        Assert.assertArrayEquals(value11, session.get(TABLE, key11));
        Assert.assertArrayEquals(value12, session.get(TABLE, key12));
        Assert.assertArrayEquals(value21, session.get(TABLE, key21));

        session.deleteRange(TABLE, new byte[]{1, 1}, new byte[]{1, -1});
        this.commit();

        Assert.assertArrayEquals(null, session.get(TABLE, key11));
        Assert.assertArrayEquals(null, session.get(TABLE, key12));
        Assert.assertArrayEquals(value21, session.get(TABLE, key21));
    }

    @Test
    public void testDeleteByRangeWithMinMaxByteValue() throws RocksDBException {
        Session session = this.rocks.session();

        byte[] key11 = new byte[]{1, 0};
        byte[] value11 = b("value-1-1");
        session.put(TABLE, key11, value11);

        byte[] key12 = new byte[]{1, 127};
        byte[] value12 = b("value-1-2");
        session.put(TABLE, key12, value12);

        byte[] key13 = new byte[]{1, (byte) 0x80}; // 128
        byte[] value13 = b("value-1-3");
        session.put(TABLE, key13, value13);

        byte[] key14 = new byte[]{1, (byte) 0xff}; // 255
        byte[] value14 = b("value-1-4");
        session.put(TABLE, key14, value14);

        byte[] key20 = new byte[]{2, 0};
        byte[] value20 = b("value-2-0");
        session.put(TABLE, key20, value20);

        session.deleteRange(TABLE,
                            new byte[]{1, 0}, new byte[]{1, (byte) 0xff});
        this.commit();

        Assert.assertArrayEquals(null, session.get(TABLE, key11));
        Assert.assertArrayEquals(null, session.get(TABLE, key12));
        Assert.assertArrayEquals(null, session.get(TABLE, key13));
        Assert.assertArrayEquals(value14, session.get(TABLE, key14));
        Assert.assertArrayEquals(value20, session.get(TABLE, key20));

        session.deleteRange(TABLE,
                            new byte[]{1, (byte) 0xff}, new byte[]{2, 0});
        this.commit();

        Assert.assertArrayEquals(null, session.get(TABLE, key11));
        Assert.assertArrayEquals(null, session.get(TABLE, key12));
        Assert.assertArrayEquals(null, session.get(TABLE, key13));
        Assert.assertArrayEquals(null, session.get(TABLE, key14));
        Assert.assertArrayEquals(value20, session.get(TABLE, key20));
    }

    @Test
    public void testDeleteSingle() throws RocksDBException {
        put("person:1gname", "James");
        put("person:1gage", "19");
        put("person:1gname", "James2");

        Assert.assertEquals("James2", get("person:1gname"));
        Assert.assertEquals("19", get("person:1gage"));

        // deleteSingle after put once
        this.rocks.session().deleteSingle(TABLE, b("person:1gage"));
        this.commit();

        Assert.assertEquals("James2", get("person:1gname"));
        Assert.assertEquals(null, get("person:1gage"));

        // deleteSingle after put twice
        this.rocks.session().deleteSingle(TABLE, b("person:1gname"));
        this.commit();

        // NOTE: maybe return "James" here
        Assert.assertEquals(null, get("person:1gname"));
        Assert.assertEquals(null, get("person:1gage"));
    }

    @Test
    public void testCompact() throws RocksDBException {
        put("person:1gname", "James");
        put("person:1gage", "19");
        put("person:1gcity", "Beijing");

        Assert.assertEquals("James", get("person:1gname"));
        Assert.assertEquals("19", get("person:1gage"));
        Assert.assertEquals("Beijing", get("person:1gcity"));

        this.rocks.session().delete(TABLE, b("person:1gage"));
        this.commit();

        Assert.assertEquals("James", get("person:1gname"));
        Assert.assertEquals(null, get("person:1gage"));
        Assert.assertEquals("Beijing", get("person:1gcity"));

        this.rocks.session().compactRange(TABLE);

        Assert.assertEquals("James", get("person:1gname"));
        Assert.assertEquals(null, get("person:1gage"));
        Assert.assertEquals("Beijing", get("person:1gcity"));
    }

    @Test
    public void testProperty() {
        int count = 1234;
        for (int i = 0; i < count; i++) {
            put("key-" + i, "value" + i);
        }
        this.commit();

        String property = "rocksdb.estimate-num-keys";
        String numKeys = this.rocks.session().property(TABLE, property);
        Assert.assertEquals(String.valueOf(count), numKeys);
    }
}
