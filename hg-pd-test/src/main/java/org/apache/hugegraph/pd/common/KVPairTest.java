package org.apache.hugegraph.pd.common;

import org.junit.Before;
import org.junit.Test;

import java.util.Objects;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class KVPairTest {

    KVPair<String, Integer> pair;
    @Before
    public void init(){
        pair = new KVPair<>("key", 1);
    }

    @Test
    public void testGetKey(){
        assertEquals(pair.getKey(), "key");
    }

    @Test
    public void testSetKey(){
        pair.setKey("key2");
        assertEquals(pair.getKey(), "key2");
    }

    @Test
    public void testGetValue(){
        assertTrue(Objects.equals(pair.getValue(), 1));
    }

    @Test
    public void testSetValue(){
        pair.setValue(2);
        assertTrue(Objects.equals(pair.getValue(), 2));
    }

    @Test
    public void testToString(){

    }

    @Test
    public void testHashCode(){

    }

    @Test
    public void testEquals(){
        var pair2 = new KVPair<>("key", 1);
        assertTrue(pair2.equals(pair));
    }
}
