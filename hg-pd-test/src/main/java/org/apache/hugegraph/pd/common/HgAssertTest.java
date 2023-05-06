package org.apache.hugegraph.pd.common;

import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.baidu.hugegraph.pd.common.HgAssert;

public class HgAssertTest {

    @Test(expected = IllegalArgumentException.class)
    public void testIsTrue() {
        HgAssert.isTrue(false, "");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testIsTrue2() {
        HgAssert.isTrue(true, null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testIsFalse() {
        HgAssert.isFalse(true, "");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testIsFalse2() {
        HgAssert.isTrue(false, null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void isArgumentValid() {
        HgAssert.isArgumentValid(new byte[0], "");
    }

    @Test(expected = IllegalArgumentException.class)
    public void isArgumentValidStr() {
        HgAssert.isArgumentValid("", "");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testIsArgumentNotNull() {
        HgAssert.isArgumentNotNull(null, "");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testIstValid() {
        HgAssert.istValid(new byte[0], "");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testIstValidStr() {
        HgAssert.isValid("", "");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testIsNotNull() {
        HgAssert.isNotNull(null, "");
    }


    @Test
    public void testIsInvalid() {
        assertFalse(HgAssert.isInvalid( "abc", "test"));
        assertTrue(HgAssert.isInvalid( "", null));
    }

    @Test
    public void testIsInvalidByte() {
        assertTrue(HgAssert.isInvalid( new byte[0]));
        assertFalse(HgAssert.isInvalid( new byte[1]));
    }

    @Test
    public void testIsInvalidMap() {
        assertTrue(HgAssert.isInvalid(new HashMap<Integer, Integer>()));
        assertFalse(HgAssert.isInvalid(new HashMap<Integer, Integer>(){{put(1, 1);}}));
    }

    @Test
    public void testIsInvalidCollection() {
        assertTrue(HgAssert.isInvalid(new ArrayList<Integer>()));
        assertFalse(HgAssert.isInvalid(new ArrayList<Integer>(){{add(1);}}));
    }

    @Test
    public void testIsContains() {
        assertTrue(HgAssert.isContains(new Object[]{new Integer(1), new Long(2)}, new Long(2)));
        assertFalse(HgAssert.isContains(new Object[]{new Integer(1), new Long(2)}, new Long(3)));
    }

    @Test
    public void testIsContainsT() {
        assertTrue(HgAssert.isContains(new ArrayList<>(){{add(1);}}, 1));
        assertFalse(HgAssert.isContains(new ArrayList<>(){{add(1);}}, 2));
    }

    @Test
    public void testIsNull() {
        assertTrue(HgAssert.isNull(null));
        assertFalse(HgAssert.isNull("abc", "cdf"));
    }

}
