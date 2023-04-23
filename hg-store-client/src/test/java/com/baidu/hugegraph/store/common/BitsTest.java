package com.baidu.hugegraph.store.common;

import com.baidu.hugegraph.store.term.Bits;
import org.junit.Assert;
// import org.junit.Test;

public class BitsTest {
    // @Test
    public void test(){
        for(int i = 0; i < Integer.MAX_VALUE; i = i + 10){
            byte[] val = new byte[4];
            Bits.putInt(val, 0, i);
            int n = Bits.getInt(val, 0);
            Assert.assertEquals(i, n);
        }
    }
}
