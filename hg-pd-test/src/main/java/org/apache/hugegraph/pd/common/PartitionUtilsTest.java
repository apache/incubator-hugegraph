package org.apache.hugegraph.pd.common;

import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.Test;

import com.baidu.hugegraph.pd.common.PartitionUtils;


@Slf4j
public class PartitionUtilsTest extends BaseCommonTest {
    @Test
    public void testCalcHashcode() {
        byte[] key = new byte[5];
        long code = PartitionUtils.calcHashcode(key);
        Assert.assertEquals(code, 31912L);
    }
}