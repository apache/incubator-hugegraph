package com.baidu.hugegraph.store.util;

import com.alipay.sofa.jraft.Status;
import org.apdplat.word.vector.F;
import org.junit.Assert;
import org.junit.Test;

public class FutureClosureTest {
    @Test
    public void test(){
        FutureClosure closure = new FutureClosure();
        new Thread(()->{
            try {
                Thread.sleep(1000);
                closure.run(Status.OK());
            } catch (InterruptedException e) {
                closure.run(new Status(-1, e.getMessage()));
            }

        }).start();

        Assert.assertEquals(closure.get().getCode(), Status.OK().getCode());
        Assert.assertEquals(closure.get().getCode(), Status.OK().getCode());
        Assert.assertEquals(closure.get().getCode(), Status.OK().getCode());
    }
}
