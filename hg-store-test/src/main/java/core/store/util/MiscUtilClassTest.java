package core.store.util;

import com.alipay.sofa.jraft.Status;
import com.baidu.hugegraph.store.HgStoreEngine;
import com.baidu.hugegraph.store.util.Asserts;
import com.baidu.hugegraph.store.util.HgRaftError;
import com.baidu.hugegraph.store.util.HgStoreException;
import com.baidu.hugegraph.store.util.ManualResetEvent;
import org.apache.kafka.common.metrics.Stat;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class MiscUtilClassTest {

    @Test
    public void testHgRaftError(){
        HgRaftError error = HgRaftError.forNumber(0);
        assertEquals(0, error.getNumber());
        assertEquals("OK", error.getMsg());
        assertEquals(Status.OK().getCode(), error.toStatus().getCode());
    }

    @Test (expected = NullPointerException.class)
    public void testAsserts(){
        assertTrue(Asserts.isInvalid(null));
        assertTrue(Asserts.isInvalid());
        assertTrue(Asserts.isInvalid(null));
        assertTrue(Asserts.isInvalid(""));
        assertFalse(Asserts.isInvalid("a"));

        Asserts.isNonNull(null);
        Asserts.isNonNull(null, "msg");
    }

    @Test (expected = IllegalArgumentException.class)
    public void testAsserts2(){
        Asserts.isTrue(false, "");
        Asserts.isFalse(true, "");
        Asserts.isTrue(true, null);
    }

    @Test
    public void testHgStoreException(){
        var exception = new HgStoreException();
        assertEquals(0, exception.getCode());
        exception = new HgStoreException("invalid");
        assertEquals(1000, exception.getCode());
        exception = new HgStoreException(1000, "invalid");
        assertEquals(1000, exception.getCode());
        exception = new HgStoreException(1000, new Throwable());
        assertEquals(1000, exception.getCode());
        exception = new HgStoreException("invalid", new Throwable());
        assertEquals(1000, exception.getCode());
        exception = new HgStoreException(1000, "%s", "invalid");
        assertEquals(1000, exception.getCode());
    }

    @Test
    public void testManualResetEvent() throws InterruptedException {
        ManualResetEvent event = new ManualResetEvent(false);
        assertFalse(event.isSignalled());
        event.set();
        assertTrue(event.isSignalled());
        event.reset();
        assertFalse(event.waitOne(1, TimeUnit.SECONDS));
        event.set();
        event.waitOne();
        assertTrue(event.isSignalled());
    }



}
