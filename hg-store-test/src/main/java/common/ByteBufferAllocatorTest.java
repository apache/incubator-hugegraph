package common;

import com.baidu.hugegraph.store.buffer.ByteBufferAllocator;
import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;

@Slf4j
public class ByteBufferAllocatorTest extends BaseCommonTest {
    @Test
    public void getAndReleaseTest() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(2);

        ByteBufferAllocator allocator = new ByteBufferAllocator(1, 2);

        new Thread(() -> {
            try {
                var buffer1 = allocator.get();
                var buffer2 = allocator.get();
                Thread.sleep(2000);
                Assert.assertEquals(buffer1.limit(), 1);
                allocator.release(buffer1);
                allocator.release(buffer2);
                latch.countDown();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }).start();

        new Thread(() -> {
            try {
                Thread.sleep(1000);
                var buffer1 = allocator.get();
                var buffer2 = allocator.get();
                Assert.assertEquals(buffer1.limit(), 1);
                allocator.release(buffer1);
                allocator.release(buffer2);
                latch.countDown();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }).start();

        latch.await();
    }
}
